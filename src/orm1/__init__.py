import typing
import types
import re
from typing import Collection, Sequence, Type
import itertools

import asyncpg
import asyncpg.transaction

# mapping

Entity = object
KeyValue = typing.Hashable
PostgresCodable = typing.Any
Rec = dict[str, PostgresCodable]
EntityFactory = typing.Callable[[], Entity]


class Field(typing.NamedTuple):
    name: str
    column: str
    skip_on_insert: bool
    skip_on_update: bool

    def write_to_entity(self, entity: Entity, partial: Rec):
        self.set_to_entity(entity, partial[self.column])

    def write_to_record(self, entity: Entity, partial: Rec):
        partial[self.column] = self.get_from_entity(entity)

    def get_from_entity(self, entity: Entity):
        return getattr(entity, self.name)

    def set_to_entity(self, entity: Entity, value: PostgresCodable):
        setattr(entity, self.name, value)

    def to_partial(self, value: PostgresCodable) -> Rec:
        return {self.column: value}

    def from_partial(self, partial: Rec) -> PostgresCodable:
        return partial[self.column]


class Key(typing.Protocol):
    columns: Sequence[str]

    def from_partial(self, partial: Rec) -> KeyValue: ...
    def to_partial(self, value: KeyValue) -> Rec: ...


class PrimaryKey(Key):
    def __init__(self, fields: Sequence[Field]):
        self.fields = fields
        self.columns = [f.column for f in fields]

    def from_partial(self, partial: Rec) -> KeyValue:
        if len(self.fields) == 1:
            return self.fields[0].from_partial(partial)
        else:
            return tuple(field.from_partial(partial) for field in self.fields)

    def to_partial(self, value: KeyValue) -> Rec:
        if len(self.fields) == 1:
            return self.fields[0].to_partial(value)
        assert isinstance(value, tuple)
        partial = {}
        for field, val in zip(self.fields, value or ()):
            partial.update(field.to_partial(val))
        return partial

    def get_from_entity(self, entity: Entity) -> KeyValue:
        if len(self.fields) == 1:
            return self.fields[0].get_from_entity(entity)
        return tuple(field.get_from_entity(entity) for field in self.fields)


class ParentalKey(Key):
    def __init__(self, columns: Sequence[str], parent_key: PrimaryKey):
        self.columns = columns
        self._parent_key = parent_key

    def from_partial(self, partial: Rec) -> KeyValue:
        rec = {pc: partial[c] for c, pc in zip(self.columns, self._parent_key.columns)}
        return self._parent_key.from_partial(rec)

    def to_partial(self, value: KeyValue) -> Rec:
        rec = self._parent_key.to_partial(value)
        return {c: rec[pc] for c, pc in zip(self.columns, self._parent_key.columns)}


class Child(typing.NamedTuple):
    name: str
    target: type[Entity]
    columns: Sequence[str]
    is_singular: bool

    def get_entity_attribute(self, entity: Entity) -> Sequence[Entity]:
        val = getattr(entity, self.name)
        if self.is_singular:
            return (val,) if val else ()
        return tuple(val) if val else ()

    def set_entity_attribute(self, entity: Entity, values: Sequence[Entity]):
        if self.is_singular:
            setattr(entity, self.name, values[0] if len(values) else None)
        else:
            setattr(entity, self.name, list(values))

    def parental_key(self, parent_key: PrimaryKey) -> ParentalKey:
        return ParentalKey(self.columns, parent_key)


class EntityMapping(typing.NamedTuple):
    entity_type: Type[Entity]
    table: str
    schema: str | None
    fields: list[Field]
    children: list[Child]
    primary_key: PrimaryKey
    entity_factory: EntityFactory

    def identify_entity(self, entity: Entity) -> KeyValue:
        return self.primary_key.get_from_entity(entity)

    def identify_record(self, full: Rec) -> KeyValue:
        return self.primary_key.from_partial(full)

    def write_record(self, entity: Entity, full: Rec):
        for f in self.fields:
            f.write_to_entity(entity, full)

    def to_record(self, entity: Entity) -> Rec:
        full: Rec = {}
        for f in self.fields:
            f.write_to_record(entity, full)
        return full

    @property
    def columns(self):
        return [f.column for f in self.fields]


# SQL Object Model


class SQLText(typing.NamedTuple):
    text: str


class SQLVar:
    def __init__(self, value: PostgresCodable):
        self.value = value


class SQLName(typing.NamedTuple):
    prefix: str | None
    name: str


class SQLFragment:
    def __init__(self, elements: "Collection[SQLFragmentElement]"):
        self.elements = elements


SQLFragmentElement = SQLText | SQLVar | SQLName | SQLFragment

_sql_parse_patt_word = re.compile(r"('[^']*'|\"[^\"]*\"|\s+|::|:\w+|\w+|[^\w\s])")
_sql_parse_patt_param = re.compile(r":(\w+)")


def _sql_parse(sql: str, params: dict[str, SQLVar | PostgresCodable]):

    tokens = list[SQLText | SQLVar]()
    words: list[str] = _sql_parse_patt_word.findall(sql)

    for word in words:
        if matched := _sql_parse_patt_param.match(word):
            if matched[1] in params:
                val = params[matched[1]]
                if isinstance(val, SQLVar):
                    tokens.append(val)
                else:
                    tokens.append(SQLVar(val))
            else:
                raise Exception(f"Parameter '{matched[1]}' not provided")
        else:
            tokens.append(SQLText(word))

    return SQLFragment(tokens)


def _sql_literal(*elements: "str | SQLFragmentElement"):
    return SQLFragment([SQLText(e) if isinstance(e, str) else e for e in elements])


def _sql_join(sep: str, elements: "typing.Iterable[SQLFragmentElement]"):
    sql_sep = SQLText(sep)
    return SQLFragment(list(itertools.chain(*((e, sql_sep) for e in elements)))[:-1])


class SQLJoinOpt(typing.NamedTuple):
    Type = typing.Literal["JOIN", "LEFT JOIN"]

    type: Type
    target: SQLFragmentElement
    alias: str
    on: SQLFragmentElement


class SQLOrderByOpt(typing.NamedTuple):
    expr: SQLFragmentElement
    ascending: bool = True
    nulls_last: bool = True


class StructuredQuery(typing.NamedTuple):
    schema: str | None
    table: str
    key_cols: list[str]
    alias: str
    joins: dict[str, SQLJoinOpt]
    filter_conds: list[SQLFragmentElement]
    order_by: list[SQLOrderByOpt]


# sessions


class SessionBackend(typing.Protocol):
    async def select_by_keys(
        self,
        schema: str | None,
        table: str,
        where_attrs: Collection[str],
        select_attrs: Collection[str],
        values: Collection[Rec],
    ) -> list[Rec]: ...
    async def insert_records(
        self,
        schema: str | None,
        table: str,
        insert_attrs: Collection[str],
        returning_attrs: Collection[str],
        values: Collection[Rec],
    ) -> list[Rec]: ...
    async def update_records(
        self,
        schema: str | None,
        table: str,
        where_attrs: Collection[str],
        set_attrs: Collection[str],
        returning_attrs: Collection[str],
        values: Collection[Rec],
    ) -> list[Rec]: ...
    async def delete_by_keys(self, schema: str | None, table: str, attrs: Collection[str], values: Collection[Rec]): ...
    async def fetch(self, query: str, *params: PostgresCodable) -> list[Rec]: ...
    async def fetch_query(self, query: StructuredQuery, limit: int | None, offset: int | None) -> list[Rec]: ...
    async def count_query(self, query: StructuredQuery) -> int: ...
    async def paginate_query(
        self,
        query: StructuredQuery,
        first: int | None,
        after: Rec | None,
        last: int | None,
        before: Rec | None,
        offset: int | None,
    ) -> tuple[list[Rec], bool, bool]: ...
    async def fetch_raw_query(self, query: SQLFragment) -> list[Rec]: ...
    async def begin(self): ...
    async def commit(self): ...
    async def rollback(self): ...


entity_mapping_registry: dict[type, EntityMapping] = {}


def register_mapping(mapping: EntityMapping):
    entity_mapping_registry[mapping.entity_type] = mapping


def lookup_mapping(type: type) -> EntityMapping:
    return entity_mapping_registry[type]


TEntity = typing.TypeVar("TEntity", bound=Entity)


class Session:
    def __init__(self, backend: SessionBackend, mappings: Collection[EntityMapping] | None = None):
        self._backend = backend
        self._identity_map = dict[tuple[type, KeyValue], object]()
        self._children_map = dict[tuple[type, KeyValue], set[KeyValue]]()

        if mappings is None:
            mappings = [*entity_mapping_registry.values(), *auto.build()]
        self.mappings = {mapping.entity_type: mapping for mapping in mappings}

    async def get(self, entity_type: Type[TEntity], id: KeyValue) -> TEntity | None:
        return (await self.batch_get(entity_type, (id,)))[0]

    async def batch_get(self, entity_type: Type[TEntity], ids: Collection[KeyValue]) -> list[TEntity | None]:
        mapping = self.mappings[entity_type]
        found = dict(await self._get_by_key(mapping, mapping.primary_key, ids))
        self._track(mapping, found.values())
        return [typing.cast(TEntity, found.get(v)) for v in ids]

    async def save(self, entity: Entity):
        await self.batch_save(type(entity), entity)

    async def batch_save(self, entity_type: Type[TEntity], *entities: TEntity):
        mapping = self.mappings[entity_type]
        values = [(mapping.identify_entity(e), e) for e in entities]
        await self._save_by_key(mapping, mapping.primary_key, values)
        self._track(mapping, entities)

    async def delete(self, entity: Entity):
        await self.batch_delete(type(entity), entity)

    async def batch_delete(self, entity_type: Type[TEntity], *entities: TEntity):
        mapping = self.mappings[entity_type]
        ids = [mapping.identify_entity(ent) for ent in entities]
        await self._delete_by_key(mapping, mapping.primary_key, ids)
        self._untrack(mapping, ids)

    def query(self, entity_type: Type[TEntity], alias: str):
        return SessionEntityQuery[TEntity](self, entity_type, alias)

    def raw(self, query: str, **params):
        return SessionRawQuery(self, query, params)

    async def begin(self):
        await self._backend.begin()

    async def commit(self):
        await self._backend.commit()

    async def rollback(self):
        await self._backend.rollback()

    async def _get_by_key(self, mapping: EntityMapping, key: Key, values: Collection[KeyValue]):
        records = await self._backend.select_by_keys(
            schema=mapping.schema,
            table=mapping.table,
            where_attrs=key.columns,
            select_attrs={c: True for c in (*mapping.columns, *key.columns)},
            values=[{**key.to_partial(kv)} for kv in values],
        )
        record_map = {mapping.identify_record(rec): rec for rec in records}

        found = dict[KeyValue, Entity]()
        for id, full in record_map.items():
            entity = self._get_entity_in_track(mapping, id) or mapping.entity_factory()
            mapping.write_record(entity, full)
            found[id] = entity

        for child in mapping.children:
            child_entities = {parent_key: list[Entity]() for parent_key in found}
            children_found = await self._get_by_key(
                self.mappings[child.target], child.parental_key(mapping.primary_key), child_entities
            )
            for key_value, entity in children_found:
                child_entities[key_value].append(entity)

            for parent_id in found:
                child.set_entity_attribute(found[parent_id], child_entities[parent_id])

        return [(key.from_partial(full), found[id]) for id, full in record_map.items()]

    async def _save_by_key(self, mapping: EntityMapping, key: Key, values: Collection[tuple[KeyValue, Entity]]):
        to_insert = list[tuple[KeyValue, Entity]]()
        to_update = list[tuple[KeyValue, Entity]]()
        for value, entity in values:
            id = mapping.identify_entity(entity)
            if self._in_track(mapping, id):
                to_update.append((value, entity))
            else:
                to_insert.append((value, entity))

        pairs = list[tuple[Entity, Rec]]()
        all_cols = {c: True for c in (*mapping.columns, *key.columns)}

        if to_update:
            cols_skipped = {f.column for f in mapping.fields if f.skip_on_update}
            updated = await self._backend.update_records(
                schema=mapping.schema,
                table=mapping.table,
                where_attrs={c: True for c in (*key.columns, *mapping.primary_key.columns)},
                set_attrs=[c for c in all_cols if c not in cols_skipped and c not in key.columns],
                returning_attrs=all_cols,
                values=[{**mapping.to_record(ev), **key.to_partial(kv)} for kv, ev in to_update],
            )
            pairs.extend((e, v) for (_, e), v in zip(to_update, updated))
        if to_insert:
            cols_skipped = {f.column for f in mapping.fields if f.skip_on_insert}
            inserted = await self._backend.insert_records(
                schema=mapping.schema,
                table=mapping.table,
                insert_attrs=[col for col in all_cols if col not in cols_skipped],
                returning_attrs=all_cols,
                values=[{**mapping.to_record(ev), **key.to_partial(kv)} for kv, ev in to_insert],
            )
            pairs.extend((e, v) for (_, e), v in zip(to_insert, inserted))

        saved = dict[KeyValue, Entity]()
        for entity, full in pairs:
            id = mapping.identify_record(full)
            mapping.write_record(entity, full)
            saved[id] = entity

        for child in mapping.children:
            child_mapping = self.mappings[child.target]
            previous_ids = self._get_children_ids_in_track(child, saved)
            new_ids = {
                child_mapping.identify_entity(c)
                for parent in saved.values()
                for c in child.get_entity_attribute(parent)
            }
            await self._delete_by_key(child_mapping, child_mapping.primary_key, previous_ids - new_ids)

            to_save = [(pid, c) for pid, parent in saved.items() for c in child.get_entity_attribute(parent)]
            await self._save_by_key(child_mapping, child.parental_key(mapping.primary_key), to_save)

        return values

    async def _delete_by_key(self, mapping: EntityMapping, key: Key, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_mapping = self.mappings[child.target]
            child_ids = self._get_children_ids_in_track(child, ids)
            await self._delete_by_key(child_mapping, child_mapping.primary_key, child_ids)

        values = [key.to_partial(id) for id in ids]
        await self._backend.delete_by_keys(mapping.schema, mapping.table, key.columns, values)

    def _track(self, mapping: EntityMapping, entities: Collection[Entity]):
        entity_map = {mapping.identify_entity(entity): entity for entity in entities}

        for id, entity in entity_map.items():
            self._identity_map[(mapping.entity_type, id)] = entity

        for child in mapping.children:
            new_ids = set[KeyValue]()
            child_mapping = self.mappings[child.target]

            for id, entity in entity_map.items():
                child_entities = child.get_entity_attribute(entity)
                child_ids = {child_mapping.identify_entity(child_obj) for child_obj in child_entities}

                self._children_map[(child.target, id)] = child_ids
                self._track(child_mapping, child_entities)
                new_ids.update(child_ids)

            previous_ids = self._get_children_ids_in_track(child, entity_map)
            self._untrack(child_mapping, previous_ids - new_ids)

    def _untrack(self, mapping: EntityMapping, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_ids = self._get_children_ids_in_track(child, ids)
            child_mapping = self.mappings[child.target]
            self._untrack(child_mapping, child_ids)
            for id in ids:
                del self._children_map[(child.target, id)]

        for id in ids:
            del self._identity_map[(mapping.entity_type, id)]

    def _in_track(self, mapping: EntityMapping, id: KeyValue) -> bool:
        return (mapping.entity_type, id) in self._identity_map

    def _get_entity_in_track(self, mapping: EntityMapping, id: KeyValue):
        return self._identity_map.get((mapping.entity_type, id))

    def _get_children_ids_in_track(self, child: Child, ids: Collection[KeyValue]):
        return {child_id for id in ids for child_id in self._children_map.get((child.target, id), ())}


class SessionEntityQuery(typing.Generic[TEntity]):
    def __init__(self, session: "Session", entity_type: type, alias: str):
        self._session = session
        self._mapping = session.mappings[entity_type]
        self._query = StructuredQuery(
            schema=self._mapping.schema,
            table=self._mapping.table,
            key_cols=list(self._mapping.primary_key.columns),
            alias=alias,
            joins={},
            filter_conds=[],
            order_by=[],
        )

    def join(self, target: type | str, alias: str, on: str, **params):
        self._query.joins[alias] = SQLJoinOpt("JOIN", self._get_target(target, params), alias, _sql_parse(on, params))
        return self

    def left_join(self, target: type | str, alias: str, on: str, **params):
        self._query.joins[alias] = SQLJoinOpt(
            "LEFT JOIN", self._get_target(target, params), alias, _sql_parse(on, params)
        )
        return self

    def filter(self, condition: str, **params):
        self._query.filter_conds.append(_sql_parse(condition, params))
        return self

    def order_by(self, *order_by: SQLOrderByOpt):
        self._query.order_by.clear()
        self._query.order_by.extend(order_by)
        return self

    def _get_target(self, target: type | str, params: dict):
        if isinstance(target, str):
            return _sql_parse(target, params)
        target_mapping = self._session.mappings[target]
        return SQLName(prefix=target_mapping.schema, name=target_mapping.table)

    async def count(self):
        return await self._session._backend.count_query(self._query)

    async def fetch(self, limit: int | None = None, offset: int | None = None):
        records = await self._session._backend.fetch_query(self._query, limit, offset)
        primary_key_values = [self._mapping.identify_record(rec) for rec in records]
        entities = await self._session.batch_get(self._mapping.entity_type, primary_key_values)
        return typing.cast(list[TEntity], entities)

    class Page(typing.NamedTuple):
        ids: list[KeyValue]
        has_previous_page: bool
        has_next_page: bool

    async def paginate(
        self,
        first: int | None = None,
        after: KeyValue | None = None,
        last: int | None = None,
        before: KeyValue | None = None,
        offset: int | None = None,
    ):
        query = StructuredQuery(
            schema=self._query.schema,
            table=self._query.table,
            key_cols=self._query.key_cols.copy(),
            alias=self._query.alias,
            joins=self._query.joins.copy(),
            filter_conds=self._query.filter_conds.copy(),
            order_by=self._query.order_by.copy(),
        )
        query.order_by.extend(SQLOrderByOpt(SQLName(self._query.alias, col)) for col in self._query.key_cols)

        records, has_previous_page, has_next_page = await self._session._backend.paginate_query(
            query=query,
            first=first,
            after=self._mapping.primary_key.to_partial(after) if after is not None else None,
            last=last,
            before=self._mapping.primary_key.to_partial(before) if before is not None else None,
            offset=offset,
        )
        ids = [self._mapping.identify_record(rec) for rec in records]
        return self.Page(ids, has_previous_page, has_next_page)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None

    @classmethod
    def asc(cls, expr: str, nulls_last=True, **params):
        return SQLOrderByOpt(_sql_parse(expr, params), True, nulls_last)

    @classmethod
    def desc(cls, expr: str, nulls_last=True, **params):
        return SQLOrderByOpt(_sql_parse(expr, params), False, nulls_last)


class SessionRawQuery:
    def __init__(self, session: "Session", query: str, params: dict[str, PostgresCodable]):
        self._frag = _sql_parse(query, params)
        self._session = session

    async def fetch(self):
        return await self._session._backend.fetch_raw_query(self._frag)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None


# adapter


class AsyncPGSessionBackend(SessionBackend):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._active: asyncpg.Pool | asyncpg.Connection = pool
        self._tx: asyncpg.transaction.Transaction | None = None

    async def select_by_keys(
        self,
        schema: str | None,
        table: str,
        where_attrs: Collection[str],
        select_attrs: Collection[str],
        values: Collection[Rec],
    ):
        with_v = self._sql_with_v(schema, table, where_attrs)
        select_list = self._sql_l(self._sql_qn("t", name) for name in select_attrs)
        from_table = self._sql_qn(schema, table)
        join_cols = self._sql_l(self._sql_q(col) for col in where_attrs)
        query = f"{with_v} SELECT {select_list} FROM {from_table} t JOIN v USING ({join_cols});"
        params = [[cvm[i] for cvm in values] for i in where_attrs]

        records = await self._active.fetch(query, *params)
        return [dict(zip(select_attrs, record)) for record in records]

    async def insert_records(
        self,
        schema: str | None,
        table: str,
        insert_attrs: Collection[str],
        returning_attrs: Collection[str],
        values: Collection[Rec],
    ):
        with_v = self._sql_with_v(schema, table, insert_attrs)
        table_name = self._sql_qn(schema, table)
        insert_cols = self._sql_l(self._sql_q(n) for n in insert_attrs)
        returning_cols = self._sql_l(self._sql_q(n) for n in returning_attrs)
        query = (
            f"{with_v} INSERT INTO {table_name} ({insert_cols}) "
            f"SELECT {insert_cols} FROM v RETURNING {returning_cols};"
        )
        params = [[cvm[i] for cvm in values] for i in insert_attrs]

        records = await self._active.fetch(query, *params)

        return [dict(zip(returning_attrs, record)) for record in records]

    async def update_records(
        self,
        schema: str | None,
        table: str,
        where_attrs: Collection[str],
        set_attrs: Collection[str],
        returning_attrs: Collection[str],
        values: Collection[Rec],
    ):
        value_attrs = [*where_attrs, *set_attrs]
        with_v = self._sql_with_v(schema, table, value_attrs)
        update = self._sql_qn(schema, table)
        set_left = self._sql_l(self._sql_q(name) for name in set_attrs)
        set_right = self._sql_l(self._sql_qn("v", name) for name in set_attrs)
        if len(set_attrs) > 1:
            set_left = f"({set_left})"
            set_right = f"({set_right})"

        where_left = self._sql_l(self._sql_qn("t", name) for name in where_attrs)
        where_right = self._sql_l(self._sql_qn("v", name) for name in where_attrs)
        returning = self._sql_l(self._sql_qn("t", name) for name in returning_attrs)

        query = (
            f"{with_v} UPDATE {update} t SET {set_left} = {set_right} "
            f"FROM v WHERE ({where_left}) = ({where_right}) RETURNING {returning};"
        )
        params = [[cvm[i] for cvm in values] for i in value_attrs]
        records = await self._active.fetch(query, *params)

        return [dict(zip(returning_attrs, record)) for record in records]

    async def delete_by_keys(self, schema: str | None, table: str, attrs: Collection[str], values: Collection[Rec]):
        with_v = self._sql_with_v(schema, table, attrs)
        delete_from = self._sql_qn(schema, table)
        where_left = self._sql_l(self._sql_qn("t", name) for name in attrs)
        where_right = self._sql_l(self._sql_qn("v", name) for name in attrs)

        query = f"{with_v} DELETE FROM {delete_from} t USING v WHERE ({where_left}) = ({where_right});"
        params = [[cvm[i] for cvm in values] for i in attrs]
        await self._active.fetch(query, *params)

    async def fetch_query(self, query: StructuredQuery, limit: int | None, offset: int | None) -> list[Rec]:
        sql, params = self._sql_select(
            select_items=[SQLName(query.alias, n) for n in query.key_cols],
            from_item=SQLName(query.schema, query.table),
            from_as=query.alias,
            joins=query.joins,
            group_by=[SQLName(query.alias, n) for n in query.key_cols],
            having=query.filter_conds,
            limit=limit,
            offset=offset,
            order_by=query.order_by,
        )
        records = await self._active.fetch(sql, *params)
        return [dict(zip(query.key_cols, record)) for record in records]

    async def paginate_query(
        self,
        query: StructuredQuery,
        first: int | None,
        after: Rec | None,
        last: int | None,
        before: Rec | None,
        offset: int | None,
    ):
        if last is None:
            order_by = query.order_by
        else:
            order_by = [SQLOrderByOpt(o.expr, not o.ascending, not o.nulls_last) for o in query.order_by]

        if after:
            after_predicate = await self._get_cursor_predicate(query, after, True)
        else:
            after_predicate = None
        if before:
            before_predicate = await self._get_cursor_predicate(query, before, False)
        else:
            before_predicate = None

        filter_conds = query.filter_conds.copy()
        if after_predicate:
            filter_conds.append(after_predicate)
        if before_predicate:
            filter_conds.append(before_predicate)

        limit = first if last is None else last
        sql, params = self._sql_select(
            select_items=[SQLName(query.alias, n) for n in query.key_cols],
            from_item=SQLName(query.schema, query.table),
            from_as=query.alias,
            joins=query.joins,
            group_by=[SQLName(query.alias, n) for n in query.key_cols],
            having=filter_conds,
            order_by=order_by,
            offset=offset,
            limit=limit + 1 if limit else None,
        )
        records = await self._active.fetch(sql, *params)
        keys = [dict(zip(query.key_cols, record)) for record in records[:limit]]

        if last is None:
            has_previous_page = bool(after_predicate)
            has_next_page = len(records) > limit if limit else False
        else:
            has_previous_page = len(records) > limit if limit else False
            has_next_page = bool(before_predicate)
            keys.reverse()

        return keys, has_previous_page, has_next_page

    async def _get_cursor_predicate(self, query: StructuredQuery, cursor: Rec, forward: bool):
        cursor_conds = [_sql_literal(SQLName(query.alias, k), " = ", SQLVar(cursor[k])) for k in cursor]
        sql, params = self._sql_select(
            select_items=[o.expr for o in query.order_by],
            from_item=SQLName(query.schema, query.table),
            from_as=query.alias,
            joins=query.joins,
            group_by=[SQLName(query.alias, n) for n in query.key_cols],
            having=query.filter_conds + cursor_conds,
        )

        if records := await self._active.fetch(sql, *params):
            cursor_vars = [SQLVar(v) for v in records[0]]
        else:
            return None

        or_predicates: list[SQLFragmentElement] = []
        for i, _ in enumerate(query.order_by):
            and_predicates: list[SQLFragmentElement] = []
            for j, sort in enumerate(query.order_by[: i + 1]):
                v = cursor_vars[j]
                op = " > " if sort.ascending == forward else " < "
                if i != j:
                    pred = _sql_literal(sort.expr, " IS NOT DISTINCT FROM ", v)
                elif sort.nulls_last == forward:
                    pred = _sql_literal("(", sort.expr, op, v, " IS NOT FALSE AND ", v, " IS NOT NULL)")
                else:
                    pred = _sql_literal("(", sort.expr, op, v, " IS NOT FALSE AND ", sort.expr, " IS NOT NULL)")

                and_predicates.append(pred)
            or_predicates.append(_sql_join(" AND ", and_predicates))

        return _sql_join(" OR ", or_predicates)

    async def count_query(self, query: StructuredQuery) -> int:
        sql, params = self._sql_select(
            select_items=[SQLName(query.alias, n) for n in query.key_cols],
            from_item=SQLName(query.schema, query.table),
            from_as=query.alias,
            joins=query.joins,
            group_by=[SQLName(query.alias, n) for n in query.key_cols],
            having=query.filter_conds,
        )
        sql = f"SELECT COUNT(*) FROM ({sql}) _;"
        records = await self._active.fetch(sql, *params)
        return records[0][0]

    async def fetch_raw_query(self, query: SQLFragment) -> list[Rec]:
        params = dict[SQLVar, int]()
        sql = "".join(self._sql_el(el, params) for el in query.elements)
        return await self._active.fetch(sql, *[var.value for var in params])

    async def fetch(self, query: str, *params) -> list[Rec]:
        return await self._active.fetch(query, *params)

    async def begin(self):
        assert not self._tx
        conn: asyncpg.Connection = await self._pool.acquire()
        tx: asyncpg.transaction.Transaction = conn.transaction()
        await tx.start()
        self._active = conn
        self._tx = tx

    async def commit(self):
        assert self._tx
        await self._tx.commit()
        await self._pool.release(self._active)
        self._active = self._pool
        self._tx = None

    async def rollback(self):
        assert self._tx
        await self._tx.rollback()
        await self._pool.release(self._active)
        self._active = self._pool
        self._tx = None

    def _sql_select(
        self,
        select_items: Collection[SQLFragmentElement],
        from_item: SQLFragmentElement,
        from_as: str,
        joins: dict[str, SQLJoinOpt],
        group_by: Collection[SQLFragmentElement],
        having: list[SQLFragmentElement],
        limit: int | None = None,
        offset: int | None = None,
        order_by: Sequence[SQLOrderByOpt] = (),
    ):
        params = dict[SQLVar, int]()

        sql = f"SELECT {self._sql_l(self._sql_el(i, params) for i in select_items)}"
        sql += f" FROM {self._sql_el(from_item, params)} AS {self._sql_q(from_as)}"
        for opt in joins:
            sql += " " + self._sql_join(joins[opt], params)

        sql += f" GROUP BY {self._sql_l(self._sql_el(i, params) for i in group_by)}"
        if having:
            sql += f" HAVING " + " AND ".join(f"({self._sql_el(cond, params)})" for cond in having)
        if len(order_by) > 0:
            sql += " ORDER BY " + ", ".join(self._sql_order(opt, params) for opt in order_by)
        if isinstance(limit, int):
            sql += f" LIMIT {self._sql_el(SQLVar(limit), params)} "
        if isinstance(offset, int):
            sql += f" OFFSET {self._sql_el(SQLVar(offset), params)}"

        return sql, [var.value for var in params]

    def _sql_join(self, opt: SQLJoinOpt, params: dict[SQLVar, int]):
        return (
            f"{opt.type} {self._sql_el(opt.target, params)} {self._sql_q(opt.alias)} ON {self._sql_el(opt.on, params)}"
        )

    def _sql_order(self, opt: SQLOrderByOpt, params: dict[SQLVar, int]):
        return f"{self._sql_el(opt.expr, params)} {'ASC' if opt.ascending else 'DESC'} {'NULLS LAST' if opt.nulls_last else 'NULLS FIRST'}"

    def _sql_with_v(self, schema: str | None, table: str, cols: Collection[str]):
        select_items = self._sql_l(self._sql_qn(None, n) for n in cols)
        unnest_args = self._sql_l(
            f"COALESCE(${i + 1}, array[(null::{self._sql_qn(schema, table)}).{self._sql_q(col)}])"
            for i, col in enumerate(cols)
        )
        return f"WITH v AS (SELECT * FROM UNNEST({unnest_args}) v({select_items}))"

    def _sql_l(self, items: typing.Iterable[str]):
        return ", ".join(items)

    def _sql_qn(self, prefix: str | None, name: str):
        if prefix:
            return self._sql_q(prefix) + "." + self._sql_q(name)
        return self._sql_q(name)

    def _sql_q(self, name: str):
        return '"' + name.replace('"', '""') + '"'

    def _sql_el(self, element: SQLFragmentElement, inout_params: dict[SQLVar, int]) -> str:
        if isinstance(element, SQLText):
            return element.text
        elif isinstance(element, SQLVar):
            if element not in inout_params:
                inout_params[element] = len(inout_params)
            return f"${inout_params[element] + 1}"
        elif isinstance(element, SQLName):
            return self._sql_qn(element.prefix, element.name)
        elif isinstance(element, SQLFragment):
            return "".join(self._sql_el(el, inout_params) for el in element.elements)


# mapped


class AutoMappingBuilder:
    class FieldConfig(typing.TypedDict, total=False):
        column: str
        primary: bool
        skip_on_insert: bool
        skip_on_update: bool

    class ChildConfig(typing.TypedDict):
        kind: typing.Literal["singular", "plural"]
        columns: list[str]
        target: type

    FieldConfigMap = dict[str, "AutoMappingBuilder.FieldConfig"]
    ChildConfigMap = dict[str, "AutoMappingBuilder.ChildConfig"]

    class EntityMappingConfig(typing.TypedDict, total=False):
        schema: str
        table: str
        fields: "typing.Callable[[], AutoMappingBuilder.FieldConfigMap] | AutoMappingBuilder.FieldConfigMap"
        children: "typing.Callable[[], AutoMappingBuilder.ChildConfigMap] | AutoMappingBuilder.ChildConfigMap"
        factory: EntityFactory

    _configs = dict[type, EntityMappingConfig]()
    _mappings = dict[type, EntityMapping]()

    def mapped(self, **kwargs: typing.Unpack[EntityMappingConfig]):
        def wrapper(entity_type):
            self._configs[entity_type] = kwargs
            return entity_type

        return wrapper

    def build(self):
        mappings = list[EntityMapping]()
        for cls, opts in self._configs.items():
            if cls in self._mappings:
                mappings.append(self._mappings[cls])
            else:
                mapping = self._build_entity_mapping(cls, opts)
                mappings.append(mapping)
                self._mappings[cls] = mapping

        return mappings

    def _build_entity_mapping(self, entity_type: type, opts: EntityMappingConfig) -> EntityMapping:
        field_configs_thunk = opts.get("fields", {})
        child_configs_thunk = opts.get("children", {})
        field_configs = field_configs_thunk() if callable(field_configs_thunk) else field_configs_thunk
        child_configs = child_configs_thunk() if callable(child_configs_thunk) else child_configs_thunk

        fields = {
            name: Field(
                name=name,
                column=config["column"] if "column" in config else self._column_name(name),
                skip_on_insert=config["skip_on_insert"] if "skip_on_insert" in config else False,
                skip_on_update=config["skip_on_update"] if "skip_on_update" in config else False,
            )
            for name, config in field_configs.items()
        }
        children = {
            name: Child(name, config["target"], config["columns"], config["kind"] == "singluar")
            for name, config in child_configs.items()
        }
        primary = [f for f, config in field_configs.items() if config.get("primary", False)] or ["id"]

        annotations = typing.get_type_hints(entity_type)
        for name, type_hint in annotations.items():
            origin = typing.get_origin(type_hint)
            args = typing.get_args(type_hint)
            # skip private fields
            if name.startswith("_"):
                continue
            # skip registered fields
            elif name in fields or name in children:
                continue
            # list of registered entity
            elif origin is list and args[0] in self._configs:
                children[name] = Child(name, args[0], self._parental_key_name(entity_type.__name__, primary), False)
            # registered entity
            elif type_hint in self._configs:
                children[name] = Child(name, type_hint, self._parental_key_name(entity_type.__name__, primary), True)
            # optional of registered entity
            elif origin == types.UnionType and len(args) == 2 and args[1] is type(None) and args[0] in self._configs:
                children[name] = Child(name, args[0], self._parental_key_name(entity_type.__name__, primary), True)
            else:
                is_optional = origin == types.UnionType and len(args) == 2 and args[1] is type(None)
                fields[name] = Field(
                    name=name,
                    column=self._column_name(name),
                    # optional primary key is skipped on insert
                    skip_on_insert=name in primary and is_optional,
                    # primary is skipped on update
                    skip_on_update=name in primary,
                )

        return EntityMapping(
            entity_type=entity_type,
            schema=opts.get("schema"),
            table=opts.get("table", self._table_name(entity_type.__name__)),
            fields=list(fields.values()),
            children=list(children.values()),
            entity_factory=opts.get("factory", lambda: object.__new__(entity_type)),
            primary_key=PrimaryKey([fields[p] for p in primary]),
        )

    def _parental_key_name(self, s: str, field_names: typing.Sequence[str]) -> list[str]:
        return [self._table_name(s) + "_" + col for col in field_names]

    def _column_name(self, s: str) -> str:
        return self._table_name(s)

    def _table_name(self, s: str, patt=re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")):
        return patt.sub("_", s).lower()


auto = AutoMappingBuilder()
