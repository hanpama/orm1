import typing
import types
import re
from typing import Collection, Sequence, Type

import asyncpg

# mapping

Entity = object
KeyValue = typing.Hashable
PostgresCodable = typing.Any
Rec = dict[str, PostgresCodable]
EntityFactory = typing.Callable[[], Entity]


class Field:
    def __init__(self, name: str, column: str, is_primary=False):
        self.name = name
        self.column = column
        self.primary = is_primary

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

    def __repr__(self) -> str:
        return f"Field({self.name!r}: {self.column!r}, primary={self.primary})"


class Key(typing.Protocol):
    columns: Sequence[str]

    def from_partial(self, partial: Rec) -> KeyValue: ...
    def to_partial(self, value: KeyValue) -> Rec: ...


class SimpleKey(Key):
    def __init__(self, field: Field):
        self.field = field
        self.columns = [field.column]

    def from_partial(self, partial: Rec) -> KeyValue:
        return self.field.from_partial(partial)

    def to_partial(self, value: KeyValue) -> Rec:
        return self.field.to_partial(value)

    def get_from_entity(self, entity: Entity) -> KeyValue:
        return self.field.get_from_entity(entity)


class CompositeKey(Key):
    def __init__(self, fields: Sequence[Field]):
        self.fields = fields
        self.columns = [f.column for f in fields]

    def from_partial(self, partial: Rec) -> KeyValue:
        return tuple(field.from_partial(partial) for field in self.fields)

    def to_partial(self, value: KeyValue) -> Rec:
        assert isinstance(value, tuple)
        partial = {}
        for field, val in zip(self.fields, value or ()):
            partial.update(field.to_partial(val))
        return partial

    def get_from_entity(self, entity: Entity) -> KeyValue:
        return tuple(field.get_from_entity(entity) for field in self.fields)


PrimaryKey = SimpleKey | CompositeKey


class ParentalKey(Key):
    def __init__(self, columns: Sequence[str], parent_key: Key):
        self.columns = columns
        self._parent_key = parent_key

    def from_partial(self, partial: Rec) -> KeyValue:
        rec = {pc: partial[c] for c, pc in zip(self.columns, self._parent_key.columns)}
        return self._parent_key.from_partial(rec)

    def to_partial(self, value: KeyValue) -> Rec:
        rec = self._parent_key.to_partial(value)
        return {c: rec[pc] for c, pc in zip(self.columns, self._parent_key.columns)}


class Child:
    def __init__(self, name: str, target: type[Entity], columns: Sequence[str]):
        self.name = name
        self.target = target
        self._columns = columns

    def get_entity_attribute(self, entity: Entity) -> Sequence[Entity]: ...
    def set_entity_attribute(self, entity: Entity, values: list[Entity]): ...

    def parental_key(self, parent_key: Key) -> ParentalKey:
        return ParentalKey(self._columns, parent_key)


class Plural(Child):
    def get_entity_attribute(self, entity: Entity) -> Sequence[Entity]:
        val = getattr(entity, self.name)
        return tuple(val) if val else ()

    def set_entity_attribute(self, entity: Entity, values: Sequence[Entity]):
        setattr(entity, self.name, list(values))


class Singular(Child):
    def get_entity_attribute(self, entity: Entity) -> Sequence[Entity]:
        val = getattr(entity, self.name)
        return (val,) if val else ()

    def set_entity_attribute(self, entity: Entity, values: Sequence[Entity]):
        setattr(entity, self.name, values[0] if len(values) else None)


class EntityMapping:
    def __init__(
        self,
        entity_type: Type[Entity],
        table: str,
        schema: str | None,
        fields: list[Field],
        children: list[Child],
        primary_key: PrimaryKey,
        entity_factory: EntityFactory,
    ):
        self.entity_type = entity_type
        self.table = table
        self.schema = schema
        self.fields = fields
        self.children = children
        self.primary_key = primary_key
        self.entity_factory = entity_factory

    @classmethod
    def define(
        cls,
        entity: Type[Entity],
        table: str,
        schema: str | None,
        fields: list[Field] = [],
        children: list[Child] = [],
        entity_factory: EntityFactory | None = None,
    ):
        primary_fields = [f for f in fields if f.primary]
        if len(primary_fields) > 1:
            primary_key = CompositeKey(primary_fields)
        elif len(primary_fields) == 1:
            primary_key = SimpleKey(primary_fields[0])
        else:
            raise Exception(f"No primary key defined: {entity.__name__}")

        if not entity_factory:
            entity_factory = cls.default_factory(entity)

        return cls(entity, table, schema, fields, children, primary_key, entity_factory)

    @classmethod
    def default_factory(cls, entity_type: Type[Entity]):
        return lambda: object.__new__(entity_type)

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


# sessions


class SessionBackend(typing.Protocol):
    async def select_by_keys(self, mapping: EntityMapping, key: Key, key_values: Collection[KeyValue]) -> list[Rec]: ...
    async def insert_records(
        self, mapping: EntityMapping, key: Key, entities: Collection[tuple[KeyValue, Entity]]
    ) -> list[Rec]: ...
    async def update_records(
        self, mapping: EntityMapping, key: Key, values: Collection[tuple[KeyValue, Entity]]
    ) -> list[Rec]: ...
    async def delete_by_keys(self, mapping: EntityMapping, key: Key, key_values: Collection[KeyValue]): ...
    async def fetch(self, query: str, *params: PostgresCodable) -> list[Rec]: ...
    async def fetch_structured_query(self, query: "StructuredQuery", limit: int | None, offset: int) -> list[Rec]: ...
    async def count_structured_query(self, query: "StructuredQuery") -> int: ...
    async def fetch_raw_query(self, query: "SQLFragment") -> list[Rec]: ...


entity_mapping_registry: dict[type, EntityMapping] = {}


def register_mapping(mapping: EntityMapping):
    entity_mapping_registry[mapping.entity_type] = mapping


def lookup_mapping(type: type) -> EntityMapping:
    return entity_mapping_registry[type]


TEntity = typing.TypeVar("TEntity", bound=Entity)


class Session:
    def __init__(
        self,
        backend: SessionBackend,
        mappings: Collection[EntityMapping] | None = None,
    ):
        self._backend = backend
        self._identity_map = dict[tuple[EntityMapping, KeyValue], object]()
        self._children_map = dict[tuple[Child, KeyValue], set[KeyValue]]()

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
        await self._save_by_key(
            mapping,
            mapping.primary_key,
            [(mapping.identify_entity(e), e) for e in entities],
        )
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

    async def _get_by_key(self, mapping: EntityMapping, key: Key, values: Collection[KeyValue]):
        records = await self._backend.select_by_keys(mapping, key, values)
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
        if to_update:
            updated = await self._backend.update_records(mapping, key, to_update)
            pairs.extend((e, v) for (_, e), v in zip(to_update, updated))
        if to_insert:
            inserted = await self._backend.insert_records(mapping, key, to_insert)
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

        await self._backend.delete_by_keys(mapping, key, ids)

    def _track(self, mapping: EntityMapping, entities: Collection[Entity]):
        entity_map = {mapping.identify_entity(entity): entity for entity in entities}

        for id, entity in entity_map.items():
            self._identity_map[(mapping, id)] = entity

        for child in mapping.children:
            new_ids = set[KeyValue]()
            child_mapping = self.mappings[child.target]

            for id, entity in entity_map.items():
                child_entities = child.get_entity_attribute(entity)
                child_ids = {child_mapping.identify_entity(child_obj) for child_obj in child_entities}

                self._children_map[(child, id)] = child_ids
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
                del self._children_map[(child, id)]

        for id in ids:
            del self._identity_map[(mapping, id)]

    def _in_track(self, mapping: EntityMapping, id: KeyValue) -> bool:
        return (mapping, id) in self._identity_map

    def _get_entity_in_track(self, mapping: EntityMapping, id: KeyValue):
        return self._identity_map.get((mapping, id))

    def _get_children_ids_in_track(self, child: Child, ids: Collection[KeyValue]):
        return {child_id for id in ids for child_id in self._children_map.get((child, id), ())}


# SQL Object Model


class SQLText(typing.NamedTuple):
    text: str


class SQLVar(typing.NamedTuple):
    value: PostgresCodable
    type: str | None


class SQLName(typing.NamedTuple):
    prefix: str | None
    name: str


class SQLFragment:
    def __init__(self, *elements: "SQLFragmentElement"):
        self._elements = elements

    _patt_word = re.compile(r"('[^']*'|\"[^\"]*\"|\s+|::|:\w+|\w+|[^\w\s])")
    _patt_param = re.compile(r":(\w+)")

    @classmethod
    def parse(cls, sql: str, params: dict[str, SQLVar | PostgresCodable]):
        tokens = list[SQLText | SQLVar]()
        words: list[str] = cls._patt_word.findall(sql)

        for word in words:
            if matched := cls._patt_param.match(word):
                if matched[1] in params:
                    val = params[matched[1]]
                    if isinstance(val, SQLVar):
                        tokens.append(val)
                    else:
                        tokens.append(SQLVar(val, None))
                else:
                    raise Exception(f"Parameter '{matched[1]}' not provided")
            else:
                tokens.append(SQLText(word))

        return cls(*tokens)


SQLFragmentElement = SQLText | SQLVar | SQLName | SQLFragment


class StructuredQuery(typing.NamedTuple):
    JoinType = typing.Literal["JOIN", "LEFT JOIN"]

    class Join(typing.NamedTuple):
        type: "StructuredQuery.JoinType"
        target: SQLFragment
        alias: str
        on: SQLFragment

    class OrderByOption(typing.NamedTuple):
        expr: SQLFragment
        ascending: bool
        nulls_last: bool

    mapping: EntityMapping
    alias: str
    joins: dict[str, Join]
    filter_conds: list[SQLFragment]
    order_by_options: list[OrderByOption] = []


class SessionEntityQuery(typing.Generic[TEntity]):
    def __init__(self, session: "Session", entity_type: type, alias: str):
        self._session = session
        self._mapping = session.mappings[entity_type]
        self._query = StructuredQuery(mapping=self._mapping, alias=alias, joins={}, filter_conds=[])

    def join(self, target: type | str, alias: str, on: str, **params):
        self._query.joins[alias] = StructuredQuery.Join(
            "JOIN", self._get_target(target, params), alias, SQLFragment.parse(on, params)
        )
        return self

    def left_join(self, target: type | str, alias: str, on: str, **params):
        self._query.joins[alias] = StructuredQuery.Join(
            "LEFT JOIN", self._get_target(target, params), alias, SQLFragment.parse(on, params)
        )
        return self

    def filter(self, condition: str, **params):
        self._query.filter_conds.append(SQLFragment.parse(condition, params))
        return self

    def order_by(self, *options: StructuredQuery.OrderByOption):
        self._query.order_by_options.clear()
        self._query.order_by_options.extend(options)
        return self

    def _get_target(self, target: type | str, params: dict):
        if isinstance(target, str):
            return SQLFragment.parse(target, params)
        target_mapping = self._session.mappings[target]
        return SQLFragment(SQLName(prefix=target_mapping.schema, name=target_mapping.table))

    async def count(self):
        return await self._session._backend.count_structured_query(self._query)

    async def fetch(self, limit: int | None = None, offset: int = 0):
        records = await self._session._backend.fetch_structured_query(self._query, limit, offset)
        primary_key_values = [self._mapping.identify_record(rec) for rec in records]
        entities = await self._session.batch_get(self._mapping.entity_type, primary_key_values)
        return typing.cast(list[TEntity], entities)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None

    @classmethod
    def asc(cls, expr: str, nulls_last=True, **params):
        return StructuredQuery.OrderByOption(SQLFragment.parse(expr, params), True, nulls_last)

    @classmethod
    def desc(cls, expr: str, nulls_last=True, **params):
        return StructuredQuery.OrderByOption(SQLFragment.parse(expr, params), False, nulls_last)


class SessionRawQuery:
    def __init__(self, session: "Session", query: str, params: dict[str, PostgresCodable]):
        self._frag = SQLFragment.parse(query, params)
        self._session = session

    async def fetch(self):
        return await self._session._backend.fetch_raw_query(self._frag)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None


# adapter


class AsyncPGSessionBackend(SessionBackend):
    def __init__(self, conn: asyncpg.Connection | asyncpg.pool.Pool):
        self._conn = conn

    async def select_by_keys(self, mapping: EntityMapping, key: Key, key_values: Collection[KeyValue]):
        if not key_values:
            return []

        select = {c: True for c in (*mapping.columns, *key.columns)}
        key_records = [key.to_partial(val) for val in key_values]

        select_list = self._sql_l(self._sql_qn("t", name) for name in select)
        join_cols = self._sql_l(self._sql_q(col) for col in key.columns)
        from_table = self._sql_qn(mapping.schema, mapping.table)
        vars = self._sql_l(
            f"(array[(null::{from_table}).{self._sql_q(col)}])[:0]||${i + 1}" for i, col in enumerate(key.columns)
        )

        query = f"SELECT {select_list} FROM {from_table} t JOIN UNNEST({vars}) v({join_cols}) USING ({join_cols});"
        params = [[rec[n] for rec in key_records] for n in key.columns]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(select, record)) for record in records]

    async def insert_records(self, mapping: EntityMapping, key: Key, entities: Collection[tuple[KeyValue, Entity]]):
        if not entities:
            return []

        cols = {c: True for c in (*mapping.columns, *key.columns)}
        values = [{**key.to_partial(kv), **mapping.to_record(ev)} for kv, ev in entities]
        insert_into = self._sql_qn(mapping.schema, mapping.table)
        col_names = self._sql_l(self._sql_q(n) for n in cols)
        returning_names = self._sql_l(self._sql_q(name) for name in cols)
        params = []
        value_exprs = []
        for record in values:
            row_args = []
            for col in cols:
                if record[col] is not None:
                    params.append(record[col])
                    row_args.append(f"${len(params)}")
                else:
                    row_args.append("DEFAULT")
            value_exprs.append(f"({self._sql_l(row_args)})")

        query = (
            f"INSERT INTO {insert_into} ({col_names}) "
            f"VALUES {self._sql_l(value_exprs)} RETURNING {returning_names};"
        )
        records = await self._conn.fetch(query, *params)

        return [dict(zip(cols, record)) for record in records]

    async def update_records(self, mapping: EntityMapping, key: Key, values: Collection[tuple[KeyValue, Entity]]):
        if not values:
            return []

        key_columns = {c: True for c in (*key.columns, *mapping.primary_key.columns)}
        set_columns = {k: True for k in mapping.columns if k not in key_columns}
        all_columns = {**set_columns, **key_columns}

        update = self._sql_qn(mapping.schema, mapping.table)
        set_left = self._sql_l(self._sql_q(name) for name in set_columns)
        set_right = self._sql_l(self._sql_qn("v", name) for name in set_columns)
        if len(set_columns) > 1:
            set_left = f"({set_left})"
            set_right = f"({set_right})"

        vars = self._sql_l(
            f"(array[(null::{update}).{self._sql_q(col)}])[:0]||${i + 1}" for i, col in enumerate(all_columns)
        )
        var_colnames = self._sql_l(self._sql_q(name) for name in all_columns)

        where_left = self._sql_l(self._sql_qn("t", name) for name in key_columns)
        where_right = self._sql_l(self._sql_qn("v", name) for name in key_columns)
        returning = self._sql_l(self._sql_qn("t", name) for name in all_columns)

        records = [{**mapping.to_record(ev), **key.to_partial(kv)} for kv, ev in values]

        query = (
            f"UPDATE {update} t "
            f"SET {set_left} = {set_right} "
            f"FROM UNNEST({vars}) v({var_colnames}) "
            f"WHERE ({where_left}) = ({where_right}) "
            f"RETURNING {returning};"
        )
        params = [[cvm[i] for cvm in records] for i in all_columns]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(all_columns, record)) for record in records]

    async def delete_by_keys(self, mapping: EntityMapping, key: Key, key_values: Collection[KeyValue]):
        if not key_values:
            return

        where_cols = key.columns

        delete_from = self._sql_qn(mapping.schema, mapping.table)
        var_colnames = self._sql_l(self._sql_q(name) for name in where_cols)
        vars = self._sql_l(
            f"(array[(null::{delete_from}).{self._sql_q(col)}])[:0]||${i + 1}" for i, col in enumerate(where_cols)
        )

        where_left = self._sql_l(self._sql_qn("t", name) for name in where_cols)
        where_right = self._sql_l(self._sql_qn("v", name) for name in where_cols)

        records = [key.to_partial(val) for val in key_values]

        query = (
            f"DELETE FROM {delete_from} t USING UNNEST({vars}) "
            f"v({var_colnames}) WHERE ({where_left}) = ({where_right});"
        )
        params = [[cvm[i] for cvm in records] for i in where_cols]
        await self._conn.fetch(query, *params)

    async def fetch_structured_query(self, query: StructuredQuery, limit: int | None, offset: int) -> list[Rec]:
        params = []

        primary_col_names = [n for n in query.mapping.primary_key.columns]
        select = self._sql_l(self._sql_qn(query.alias, n) for n in primary_col_names)
        from_table = self._sql_qn(query.mapping.schema, query.mapping.table)
        from_alias = self._sql_q(query.alias)
        joins = self._sql_l(self._sql_join(join, params) for join in query.joins.values())
        filters = self._sql_filter_conds(query.filter_conds, params)
        order_by = self._sql_l(self._sql_order_by_option(option, params) for option in query.order_by_options)

        params.append(limit)
        limit_cluase = f"LIMIT ${len(params)}"
        params.append(offset)
        offset_clause = f"OFFSET ${len(params)}"

        sql = (
            f"SELECT {select} FROM {from_table} AS {from_alias} {joins} "
            f"GROUP BY {select} HAVING {filters} ORDER BY {order_by} "
            f"{limit_cluase} {offset_clause};"
        )
        records = await self._conn.fetch(sql, *params)
        return [dict(zip(primary_col_names, record)) for record in records]

    async def count_structured_query(self, query: StructuredQuery) -> int:
        params = []

        primary_key = query.mapping.primary_key
        select = self._sql_l(self._sql_qn(query.alias, n) for n in primary_key.columns)
        from_table = self._sql_qn(query.mapping.schema, query.mapping.table)
        from_alias = self._sql_q(query.alias)
        joins = self._sql_l(self._sql_join(join, params) for join in query.joins.values())
        filters = self._sql_filter_conds(query.filter_conds, params)

        sql = (
            f"SELECT COUNT(*) FROM (SELECT {select} FROM {from_table} AS {from_alias} {joins} "
            f"GROUP BY {select} HAVING {filters}) _;"
        )
        records = await self._conn.fetch(sql, *params)
        return records[0][0]

    async def fetch_raw_query(self, query: SQLFragment) -> list[Rec]:
        params = []
        sql = "".join(self._sql_element(el, params) for el in query._elements)
        return await self._conn.fetch(sql, *params)

    async def fetch(self, query: str, *params) -> list[Rec]:
        return await self._conn.fetch(query, *params)

    def _sql_l(self, items: typing.Iterable[str]):
        return ", ".join(items)

    def _sql_qn(self, prefix: str | None, name: str):
        if prefix:
            return self._sql_q(prefix) + "." + self._sql_q(name)
        return self._sql_q(name)

    def _sql_q(self, name: str):
        return '"' + name.replace('"', '""') + '"'

    def _sql_filter_conds(self, conds: list[SQLFragment], inout_params: list):
        return " AND ".join(f"({self._sql_element(cond, inout_params)})" for cond in conds)

    def _sql_join(self, join: StructuredQuery.Join, inout_params: list):
        target_expr = self._sql_element(join.target, inout_params)
        condition_expr = self._sql_element(join.on, inout_params)
        return f"{join.type} {target_expr} ON {condition_expr}"

    def _sql_order_by_option(self, option: StructuredQuery.OrderByOption, input_params: list):
        expr = self._sql_element(option.expr, input_params)
        direction = "ASC" if option.ascending else "DESC"
        nulls = "NULLS LAST" if option.nulls_last else "NULLS FIRST"
        return f"{expr} {direction} {nulls}"

    def _sql_element(self, element: SQLFragmentElement, inout_params: list):
        if isinstance(element, SQLText):
            return element.text
        elif isinstance(element, SQLVar):
            inout_params.append(element.value)
            return f"${len(inout_params)}::{element.type}" if element.type else f"${len(inout_params)}"
        elif isinstance(element, SQLName):
            return self._sql_qn(element.prefix, element.name)
        elif isinstance(element, SQLFragment):
            return "".join(self._sql_element(el, inout_params) for el in element._elements)


# mapped


class AutoMappingBuilder:
    class EntityMappingConfig(typing.TypedDict, total=False):
        schema: str
        table: str
        primary: str | list[str]
        fields: typing.Callable[[], dict[str, str]]
        children: typing.Callable[[], dict[str, "AutoMappingBuilder.ChildConfig"]]
        factory: EntityFactory

    class ChildConfig(typing.TypedDict):
        kind: typing.Literal["singular", "plural"]
        columns: list[str]
        target: type

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
        field_configs = opts.get("fields", lambda: {})()
        child_configs = opts.get("children", lambda: {})()
        maybe_str_or_list = opts.get("primary", "id")

        if isinstance(maybe_str_or_list, str):
            primary = [maybe_str_or_list]
        else:
            primary = maybe_str_or_list

        fields = {name: Field(name, column, name in primary) for name, column in field_configs.items()}
        children = {
            name: (
                Plural(name, config["target"], *config["columns"])
                if config["kind"] == "plural"
                else Singular(name, config["target"], *config["columns"])
            )
            for name, config in child_configs.items()
        }
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
                children[name] = Plural(name, args[0], self._parental_key_name(entity_type.__name__, primary))
            # registered entity
            elif type_hint in self._configs:
                children[name] = Singular(name, type_hint, self._parental_key_name(entity_type.__name__, primary))
            # optional of registered entity
            elif origin == types.UnionType and len(args) == 2 and args[1] is type(None) and args[0] in self._configs:
                children[name] = Singular(name, args[0], self._parental_key_name(entity_type.__name__, primary))
            else:
                fields[name] = Field(name, self._column_name(name), name in primary)

        return EntityMapping.define(
            entity_type,
            opts.get("table", self._table_name(entity_type.__name__)),
            opts.get("schema"),
            list(fields.values()),
            list(children.values()),
            opts.get("factory"),
        )

    def _parental_key_name(self, s: str, field_names: typing.Sequence[str]) -> list[str]:
        return [self._table_name(s) + "_" + col for col in field_names]

    def _column_name(self, s: str) -> str:
        return self._table_name(s)

    def _table_name(self, s: str, patt=re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")):
        return patt.sub("_", s).lower()


auto = AutoMappingBuilder()
