import typing
import re
from typing import Collection, Iterable, Sequence, Type
import functools

import asyncpg


class Table:
    def __init__(self, schema: str, name: str):
        self._schema = schema
        self._name = name

    def __repr__(self) -> str:
        return f"Table({self._schema!r}, {self._name!r})"


class Column:
    def __init__(self, name: str, type: str, default="", updatable=True, primary=False):
        self.name = name
        self.type = type
        self.default = default
        self.updatable = updatable
        self.primary = primary

    def __repr__(self) -> str:
        return f"Column({self.name!r}, {self.type!r}, default={self.default!r}, updatable={self.updatable}, primary={self.primary})"


Entity = object
KeyValue = typing.Hashable
PostgresCodable = typing.Any
Rec = dict[Column, PostgresCodable]


class SelectOperation(typing.NamedTuple):
    table: Table
    select: Sequence[Column]
    where: Sequence[Column]
    values: Sequence[Rec]


class InsertOperation(typing.NamedTuple):
    table: Table
    returning_cols: Sequence[Column]
    insert_cols: Sequence[Column]
    values: Sequence[Rec]


class UpdateOperation(typing.NamedTuple):
    table: Table
    where_cols: list[Column]
    update_cols: list[Column]
    returning_cols: list[Column]
    values: list[Rec]


class DeleteOperation(typing.NamedTuple):
    table: Table
    key_cols: Sequence[Column]
    values: Sequence[Rec]


class Field:
    def __init__(self, name: str, column: Column, is_primary=False):
        self.name = name
        self.column = column
        self.primary = is_primary

    def read_partial(self, entity: Entity):
        return self.to_partial(self.get_from_entity(entity))

    def write_partial(self, entity: Entity, partial: Rec):
        self.set_to_entity(entity, self.from_partial(partial))

    def get_from_entity(self, entity: Entity):
        return getattr(entity, self.name)

    def set_to_entity(self, entity: Entity, value: PostgresCodable):
        setattr(entity, self.name, value)

    def to_partial(self, value: PostgresCodable) -> Rec:  # TODO: write to partial
        return {self.column: value}

    def from_partial(self, partial: Rec) -> PostgresCodable:  # TODO: read from partial
        return partial[self.column]

    def __repr__(self) -> str:
        return f"Field({self.name!r}: {self.column.name!r}, primary={self.primary})"


class Key(typing.Protocol):
    columns: Sequence[Column]

    def from_partial(self, partial: Rec) -> KeyValue: ...  # TODO: read from record
    def to_partial(self, value: KeyValue) -> Rec: ...  # TODO: write to record


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
        self.columns = [field.column for field in fields]

    def from_partial(self, partial: Rec) -> KeyValue:
        return tuple(field.from_partial(partial) for field in self.fields)

    def to_partial(self, value: KeyValue) -> Rec:
        assert isinstance(value, tuple)
        partial = {}
        for field, val in zip(self.fields, value):
            partial.update(field.to_partial(val))
        return partial

    def get_from_entity(self, entity: Entity) -> KeyValue:
        return tuple(field.get_from_entity(entity) for field in self.fields)


class ParentalKey(Key):
    def __init__(self, columns: Sequence[Column], parent_key: Key):
        self.columns = columns
        self._parent_key = parent_key

    def from_partial(self, partial: Rec) -> KeyValue:
        rec = {pc: partial[c] for c, pc in zip(self.columns, self._parent_key.columns)}
        return self._parent_key.from_partial(rec)

    def to_partial(self, value: KeyValue) -> Rec:
        rec = self._parent_key.to_partial(value)
        return {c: rec[pc] for c, pc in zip(self.columns, self._parent_key.columns)}


class Child:
    def __init__(
        self,
        name: str,
        mapping: typing.Callable[..., "EntityMapping"],
        *columns: Column,
    ):
        self.name = name
        self.columns = columns
        self._mapping = mapping

    def get(self, entity: Entity) -> Sequence[Entity]: ...
    def set(self, entity: Entity, values: list[Entity]): ...

    @functools.cached_property
    def mapping(self) -> "EntityMapping":
        return self._mapping()

    def parental_key(self, parent_key: Key) -> ParentalKey:
        return ParentalKey(self.columns, parent_key)


class Plural(Child):
    def get(self, entity: Entity) -> Sequence[Entity]:
        val = getattr(entity, self.name)
        return tuple(val) if val else ()

    def set(self, entity: Entity, values: Sequence[Entity]):
        setattr(entity, self.name, list(values))


class Singular(Child):
    def get(self, entity: Entity) -> Sequence[Entity]:
        val = getattr(entity, self.name)
        return (val,) if val else ()

    def set(self, entity: Entity, values: Sequence[Entity]):
        setattr(entity, self.name, values[0] if len(values) else None)


class EntityMapping:
    def __init__(
        self,
        entity: Type[Entity],
        table: Table,
        fields: list[Field] = [],
        children: list[Child] = [],
    ):
        self.object_type = entity
        self.table = table
        self.fields = fields
        self.children = children

        self._primary_fields = [f for f in self.fields if f.primary]
        if len(self._primary_fields) > 1:
            self.primary_key = CompositeKey(self._primary_fields)
        elif len(self._primary_fields) == 1:
            self.primary_key = SimpleKey(self._primary_fields[0])
        else:
            raise Exception(f"No primary key defined: {entity.__name__}")

        self._mapped_properties = {
            **{f.name: f for f in self.fields},
            **{c.name: c for c in self.children},
        }
        self._field_columns = {f.column for f in self.fields}

    def identify_entity(self, entity: Entity) -> KeyValue:
        return self.primary_key.get_from_entity(entity)

    def identify_record(self, full: Rec) -> KeyValue:
        return self.primary_key.from_partial(full)

    def create_object(self) -> object:
        return object.__new__(self.object_type)

    def create_select_operation(self, key: Key, key_values: Collection[KeyValue]):
        select_cols = self._collect_columns(*self._field_columns, *key.columns)
        where_cols = key.columns
        values = [key.to_partial(val) for val in key_values]

        return SelectOperation(self.table, select_cols, where_cols, values)

    def create_insert_operation(self, key: Key, entities: Collection[tuple[KeyValue, Entity]]):
        select_cols = self._collect_columns(*self._field_columns, *key.columns)
        insert_cols = [col for col in select_cols if col.updatable]
        values = [{**self._read_to_record(ent), **key.to_partial(val)} for val, ent in entities]

        return InsertOperation(self.table, select_cols, insert_cols, values)

    def create_update_operation(self, key: Key, entities: Collection[tuple[KeyValue, Entity]]):
        where_cols = self._collect_columns(*key.columns, *self.primary_key.columns)
        returning_cols = self._collect_columns(*where_cols, *self._field_columns)
        update_cols = [col for col in returning_cols if col.updatable and col not in where_cols]
        values = [{**self._read_to_record(ent), **key.to_partial(val)} for val, ent in entities]

        return UpdateOperation(self.table, where_cols, update_cols, returning_cols, values)

    def create_delete_operation(self, key: Key, key_values: Collection[KeyValue]):
        where_cols = key.columns
        values = [key.to_partial(val) for val in key_values]

        return DeleteOperation(self.table, where_cols, values)

    def write_record(self, entity: Entity, full: Rec):
        for f in self.fields:
            f.write_partial(entity, full)

    def _read_to_record(self, entity: Entity) -> Rec:
        full: Rec = {}
        for f in self.fields:
            full.update(f.read_partial(entity))
        return full

    def _collect_columns(self, *columns: Column):
        return list(dict.fromkeys(c for c in columns))


class SessionBackend:
    def __init__(self, conn: asyncpg.Connection | asyncpg.pool.Pool):
        self._conn = conn

    async def select_by_keys(self, op: SelectOperation) -> list[Rec]:
        if not op.values:
            return []

        select_list = self._sql_tablecollist("t", op.select)
        vars = self._sql_varlist(op.where)
        join_cols = self._sql_colnamelist(op.where)
        join_table = self._sql_qn(op.table._schema, op.table._name)

        query = f"SELECT {select_list} FROM {join_table} t JOIN UNNEST({vars}) v({join_cols}) USING ({join_cols});"
        params = [[rec[i] for rec in op.values] for i in op.where]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(op.select, record)) for record in records]

    async def insert_records(self, op: InsertOperation) -> list[Rec]:
        if not op.values:
            return []
        insert_into = self._sql_qn(op.table._schema, op.table._name)
        col_names = self._sql_colnamelist(op.insert_cols)
        returning_names = self._sql_colnamelist(op.returning_cols)
        vars = self._sql_varlist(op.insert_cols)
        selects = self._sql_list(f"COALESCE(v.{col.name}, {col.default or 'NULL'})" for col in op.insert_cols)

        query = f"INSERT INTO {insert_into} ({col_names}) SELECT {selects} FROM UNNEST({vars}) v({col_names}) RETURNING {returning_names};"
        params = [[cvm[i] for cvm in op.values] for i in op.insert_cols]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(op.returning_cols, record)) for record in records]

    async def update_records(self, op: UpdateOperation) -> list[Rec]:
        if not op.values:
            return []
        all_columns = [*op.where_cols, *op.update_cols]

        update = self._sql_qn(op.table._schema, op.table._name)
        set_left = self._sql_colnamelist(op.update_cols)
        set_right = self._sql_tablecollist("v", op.update_cols)
        if len(op.update_cols) > 1:
            set_left = f"({set_left})"
            set_right = f"({set_right})"

        vars = self._sql_varlist(all_columns)
        var_colnames = self._sql_colnamelist(all_columns)
        returning_colnames = self._sql_tablecollist("t", op.returning_cols)

        t_key_cols = self._sql_tablecollist("t", op.where_cols)
        v_key_cols = self._sql_tablecollist("v", op.where_cols)

        query = (
            f"UPDATE {update} t "
            f"SET {set_left} = {set_right} "
            f"FROM UNNEST({vars}) v({var_colnames}) "
            f"WHERE ({t_key_cols}) = ({v_key_cols}) "
            f"RETURNING {returning_colnames};"
        )
        params = [[cvm[i] for cvm in op.values] for i in all_columns]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(op.returning_cols, record)) for record in records]

    async def delete_by_keys(self, op: DeleteOperation):
        if not op.values:
            return []
        delete_from = self._sql_qn(op.table._schema, op.table._name)
        vars = self._sql_varlist(op.key_cols)
        var_colnames = self._sql_colnamelist(op.key_cols)
        t_key_cols = self._sql_tablecollist("t", op.key_cols)
        v_key_cols = self._sql_tablecollist("v", op.key_cols)

        query = (
            f"DELETE FROM {delete_from} t USING UNNEST({vars}) v({var_colnames}) WHERE ({t_key_cols}) = ({v_key_cols});"
        )
        params = [[cvm[i] for cvm in op.values] for i in op.key_cols]
        await self._conn.fetch(query, *params)

    async def fetch(self, query: str, params: Sequence[PostgresCodable]) -> list[typing.Mapping[str, PostgresCodable]]:
        return await self._conn.fetch(query, *params)

    def _sql_list(self, items: Iterable[str]):
        return ", ".join(items)

    def _sql_varlist(self, cols: Iterable[Column]):
        return self._sql_list(f"${i + 1}::{col.type}[]" for i, col in enumerate(cols))

    def _sql_colnamelist(self, cols: Iterable[Column]):
        return self._sql_list(self._sql_quote(col.name) for col in cols)

    def _sql_tablecollist(self, table: str, cols: Iterable[Column]):
        return self._sql_list(self._sql_qn(table, col.name) for col in cols)

    def _sql_qn(self, *segments: str):
        return ".".join(self._sql_quote(s) for s in segments)

    def _sql_quote(self, name: str):
        return '"' + name.replace('"', '""') + '"'


entity_mapping_registry: dict[type, EntityMapping] = {}


def register_mapping(mapping: EntityMapping):
    entity_mapping_registry[mapping.object_type] = mapping


def lookup_mapping(type: type) -> EntityMapping:
    return entity_mapping_registry[type]


TEntity = typing.TypeVar("TEntity", bound=Entity)


class Session:
    def __init__(
        self,
        backend: SessionBackend,
        mappings: Collection[EntityMapping] = entity_mapping_registry.values(),
    ):
        self._backend = backend
        self._identity_map = dict[tuple[EntityMapping, KeyValue], object]()
        self._children_map = dict[tuple[Child, KeyValue], set[KeyValue]]()
        self._mappings = {mapping.object_type: mapping for mapping in mappings}

    async def get(self, entity_type: Type[TEntity], id: KeyValue) -> TEntity | None:
        return (await self.batch_get(entity_type, (id,)))[0]

    async def batch_get(self, entity_type: Type[TEntity], ids: Collection[KeyValue]) -> list[TEntity | None]:
        mapping = self._mappings[entity_type]
        found = dict(await self._get_by_key(mapping, mapping.primary_key, ids))
        self._track(mapping, found.values())
        return [typing.cast(TEntity, found.get(v)) for v in ids]

    async def save(self, entity: Entity):
        await self.batch_save(type(entity), entity)

    async def batch_save(self, entity_type: Type[TEntity], *entities: TEntity):
        mapping = self._mappings[entity_type]
        await self._save_by_key(
            mapping,
            mapping.primary_key,
            [(mapping.identify_entity(e), e) for e in entities],
        )
        self._track(mapping, entities)

    async def delete(self, entity: Entity):
        await self.batch_delete(type(entity), entity)

    async def batch_delete(self, entity_type: Type[TEntity], *entities: TEntity):
        mapping = self._mappings[entity_type]
        ids = [mapping.identify_entity(ent) for ent in entities]
        await self._delete_by_key(mapping, mapping.primary_key, ids)
        self._untrack(mapping, ids)

    def select(self, *columns: str):
        return SessionBoundSelectQuery(self, columns)

    def find(self, entity_type: Type[TEntity], alias: str):
        mapping = self._mappings[entity_type]
        return SessionBoundFindQuery[TEntity](self, mapping, alias)

    def raw(self, query: str, **params):
        return SessionBoundRawQuery(self, query, params)

    async def _get_by_key(self, mapping: EntityMapping, key: Key, values: Collection[KeyValue]):
        op = mapping.create_select_operation(key, values)
        records = await self._backend.select_by_keys(op)
        record_map = {mapping.identify_record(rec): rec for rec in records}

        found = dict[KeyValue, Entity]()
        for id, full in record_map.items():
            entity = self._get_entity_in_track(mapping, id) or mapping.create_object()
            mapping.write_record(entity, full)
            found[id] = entity

        for child in mapping.children:
            child_entities = {parent_key: list[Entity]() for parent_key in found}
            children_found = await self._get_by_key(
                child.mapping, child.parental_key(mapping.primary_key), child_entities
            )
            for key_value, entity in children_found:
                child_entities[key_value].append(entity)

            for parent_id in found:
                child.set(found[parent_id], child_entities[parent_id])

        return [(key.from_partial(full), found[id]) for id, full in record_map.items()]

    async def _save_by_key(
        self,
        mapping: EntityMapping,
        key: Key,
        key_entities: Collection[tuple[KeyValue, Entity]],
    ):
        to_insert = list[tuple[KeyValue, Entity]]()
        to_update = list[tuple[KeyValue, Entity]]()

        for value, entity in key_entities:
            id = mapping.identify_entity(entity)
            if self._in_track(mapping, id):
                to_update.append((value, entity))
            else:
                to_insert.append((value, entity))

        pairs = list[tuple[Entity, Rec]]()
        if to_update:
            update_op = mapping.create_update_operation(key, to_update)
            updated = await self._backend.update_records(update_op)
            pairs.extend((e, v) for (_, e), v in zip(to_update, updated))
        if to_insert:
            insert_op = mapping.create_insert_operation(key, to_insert)
            inserted = await self._backend.insert_records(insert_op)
            pairs.extend((e, v) for (_, e), v in zip(to_insert, inserted))

        saved = dict[KeyValue, Entity]()
        for entity, full in pairs:
            id = mapping.identify_record(full)
            mapping.write_record(entity, full)
            saved[id] = entity

        for child in mapping.children:
            previous_ids = self._get_children_ids_in_track(child, saved)
            new_ids = {child.mapping.identify_entity(c) for parent in saved.values() for c in child.get(parent)}
            await self._delete_by_key(child.mapping, child.mapping.primary_key, previous_ids - new_ids)

            to_save = [(pid, c) for pid, parent in saved.items() for c in child.get(parent)]
            await self._save_by_key(child.mapping, child.parental_key(mapping.primary_key), to_save)

        return key_entities

    async def _delete_by_key(self, mapping: EntityMapping, key: Key, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_ids = self._get_children_ids_in_track(child, ids)
            await self._delete_by_key(child.mapping, child.mapping.primary_key, child_ids)

        delete_op = mapping.create_delete_operation(key, ids)
        await self._backend.delete_by_keys(delete_op)

    def _track(self, mapping: EntityMapping, entities: Collection[Entity]):
        entity_map = {mapping.identify_entity(entity): entity for entity in entities}

        for id, entity in entity_map.items():
            self._identity_map[(mapping, id)] = entity

        for child in mapping.children:
            new_ids = set[KeyValue]()

            for id, entity in entity_map.items():
                child_entities = child.get(entity)
                child_ids = {child.mapping.identify_entity(child_obj) for child_obj in child_entities}

                self._children_map[(child, id)] = child_ids
                self._track(child.mapping, child_entities)
                new_ids.update(child_ids)

            previous_ids = self._get_children_ids_in_track(child, entity_map)
            self._untrack(child.mapping, previous_ids - new_ids)

    def _untrack(self, mapping: EntityMapping, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_ids = self._get_children_ids_in_track(child, ids)
            self._untrack(child.mapping, child_ids)
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


# Query Object models


class SQLText:
    def __init__(self, text: str):
        self.text = text

    def __iter__(self):
        yield self


class SQLVar:
    def __init__(self, value: PostgresCodable):
        self.value = value

    def __iter__(self):
        yield self


SQLTokenIterable = typing.Iterable[SQLText | SQLVar]


class SQLBuildable(SQLTokenIterable):
    def build_sql(self) -> tuple[str, list]:
        strings = []
        params = []
        vars: dict[SQLVar, int] = {}

        for token in self:
            if isinstance(token, SQLText):
                strings.append(token.text)
            elif isinstance(token, SQLVar):
                if token not in vars:
                    vars[token] = len(vars) + 1
                    params.append(token.value)
                strings.append(f"${vars[token]}")

        return "".join(strings), params


class SQLFragment(SQLBuildable):
    def __init__(self, *elements: SQLTokenIterable | SQLText | SQLVar):
        self._elements = elements

    def __iter__(self):
        for el in self._elements:
            if isinstance(el, SQLText):
                yield el
            elif isinstance(el, SQLVar):
                yield el
            else:
                yield from el

    _patt_word = re.compile(r"(\s+|::|:\w+|\w+|[^\w\s])")
    _patt_param = re.compile(r":(\w+)")

    @classmethod
    def parse(cls, sql: str, params: dict[str, PostgresCodable]):
        tokens = list[SQLText | SQLVar]()
        words: list[str] = cls._patt_word.findall(sql)

        for word in words:
            if matched := cls._patt_param.match(word):
                if matched[1] in params:
                    tokens.append(SQLVar(params[matched[1]]))
                else:
                    raise Exception(f"Parameter '{matched[1]}' not provided")
            else:
                tokens.append(SQLText(word))

        return SQLFragment(*tokens)


class SQLSelect(SQLBuildable):
    def __init__(self, *select_items: SQLTokenIterable):
        self._select = select_items
        self._froms = list[SQLTokenIterable]()
        self._joins = list[SQLTokenIterable]()
        self._where = list[SQLTokenIterable]()
        self._group_by: SQLTokenIterable | None = None
        self._offset: SQLTokenIterable | None = None
        self._limit: SQLTokenIterable | None = None
        self._order_bys = list[SQLTokenIterable]()

    def from_(self, from_item: str, **params):
        self._froms.append(SQLFragment.parse(from_item, params))
        return self

    def join(self, join_item: str, **params):
        self._joins.append(SQLFragment.parse("JOIN " + join_item, params))
        return self

    def left_join(self, join_item: str, **params):
        self._joins.append(SQLFragment.parse("LEFT JOIN " + join_item, params))
        return self

    def right_join(self, join_item: str, **params):
        self._joins.append(SQLFragment.parse("RIGHT JOIN " + join_item, params))
        return self

    def where(self, condition: str, **params):
        self._where.append(SQLFragment.parse(f"({condition})", params))
        return self

    def group_by(self, group_by: str, **params):
        self._group_by = SQLFragment.parse(group_by, params)
        return self

    def order_by(self, order_by: str, **params):
        self._order_bys.append(SQLFragment.parse(order_by, params))
        return self

    def limit(self, limit: str | int, **params):
        if isinstance(limit, int):
            self._limit = SQLFragment(SQLVar(limit))
        else:
            self._limit = SQLFragment.parse(limit, params)
        return self

    def offset(self, offset: str | int, **params):
        if isinstance(offset, int):
            self._offset = SQLFragment(SQLVar(offset))
        else:
            self._offset = SQLFragment.parse(offset, params)
        return self

    def __iter__(self):
        yield SQLText("SELECT ")
        for i, select in enumerate(self._select):
            if i:
                yield SQLText(", ")
            yield from select
        if self._froms:
            yield SQLText(" FROM ")
            for i, from_ in enumerate(self._froms):
                if i:
                    yield SQLText(", ")
                yield from from_
            for join in self._joins:
                yield SQLText(" ")
                yield from join
        if self._where:
            yield SQLText(" WHERE ")
            for i, where in enumerate(self._where):
                if i:
                    yield SQLText(" AND ")
                yield from where
        if self._group_by:
            yield SQLText(" GROUP BY ")
            yield from self._group_by
        if self._order_bys:
            yield SQLText(" ORDER BY ")
            for i, order_by in enumerate(self._order_bys):
                if i:
                    yield SQLText(", ")
                yield from order_by
        if self._limit:
            yield SQLText(" LIMIT ")
            yield from self._limit
        if self._offset:
            yield SQLText(" OFFSET ")
            yield from self._offset


class SessionBoundFindQuery(SQLSelect, typing.Generic[TEntity]):
    def __init__(self, session: "Session", mapping: EntityMapping, alias: str):
        super().__init__(SQLText(f"DISTINCT {self._quote(alias)}.{self._quote(c.name)}") for c in mapping.fields)
        self._froms = [
            SQLText(f"{self._quote(mapping.table._schema)}.{self._quote(mapping.table._name)} AS {self._quote(alias)}")
        ]
        self._session = session
        self._alias = alias
        self._mapping = mapping

    async def fetch(self):
        raw = await self._session._backend.fetch(*self.build_sql())
        records = [{col: r[col.name] for col in self._mapping.primary_key.columns} for r in raw]
        ids = [self._mapping.identify_record(r) for r in records]
        entities = await self._session._get_by_key(self._mapping, self._mapping.primary_key, ids)
        return [typing.cast(TEntity, e) for _, e in entities]

    async def fetch_one(self):
        self.limit(1)
        results = await self.fetch()
        return results[0] if results else None

    def _quote(self, name: str):
        return '"' + name.replace('"', '""') + '"'


class SessionBoundSelectQuery(SQLSelect):
    def __init__(self, session: "Session", columns: Sequence[str]):
        super().__init__(SQLText(col) for col in columns)
        self._session = session

    async def fetch(self):
        return await self._session._backend.fetch(*self.build_sql())

    async def fetch_one(self):
        self.limit(1)
        results = await self.fetch()
        return results[0] if results else None


class SessionBoundRawQuery(SQLFragment):
    def __init__(self, session: "Session", query: str, params: dict[str, PostgresCodable]):
        super().__init__(*SQLFragment.parse(query, params))
        self._session = session

    async def fetch(self):
        return await self._session._backend.fetch(*self.build_sql())

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None


# mapped

definition_registry = dict[type, "EntityMappingConfig"]()


def mapped(**kwargs: typing.Unpack["EntityMappingConfig"]):
    def wrapper(entity_type):
        definition_registry[entity_type] = kwargs
        return entity_type

    return wrapper


class EntityMappingConfig(typing.TypedDict, total=False):
    schema: str
    table: str
    primary: str | list[str]
    fields: typing.Callable[[], dict[str, str]]
    children: typing.Callable[[], dict[str, "ChildConfig"]]


class ChildConfig(typing.TypedDict):
    kind: typing.Literal["singular", "plural"]
    columns: list[str]
    target: type
