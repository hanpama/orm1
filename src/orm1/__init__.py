import dataclasses
import typing
import re
from typing import Collection, Sequence, Type

import asyncpg


class Table(typing.NamedTuple):
    schema: str | None
    name: str


class Column(typing.NamedTuple):
    name: str
    type: str
    default: str
    updatable: bool
    primary: bool


Entity = object
KeyValue = typing.Hashable
PostgresCodable = typing.Any
Rec = dict[str, PostgresCodable]


class Field:
    def __init__(self, name: str, column: Column, is_primary=False):
        self.name = name
        self.column = column
        self.primary = is_primary

    def write_to_entity(self, entity: Entity, partial: Rec):
        self.set_to_entity(entity, partial[self.column.name])

    def write_to_record(self, entity: Entity, partial: Rec):
        partial[self.column.name] = self.get_from_entity(entity)

    def get_from_entity(self, entity: Entity):
        return getattr(entity, self.name)

    def set_to_entity(self, entity: Entity, value: PostgresCodable):
        setattr(entity, self.name, value)

    def to_partial(self, value: PostgresCodable) -> Rec:
        return {self.column.name: value}

    def from_partial(self, partial: Rec) -> PostgresCodable:
        return partial[self.column.name]

    def __repr__(self) -> str:
        return f"Field({self.name!r}: {self.column.name!r}, primary={self.primary})"


class Key(typing.Protocol):
    columns: Sequence[Column]

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
        self.columns = [field.column for field in fields]

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
    def __init__(self, columns: Sequence[Column], parent_key: Key):
        self.columns = columns
        self._parent_key = parent_key

    def from_partial(self, partial: Rec) -> KeyValue:
        rec = {pc.name: partial[c.name] for c, pc in zip(self.columns, self._parent_key.columns)}
        return self._parent_key.from_partial(rec)

    def to_partial(self, value: KeyValue) -> Rec:
        rec = self._parent_key.to_partial(value)
        return {c.name: rec[pc.name] for c, pc in zip(self.columns, self._parent_key.columns)}


class Child:
    def __init__(self, name: str, mapping: typing.Callable[..., "EntityMapping"]):
        self.name = name
        self._mapping = mapping

    def get_entity_attribute(self, entity: Entity) -> Sequence[Entity]: ...
    def set_entity_attribute(self, entity: Entity, values: list[Entity]): ...

    def get_mapping(self) -> "EntityMapping":
        return self._mapping()

    def parental_key(self, parent_key: Key) -> ParentalKey:
        return ParentalKey(self._mapping().parent_reference, parent_key)


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


class EntityMapping(typing.NamedTuple):
    object_type: Type[Entity]
    table: Table
    fields: list[Field]
    children: list[Child]
    primary_key: PrimaryKey
    parent_reference: list[Column]

    @classmethod
    def define(
        cls,
        entity: Type[Entity],
        table: Table,
        fields: list[Field] = [],
        children: list[Child] = [],
        parental_reference: list[Column] = [],
    ):
        primary_fields = [f for f in fields if f.primary]
        if len(primary_fields) > 1:
            primary_key = CompositeKey(primary_fields)
        elif len(primary_fields) == 1:
            primary_key = SimpleKey(primary_fields[0])
        else:
            raise Exception(f"No primary key defined: {entity.__name__}")
        return cls(entity, table, fields, children, primary_key, parental_reference)

    def identify_entity(self, entity: Entity) -> KeyValue:
        return self.primary_key.get_from_entity(entity)

    def identify_record(self, full: Rec) -> KeyValue:
        return self.primary_key.from_partial(full)

    def create_object(self) -> object:
        return object.__new__(self.object_type)

    def write_record(self, entity: Entity, full: Rec):
        for f in self.fields:
            f.write_to_entity(entity, full)

    def to_record(self, entity: Entity) -> Rec:
        full: Rec = {}
        for f in self.fields:
            f.write_to_record(entity, full)
        return full

    def get_columns(self):
        cols = dict[str, Column]()
        for f in self.fields:
            cols[f.column.name] = f.column
        for c in self.parent_reference:
            cols[c.name] = c
        return list(cols.values())


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
    async def fetch_structured_query(self, query: "SQLStructuredQuery") -> list[Rec]: ...


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
        self.mappings = {mapping.object_type: mapping for mapping in mappings}

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

    # def query(self, entity_type: Type[TEntity], alias: str):
    #     return EntityQuery[TEntity](self, entity_type, alias)

    def raw(self, query: str, **params):
        return RawQuery(self, query, params)

    async def _get_by_key(self, mapping: EntityMapping, key: Key, values: Collection[KeyValue]):
        records = await self._backend.select_by_keys(mapping, key, values)
        record_map = {mapping.identify_record(rec): rec for rec in records}

        found = dict[KeyValue, Entity]()
        for id, full in record_map.items():
            entity = self._get_entity_in_track(mapping, id) or mapping.create_object()
            mapping.write_record(entity, full)
            found[id] = entity

        for child in mapping.children:
            child_entities = {parent_key: list[Entity]() for parent_key in found}
            children_found = await self._get_by_key(
                child.get_mapping(), child.parental_key(mapping.primary_key), child_entities
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
            previous_ids = self._get_children_ids_in_track(child, saved)
            new_ids = {
                child.get_mapping().identify_entity(c)
                for parent in saved.values()
                for c in child.get_entity_attribute(parent)
            }
            await self._delete_by_key(child.get_mapping(), child.get_mapping().primary_key, previous_ids - new_ids)

            to_save = [(pid, c) for pid, parent in saved.items() for c in child.get_entity_attribute(parent)]
            await self._save_by_key(child.get_mapping(), child.parental_key(mapping.primary_key), to_save)

        return values

    async def _delete_by_key(self, mapping: EntityMapping, key: Key, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_ids = self._get_children_ids_in_track(child, ids)
            await self._delete_by_key(child.get_mapping(), child.get_mapping().primary_key, child_ids)

        await self._backend.delete_by_keys(mapping, key, ids)

    def _track(self, mapping: EntityMapping, entities: Collection[Entity]):
        entity_map = {mapping.identify_entity(entity): entity for entity in entities}

        for id, entity in entity_map.items():
            self._identity_map[(mapping, id)] = entity

        for child in mapping.children:
            new_ids = set[KeyValue]()

            for id, entity in entity_map.items():
                child_entities = child.get_entity_attribute(entity)
                child_ids = {child.get_mapping().identify_entity(child_obj) for child_obj in child_entities}

                self._children_map[(child, id)] = child_ids
                self._track(child.get_mapping(), child_entities)
                new_ids.update(child_ids)

            previous_ids = self._get_children_ids_in_track(child, entity_map)
            self._untrack(child.get_mapping(), previous_ids - new_ids)

    def _untrack(self, mapping: EntityMapping, ids: Collection[KeyValue]):
        for child in mapping.children:
            child_ids = self._get_children_ids_in_track(child, ids)
            self._untrack(child.get_mapping(), child_ids)
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


# SQL object models


class SQLText(typing.NamedTuple):
    text: str


class SQLVar(typing.NamedTuple):
    value: PostgresCodable
    type: str = ""


class SQLName(typing.NamedTuple):
    prefix: str | None
    name: str


class SQLFragment:
    def __init__(self, elements: Collection["SQLElement"]):
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
                        tokens.append(SQLVar(val))
                else:
                    raise Exception(f"Parameter '{matched[1]}' not provided")
            else:
                tokens.append(SQLText(word))

        return cls(tokens)


SQLElement = SQLText | SQLVar | SQLName | SQLFragment


@dataclasses.dataclass
class SQLStructuredQuery:
    class OrderByItem(typing.NamedTuple):
        expr: SQLElement
        ascending: bool
        nulls_last: bool

    class Join(typing.NamedTuple):
        type: "SQLStructuredQuery.JoinType"
        target: SQLElement
        alias: str
        on: SQLElement

    JoinType = typing.Literal["JOIN", "LEFT JOIN"]

    select_items: list[SQLElement] = dataclasses.field(default_factory=list)
    from_item: SQLElement | None = None
    from_alias: str = ""
    joins: dict[str, Join] = dataclasses.field(default_factory=dict)
    where_conds: list[SQLElement] = dataclasses.field(default_factory=list)
    group_by_exprs: list[SQLElement] = dataclasses.field(default_factory=list)
    having_conds: list[SQLElement] = dataclasses.field(default_factory=list)
    limit: SQLElement | None = None
    offset: SQLElement | None = None
    order_by_items: list[OrderByItem] = dataclasses.field(default_factory=list)

    @classmethod
    def asc(cls, expr: str, nulls_last=True, **params):
        return cls.OrderByItem(SQLFragment.parse(expr, params), True, nulls_last)

    @classmethod
    def desc(cls, expr: str, nulls_last=True, **params):
        return cls.OrderByItem(SQLFragment.parse(expr, params), False, nulls_last)

    def set_limit(self, limit: int | str, **params):
        self.limit = SQLVar(limit) if isinstance(limit, int) else SQLFragment.parse(limit, params)

    def set_offset(self, offset: int | str, **params):
        self.offset = SQLVar(offset) if isinstance(offset, int) else SQLFragment.parse(offset, params)

    def add_join(self, target: SQLElement, alias: str, on: SQLElement):
        self.joins[alias] = SQLStructuredQuery.Join("JOIN", target, alias, on)

    def add_left_join(self, target: SQLElement, alias: str, on: SQLElement):
        self.joins[alias] = SQLStructuredQuery.Join("LEFT JOIN", target, alias, on)


class SessionEntityQuery(typing.Generic[TEntity]):
    def __init__(self, session: "Session", entity_type: type, alias: str):
        mapping = session.mappings[entity_type]
        primary_columns = mapping.primary_key.columns
        self._session = session
        self._mapping = mapping
        self._query = SQLStructuredQuery(
            select_items=[SQLName(prefix=alias, name=c.name) for c in primary_columns],
            from_item=SQLName(prefix=mapping.table.schema, name=mapping.table.name),
            from_alias=alias,
            group_by_exprs=[SQLName(prefix=alias, name=c.name) for c in primary_columns],
        )

    def asc(self, expr: str, nulls_last=True, **params):
        return SQLStructuredQuery.asc(expr, nulls_last, **params)

    def desc(self, expr: str, nulls_last=True, **params):
        return SQLStructuredQuery.desc(expr, nulls_last, **params)

    def join(self, target: type | str, alias: str, on: SQLFragment, **params):
        self._query.add_join(self._get_target_expr(target, params), alias, on)
        return self

    def left_join(self, target: type | str, alias: str, on: SQLFragment, **params):
        self._query.add_left_join(self._get_target_expr(target, params), alias, on)
        return self

    def filter(self, condition: str, **params):
        self._query.having_conds.append(SQLFragment.parse(condition, params))
        return self

    # order, limit, offset ()
    def order_by(self, *items: SQLStructuredQuery.OrderByItem):
        self._query.order_by_items = list(items)
        return self

    def limit(self, limit: int | str, **params):
        self._query.set_limit(limit, **params)
        return self

    def offset(self, offset: int | str, **params):
        self._query.set_offset(offset, **params)
        return self

    def _get_target_expr(self, target: type | str, params: dict):
        if isinstance(target, str):
            return SQLFragment.parse(target, params)
        target_mapping = self._session.mappings[target]
        return SQLName(prefix=target_mapping.table.schema, name=target_mapping.table.name)

    async def count(self):
        query = SQLStructuredQuery(
            select_items=[SQLText("COUNT(*)")],
            from_item=self._query,
        )
        return await self._session._backend.fetch_structured_query(query)

    async def fetch(self):
        records = await self._session._backend.fetch_structured_query(self._query)
        primary_key_values = [self._mapping.identify_record(rec) for rec in records]
        entities = await self._session.batch_get(self._mapping.object_type, primary_key_values)
        return typing.cast(list[TEntity], entities)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None


class SessionRawQuery:
    def __init__(self, session: "Session", query: str, params: dict[str, PostgresCodable]):
        self._frag = SQLFragment.parse(query, params)
        self._session = session

    async def fetch(self):
        return await self._session._backend.fetch(*self._frag.build_sql())

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


# adapter


class AsyncPGSessionBackend(SessionBackend):
    def __init__(self, conn: asyncpg.Connection | asyncpg.pool.Pool):
        self._conn = conn

    async def fetch(self, query: str, params: Sequence[PostgresCodable]) -> list[typing.Mapping[str, PostgresCodable]]:
        return await self._conn.fetch(query, *params)

    async def select_by_keys(self, mapping: EntityMapping, key: Key, key_values: Collection[KeyValue]):
        if not key_values:
            return []

        select = {col.name: col for col in (*mapping.get_columns(), *key.columns)}
        key_records = [key.to_partial(val) for val in key_values]

        select_list = self.sql_l(self._sql_qn("t", name) for name in select)
        vars = self.sql_l(f"${i + 1}::{col.type}[]" for i, col in enumerate(key.columns))
        join_cols = self.sql_l(self._sql_q(col.name) for col in key.columns)
        from_table = self._sql_qn(mapping.table.schema, mapping.table.name)

        query = f"SELECT {select_list} FROM {from_table} t JOIN UNNEST({vars}) v({join_cols}) USING ({join_cols});"
        params = [[rec[i.name] for rec in key_records] for i in key.columns]
        records = await self._conn.fetch(query, *params)
        return [dict(zip(select, record)) for record in records]

    async def insert_records(self, mapping: EntityMapping, key: Key, entities: Collection[tuple[KeyValue, Entity]]):
        if not entities:
            return []

        cols = {col.name: col for col in (*mapping.get_columns(), *key.columns)}
        values = [key.to_partial(val) for val in entities]

        insert_into = self._sql_qn(mapping.table.schema, mapping.table.name)
        col_names = self.sql_l(self._sql_q(col.name) for col in mapping.get_columns())
        returning_names = self.sql_l(self._sql_q(name) for name in cols)
        vars = self.sql_l(f"${i + 1}::{col.type}[]" for i, col in enumerate(mapping.get_columns()))
        selects = self.sql_l(f"COALESCE(v.{col.name}, {col.default or 'NULL'})" for col in mapping.get_columns())

        query = f"INSERT INTO {insert_into} ({col_names}) SELECT {selects} FROM UNNEST({vars}) v({col_names}) RETURNING {returning_names};"
        params = [[cvm[name] for cvm in values] for name in cols]
        records = await self._conn.fetch(query, *params)

        return [dict(zip(cols, record)) for record in records]

    async def update_records(self, mapping: EntityMapping, key: Key, values: Collection[tuple[KeyValue, Entity]]):
        if not values:
            return []

        all_columns = {c.name: c for c in mapping.get_columns()}
        key_columns = {c.name: c for c in key.columns}
        set_columns = {c.name: c for c in mapping.get_columns() if c.name not in key_columns}

        update = self._sql_qn(mapping.table.schema, mapping.table.name)
        set_left = self.sql_l(self._sql_q(name) for name in set_columns)
        set_right = self.sql_l(self._sql_qn("v", name) for name in set_columns)
        if len(set_columns) > 1:
            set_left = f"({set_left})"
            set_right = f"({set_right})"

        vars = self.sql_l(f"${i + 1}::{col.type}[]" for i, col in enumerate(all_columns.values()))
        var_colnames = self.sql_l(self._sql_q(name) for name in all_columns)

        returning = self.sql_l(self._sql_q(name) for name in all_columns)
        where_left = self.sql_l(self._sql_qn("t", name) for name in key_columns)
        where_right = self.sql_l(self._sql_qn("v", name) for name in key_columns)

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

        where_cols = {c.name: c for c in key.columns}

        delete_from = self._sql_qn(mapping.table.schema, mapping.table.name)
        vars = self.sql_l(f"${i + 1}::{col.type}[]" for i, col in enumerate(where_cols.values()))
        var_colnames = self.sql_l(self._sql_q(name) for name in where_cols)

        where_left = self.sql_l(self._sql_qn("t", name) for name in where_cols)
        where_right = self.sql_l(self._sql_qn("v", name) for name in where_cols)

        records = [key.to_partial(val) for val in key_values]

        query = (
            f"DELETE FROM {delete_from} t USING UNNEST({vars}) "
            f"v({var_colnames}) WHERE ({where_left}) = ({where_right});"
        )
        params = [[cvm[i] for cvm in records] for i in where_cols]
        await self._conn.fetch(query, *params)

    async def fetch_structured_query(self, query: SQLStructuredQuery) -> list[Rec]:
        pass
        # sql, params = query.build_sql()
        # return await self.fetch(sql, *params)

    def sql_l(self, items: typing.Iterable[str]):
        return ", ".join(items)

    def _sql_qn(self, prefix: str | None, name: str):
        if prefix:
            return self._sql_q(prefix) + "." + self._sql_q(name)
        return self._sql_q(name)

    def _sql_q(self, name: str):
        return '"' + name.replace('"', '""') + '"'
