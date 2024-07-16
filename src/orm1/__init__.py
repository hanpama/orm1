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


class SQLText:
    def __init__(self, text: str):
        self.text = text

    def __iter__(self):
        yield self

    @classmethod
    def quote(cls, name: str):
        name = name.replace('"', '""')
        return cls(f'"{name}"')

    dot: "SQLText"
    space: "SQLText"
    comma: "SQLText"
    lparen: "SQLText"
    rparen: "SQLText"


SQLText.dot = SQLText(".")
SQLText.space = SQLText(" ")
SQLText.comma = SQLText(", ")
SQLText.lparen = SQLText("(")
SQLText.rparen = SQLText(")")


class SQLVar:
    def __init__(self, value: PostgresCodable, type=""):
        self.value = value
        self.type = type

    def __iter__(self):
        yield self

    def typed(self, type: str):
        return SQLFragment((self, SQLText("::"), SQLText(type)))


class SQLFragment:
    def __init__(self, elements: Collection["SQLFragment | SQLText | SQLVar"]):
        self._elements = elements

    def __iter__(self):
        for el in self._elements:
            if isinstance(el, SQLText):
                yield el
            elif isinstance(el, SQLVar):
                yield el
            else:
                yield from el

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

        return SQLFragment(tokens)

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
                if token.type:
                    strings.append(f"${vars[token]}::{token.type}")
                else:
                    strings.append(f"${vars[token]}")

        return "".join(strings), params

    def alias(self, alias: str):
        return SQLFragment.concat(self, SQLText("AS"), SQLText.quote(alias))

    @classmethod
    def qn(cls, prefix: str, name: str):
        return SQLFragment((SQLText.quote(prefix), SQLText.dot, SQLText.quote(name)))

    @classmethod
    def join(cls, sep: SQLText, fragments: "Iterable[SQLFragment | SQLText | SQLVar]"):
        elements = list[SQLFragment | SQLText | SQLVar]()
        for i, f in enumerate(fragments):
            if i:
                elements.append(sep)
            elements.append(f)
        return SQLFragment(elements)

    @classmethod
    def list(cls, fragments: "Iterable[SQLFragment | SQLText | SQLVar]"):
        return SQLFragment.join(SQLText.comma, fragments)

    @classmethod
    def variadic(cls, fragments: "Sequence[SQLFragment | SQLText | SQLVar]"):
        if len(fragments) == 1:
            return fragments[0]
        return SQLFragment.paren(SQLFragment.list(fragments))

    @classmethod
    def concat(cls, *fragments: "SQLFragment | SQLText | SQLVar"):
        return SQLFragment.join(SQLText.space, fragments)

    @classmethod
    def paren(cls, fragment: "SQLFragment | SQLText | SQLVar"):
        return SQLFragment((SQLText.lparen, fragment, SQLText.rparen))

    @classmethod
    def func_call(cls, name: str, args: "Iterable[SQLFragment | SQLText | SQLVar]"):
        return SQLFragment((SQLText(name), SQLText.lparen, SQLFragment.join(SQLText.comma, args), SQLText.rparen))


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

        frag = SQLFragment.concat(
            SQLText("SELECT"),
            SQLFragment.list(SQLFragment.qn("t", col.name) for col in op.select),
            SQLText("FROM"),
            SQLFragment.qn(op.table._schema, op.table._name),
            SQLText("t"),
            SQLText("JOIN"),
            SQLFragment.func_call(
                "UNNEST",
                (SQLVar([rec[c] for rec in op.values], type=c.type + "[]") for c in op.where),
            ),
            SQLFragment.func_call("v", (SQLText.quote(c.name) for c in op.where)),
            SQLText("USING"),
            SQLFragment.paren(SQLFragment.list(SQLText.quote(c.name) for c in op.where)),
        )
        records = await self.fetch(*frag.build_sql())
        return [dict(zip(op.select, record)) for record in records]

    async def insert_records(self, op: InsertOperation) -> list[Rec]:
        if not op.values:
            return []

        frag = SQLFragment.concat(
            SQLText("INSERT INTO"),
            SQLFragment.qn(op.table._schema, op.table._name),
            SQLText("AS t"),
            SQLFragment.paren(SQLFragment.list(SQLText.quote(c.name) for c in op.insert_cols)),
            SQLText("SELECT"),
            SQLFragment.list(
                SQLFragment.func_call("COALESCE", (SQLFragment.qn("v", c.name), SQLText(c.default or "NULL")))
                for c in op.insert_cols
            ),
            SQLText("FROM"),
            SQLFragment.func_call(
                "UNNEST",
                (SQLVar([rec[c] for rec in op.values], type=c.type + "[]") for c in op.insert_cols),
            ),
            SQLFragment.func_call("v", (SQLText.quote(c.name) for c in op.insert_cols)),
            SQLText("RETURNING"),
            SQLFragment.list(SQLFragment.qn("t", c.name) for c in op.returning_cols),
        )
        records = await self.fetch(*frag.build_sql())
        return [dict(zip(op.returning_cols, record)) for record in records]

    async def update_records(self, op: UpdateOperation) -> list[Rec]:
        if not op.values:
            return []
        all_columns = [*op.where_cols, *op.update_cols]
        frag = SQLFragment.concat(
            SQLText("UPDATE"),
            SQLFragment.qn(op.table._schema, op.table._name),
            SQLText("t"),
            SQLText("SET"),
            SQLFragment.concat(
                SQLFragment.variadic([SQLText.quote(c.name) for c in op.update_cols]),
                SQLText("="),
                SQLFragment.variadic([SQLFragment.qn("v", c.name) for c in op.update_cols]),
            ),
            SQLText("FROM"),
            SQLFragment.func_call(
                "UNNEST",
                (SQLVar([rec[c] for rec in op.values], type=c.type + "[]") for c in all_columns),
            ),
            SQLFragment.func_call("v", (SQLText.quote(c.name) for c in all_columns)),
            SQLText("WHERE"),
            SQLFragment.concat(
                SQLFragment.variadic([SQLFragment.qn("t", c.name) for c in op.where_cols]),
                SQLText("="),
                SQLFragment.variadic([SQLFragment.qn("v", c.name) for c in op.where_cols]),
            ),
            SQLText("RETURNING"),
            SQLFragment.list(SQLFragment.qn("t", c.name) for c in op.returning_cols),
        )

        records = await self.fetch(*frag.build_sql())
        return [dict(zip(op.returning_cols, record)) for record in records]

    async def delete_by_keys(self, op: DeleteOperation):
        if not op.values:
            return []

        frag = SQLFragment.concat(
            SQLText("DELETE FROM"),
            SQLFragment.qn(op.table._schema, op.table._name),
            SQLText("t"),
            SQLText("USING"),
            SQLFragment.func_call(
                "UNNEST",
                (SQLVar([rec[c] for rec in op.values], type=c.type + "[]") for c in op.key_cols),
            ),
            SQLFragment.func_call("v", (SQLText.quote(c.name) for c in op.key_cols)),
            SQLText("WHERE"),
            SQLFragment.concat(
                SQLFragment.variadic([SQLFragment.qn("t", c.name) for c in op.key_cols]),
                SQLText("="),
                SQLFragment.variadic([SQLFragment.qn("v", c.name) for c in op.key_cols]),
            ),
        )

        await self.fetch(*frag.build_sql())

    async def fetch(self, query: str, params: Sequence[PostgresCodable]) -> list[typing.Mapping[str, PostgresCodable]]:
        return await self._conn.fetch(query, *params)


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

    # def select(self, *columns: str):
    #     return SessionBoundSelectQuery(self, columns)

    def find(self, entity_type: Type[TEntity], alias: str):
        mapping = self.mappings[entity_type]
        return SessionBoundFindQuery[TEntity](self, entity_type, alias)

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


class SessionBoundFindQuery(typing.Generic[TEntity]):
    class Sort(typing.NamedTuple):
        column: str
        ascending: bool

    def __init__(self, session: "Session", type: Type[Entity], alias: str):
        self._session = session
        self._alias = alias
        self._mapping = session.mappings[type]

        self._joins = {}
        self._filters = list[SQLFragment]()
        self._default_sort = [self.asc(col.name) for col in self._mapping.primary_key.columns]

    def join(self, target: Type[Entity] | str, alias: str, condition: str, **params):
        return self._join("JOIN", target, alias, condition, **params)

    def left_join(self, target: Type[Entity] | str, alias: str, condition: str, **params):
        return self._join("LEFT JOIN", target, alias, condition, **params)

    def _join(self, join_type: str, target: Type[Entity] | str, alias: str, condition: str, **params):
        if isinstance(target, type):
            table = self._session.mappings[target].table
            target_qn = SQLFragment.qn(table._schema, table._name)
        else:
            target_qn = SQLText(target)

        self._joins[alias] = (join_type, target_qn, SQLFragment.parse(condition, params))
        return self

    def filter(self, condition: str, **params):
        self._filters.append(SQLFragment.parse(condition, params))
        return self

    @classmethod
    def asc(cls, column: str):
        return cls.Sort(column, True)

    @classmethod
    def desc(cls, column: str):
        return cls.Sort(column, False)

    async def fetch(
        self,
        first: int | None = None,
        after: KeyValue | None = None,
        last: int | None = None,
        before: KeyValue | None = None,
        offset: int = 0,
        sort: Sequence[Sort] | None = None,
    ):
        cursor_fetch_results = await self.fetch_ids(first, after, last, before, offset, sort)
        objects = await self._session.batch_get(self._mapping.object_type, cursor_fetch_results)
        return FetchResult(
            elements=typing.cast(list[TEntity], objects),
            has_next_page=cursor_fetch_results.has_next_page,
            has_previous_page=cursor_fetch_results.has_previous_page,
            start_cursor=cursor_fetch_results.start_cursor,
            end_cursor=cursor_fetch_results.end_cursor,
        )

    async def fetch_one(self):
        results = await self.fetch(first=1)
        return results[0] if results else None

    async def fetch_ids(
        self,
        first: int | None = None,
        after: KeyValue | None = None,
        last: int | None = None,
        before: KeyValue | None = None,
        offset: int = 0,
        sort: Sequence[Sort] | None = None,
    ):
        count = first or last
        cursor = after or before
        is_forward = isinstance(first, int)

        assert count, 'Either "first" or "last" must be present'
        assert not (first and last), 'Argument "first" and "last" must not be present at the same time'
        assert not (after and before), 'Argument "after" and "before" must not be present at the same time'
        assert not (first and before), 'Argument "first" and "before" must not be present at the same time'
        assert not (last and after), 'Argument "last" and "after" must not be present at the same time'
        assert first is None or first >= 0, 'Argument "first" cannot be a negative number'
        assert last is None or last >= 0, 'Argument "last" cannot be a negative number'

        primary_columns = self._mapping.primary_key.columns
        fragment = self._format_pagination_frag(
            is_forward,
            count,
            cursor,
            offset,
            sort or self._default_sort,
        )
        results = await self._session._backend.fetch(*fragment.build_sql())

        partials = [dict(zip(primary_columns, record)) for record in results]
        ids = [self._mapping.primary_key.from_partial(rec) for rec in partials]

        has_cursor_side_edge = (ids[0] if ids else None) == cursor
        if has_cursor_side_edge:
            ids = ids[1:]
        has_extra_edge = len(ids) > count
        if has_extra_edge:
            ids = ids[:count]

        start_cursor = ids[0] if ids else None
        end_cursor = ids[-1] if ids else None

        if is_forward:
            return FetchResult(
                elements=ids,
                has_next_page=has_extra_edge,
                has_previous_page=has_cursor_side_edge,
                start_cursor=start_cursor,
                end_cursor=end_cursor,
            )
        else:
            return FetchResult(
                elements=ids[::-1],
                has_next_page=has_cursor_side_edge,
                has_previous_page=has_extra_edge,
                start_cursor=end_cursor,
                end_cursor=start_cursor,
            )

    async def count(self):
        primary_columns = self._mapping.primary_key.columns

        frag = SQLFragment.concat(
            SQLText("SELECT"),
            SQLFragment.func_call("COUNT", (SQLFragment.qn(self._alias, c.name) for c in primary_columns)),
            self._format_from_where_frag(),
        )
        results = await self._session._backend.fetch(*frag.build_sql())
        return int(results[0]["count"])

    def _format_pagination_frag(
        self,
        is_forward: bool,
        count: int,
        cursor: KeyValue | None,
        offset: int,
        sort: Sequence[Sort],
    ):
        assert sort
        primary_columns = self._mapping.primary_key.columns
        cursor_partial = self._mapping.primary_key.to_partial(cursor)

        return SQLFragment.concat(
            SQLFragment(
                (
                    SQLText("WITH to_find AS NOT MATERIALIZED "),
                    SQLFragment.paren(
                        SQLFragment.concat(
                            SQLText("SELECT"),
                            SQLFragment.list(
                                (
                                    *(
                                        SQLFragment.concat(
                                            SQLFragment.qn(self._alias, c.name), SQLText("AS"), SQLText(f"c{i}")
                                        )
                                        for i, c in enumerate(primary_columns)
                                    ),
                                    *(SQLText(f"{col} AS s{i}") for i, (col, _) in enumerate(sort)),
                                )
                            ),
                            self._format_from_where_frag(),
                            SQLText("GROUP BY"),
                            SQLFragment.list(
                                SQLFragment.concat(SQLFragment.qn(self._alias, c.name))
                                for i, c in enumerate(primary_columns)
                            ),
                        )
                    ),
                    SQLText.comma,
                    SQLText("cursor_edge AS "),
                    SQLFragment.paren(
                        SQLFragment.concat(
                            SQLText("SELECT"),
                            SQLFragment.list(
                                (
                                    *(SQLText(f"c{i}") for i, _ in enumerate(primary_columns)),
                                    *(SQLText(f"s{i}") for i, _ in enumerate(sort)),
                                )
                            ),
                            SQLText("FROM to_find"),
                            SQLText("WHERE"),
                            SQLFragment.join(
                                SQLText(" AND "),
                                (
                                    SQLFragment.parse(f"c{i} = :value", {"value": cursor_partial[c]})
                                    for i, c in enumerate(primary_columns)
                                ),
                            ),
                        )
                    ),
                )
            ),
            SQLText("SELECT"),
            SQLFragment.list(
                SQLFragment.concat(SQLFragment.qn("t1", f"c{i}"), SQLText("AS"), SQLText.quote(c.name))
                for i, c in enumerate(primary_columns)
            ),
            SQLText("FROM to_find t1 LEFT JOIN cursor_edge t2 ON TRUE"),
            SQLText("WHERE"),
            SQLFragment.func_call(
                "COALESCE",
                (
                    SQLFragment.join(
                        SQLText(" OR "),
                        (
                            SQLFragment.paren(
                                SQLFragment.join(
                                    SQLText(" AND "),
                                    (
                                        (
                                            SQLFragment.concat(
                                                SQLText(f"t1.s{j}"),
                                                SQLText(">") if is_forward == s.ascending else SQLText("<"),
                                                SQLText(f"t2.s{j}"),
                                            )
                                            if i == j
                                            else SQLText(f"t1.s{j} = t2.s{j}")
                                        )
                                        for j, s in enumerate(sort[: i + 1])
                                    ),
                                )
                            )
                            for i in range(len(sort) + 1)
                        ),
                    ),
                    SQLText("TRUE"),
                ),
            ),
            SQLText("ORDER BY"),
            SQLFragment.list(
                SQLFragment.concat(
                    SQLFragment.qn("t1", f"s{i}"),
                    SQLText("ASC") if is_forward == s.ascending else SQLText("DESC"),
                )
                for i, s in enumerate(sort)
            ),
            SQLText("LIMIT"),
            SQLVar(count + 1),
            SQLText("OFFSET"),
            SQLVar(offset),
        )

    def _format_from_where_frag(self):
        schema = self._mapping.table._schema
        table = self._mapping.table._name

        return SQLFragment.concat(
            SQLText("FROM"),
            SQLFragment.qn(schema, table),
            SQLText("AS"),
            SQLText.quote(self._alias),
            *(
                SQLFragment.concat(
                    SQLText(f"{join}"),
                    target,
                    SQLText("AS"),
                    SQLText.quote(alias),
                    SQLText("ON"),
                    condition,
                )
                for alias, (join, target, condition) in self._joins.items()
            ),
            SQLText("WHERE"),
            SQLFragment.join(SQLText(" AND "), self._filters),
        )


T = typing.TypeVar("T")


class FetchResult(list[T]):
    def __init__(
        self,
        elements: Iterable[T],
        has_next_page: bool,
        has_previous_page: bool,
        start_cursor: KeyValue | None,
        end_cursor: KeyValue | None,
    ):
        super().__init__(elements)
        self.has_next_page = has_next_page
        self.has_previous_page = has_previous_page
        self.start_cursor = start_cursor
        self.end_cursor = end_cursor


class SessionBoundRawQuery:
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
