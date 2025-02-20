import re
import types
import typing
from typing import Collection, Sequence, Type
from contextlib import asynccontextmanager

import asyncpg
import asyncpg.transaction

Entity = object
Key = typing.Hashable | tuple[typing.Hashable, ...]
PostgresCodable = typing.Any
Rec = dict[str, PostgresCodable]
EntityFactory = typing.Callable[[], Entity]
ParamMap = dict[typing.Hashable, PostgresCodable]
TEntity = typing.TypeVar("TEntity", bound=Entity)


class Field(typing.NamedTuple):
    column: str
    setter: typing.Callable[[Entity, PostgresCodable], None]
    getter: typing.Callable[[Entity], PostgresCodable]
    insertable: bool
    updatable: bool


class Child(typing.NamedTuple):
    target: type[Entity]
    getter: typing.Callable[[Entity], Sequence[Entity]]
    setter: typing.Callable[[Entity, Sequence[Entity]], None]


class EntityMapping(typing.NamedTuple):
    entity_type: Type[Entity]
    entity_factory: EntityFactory
    table: str
    schema: str
    fields: dict[str, Field]
    children: dict[str, Child]
    primary_key_fields: list[str]
    parental_key_fields: list[str]

    @property
    def selectable_cols(self):
        return [f.column for f in self.fields.values()]

    @property
    def insertable_cols(self):
        return [f.column for f in self.fields.values() if f.insertable]

    @property
    def updatable_cols(self):
        return [f.column for f in self.fields.values() if f.updatable]

    @property
    def primary_cols(self):
        return [self.fields[name].column for name in self.primary_key_fields]

    @property
    def parental_cols(self):
        return [self.fields[name].column for name in self.parental_key_fields]

    def id_from_entity(self, entity: Entity) -> Key:
        return self._key_from_entity(entity, self.primary_key_fields)

    def id_from_record(self, rec: Rec) -> Key:
        return self._key_from_record(rec, self.primary_key_fields)

    def id_to_record(self, id: Key) -> Rec:
        return self._key_to_record(id, self.primary_key_fields)

    def parental_key_from_entity(self, entity: Entity) -> Key:
        return self._key_from_entity(entity, self.parental_key_fields)

    def parental_key_from_record(self, rec: Rec) -> Key:
        return self._key_from_record(rec, self.parental_key_fields)

    def parental_key_to_record(self, val: Key) -> Rec:
        return self._key_to_record(val, self.parental_key_fields)

    def format_to_record(self, entity: Entity) -> Rec:
        return {f.column: f.getter(entity) for f in self.fields.values()}

    def write_to_entity(self, entity: Entity, rec: Rec):
        for field in self.fields.values():
            field.setter(entity, rec[field.column])

    def _key_from_entity(self, entity: Entity, field_names: Sequence[str]) -> Key:
        if len(field_names) == 1:
            return self.fields[field_names[0]].getter(entity)
        return tuple(self.fields[f].getter(entity) for f in field_names)

    def _key_from_record(self, rec: Rec, field_names: Sequence[str]) -> Key:
        if len(field_names) == 1:
            return rec[self.fields[field_names[0]].column]
        return tuple(rec[self.fields[f].column] for f in field_names)

    def _key_to_record(self, key: Key, field_names: Sequence[str]) -> Rec:
        if len(field_names) == 1:
            return {self.fields[field_names[0]].column: key}
        assert isinstance(key, tuple), f"Composite key must be a tuple but got {key}"
        return {self.fields[f].column: v for f, v in zip(field_names, key)}


sql_n = typing.NamedTuple("sql_n", [("part", str)])
sql_qn = typing.NamedTuple("sql_qn", [("part1", str), ("part2", str)])
sql_text = typing.NamedTuple("sql_text", [("text", str)])
sql_param = typing.NamedTuple("sql_param", [("id", typing.Hashable)])
sql_all = typing.NamedTuple("sql_all", [("els", list["SQL"])])
sql_any = typing.NamedTuple("sql_any", [("els", list["SQL"])])
sql_eq = typing.NamedTuple("sql_eq", [("left", "SQL"), ("right", "SQL")])
sql_lt = typing.NamedTuple("sql_lt", [("left", "SQL"), ("right", "SQL")])
sql_gt = typing.NamedTuple("sql_gt", [("left", "SQL"), ("right", "SQL")])
sql_is_null = typing.NamedTuple("sql_is_null", [("operand", "SQL")])
sql_is_not_null = typing.NamedTuple("sql_is_not_null", [("operand", "SQL")])
sql_fragment = typing.NamedTuple("sql_fragment", [("els", list["SQL"])])

SQL = (
    sql_n
    | sql_qn
    | sql_text
    | sql_param
    | sql_all
    | sql_any
    | sql_eq
    | sql_lt
    | sql_gt
    | sql_is_null
    | sql_is_not_null
    | sql_fragment
)


class SQLSelect(typing.NamedTuple):
    class Join(typing.NamedTuple):
        type: typing.Literal["JOIN", "LEFT JOIN"]
        table: SQL
        alias: SQL
        on: SQL

    class OrderBy(typing.NamedTuple):
        expr: SQL
        ascending: bool = True
        nulls_last: bool = True

    select: Collection[SQL]
    from_table: SQL
    from_alias: SQL
    joins: Collection[Join] = ()
    where: SQL | None = None
    order_bys: Collection[OrderBy] = ()
    group_by: Collection[SQL] = ()
    having: SQL | None = None
    limit: SQL | None = None
    offset: SQL | None = None


class SQLInsert(typing.NamedTuple):
    into_table: SQL
    insert: Collection[SQL]
    values: Collection[SQL]
    returning: Collection[SQL]


class SQLUpdate(typing.NamedTuple):
    table: SQL
    sets: Collection[tuple[SQL, SQL]]
    where: SQL
    returning: Collection[SQL]


class SQLDelete(typing.NamedTuple):
    from_table: SQL
    where: SQL
    returning: Collection[SQL]


class SessionBackend(typing.Protocol):
    async def select(self, stmt: SQLSelect, *param_maps: ParamMap) -> list[Rec]: ...
    async def insert(self, stmt: SQLInsert, *param_maps: ParamMap) -> list[Rec]: ...
    async def update(self, stmt: SQLUpdate, *param_maps: ParamMap) -> list[Rec]: ...
    async def delete(self, stmt: SQLDelete, *param_maps: ParamMap) -> list[Rec]: ...
    async def count(self, stmt: SQLSelect, param_map: ParamMap) -> int: ...
    async def fetch_raw(self, raw: SQL, param_map: ParamMap) -> list[Rec]: ...
    async def begin(self): ...
    async def commit(self): ...
    async def rollback(self): ...
    async def savepoint(self, name: str): ...
    async def release(self, name: str): ...
    async def rollback_to(self, name: str): ...


class Session:
    def __init__(self, backend: SessionBackend, mappings: Collection[EntityMapping]):
        self._backend = backend
        self._mappings = {mapping.entity_type: mapping for mapping in mappings}
        self._idm = {}
        self._tx_depth = 0

    async def get(self, entity_type: Type[TEntity], id: typing.Any):
        return (await self.batch_get(entity_type, (id,)))[0]

    async def save(self, entity: Entity):
        await self.batch_save(type(entity), entity)

    async def delete(self, entity: Entity):
        return (await self.batch_delete(type(entity), entity))[0]

    async def batch_get(self, entity_type: Type[TEntity], ids: typing.Iterable[typing.Any]):
        mapping = self.get_mapping(entity_type)
        entity_map = await self._get(mapping, mapping.primary_cols, [mapping.id_to_record(id) for id in ids])
        return typing.cast(list[TEntity | None], [entity_map.get(id) for id in ids])

    async def batch_save(self, entity_type: Type[TEntity], *entities: TEntity):
        mapping = self.get_mapping(entity_type)
        await self._save(mapping, ((mapping.parental_key_from_entity(e), e) for e in entities))

    async def batch_delete(self, entity_type: Type[TEntity], *entities: TEntity):
        return await self._delete(self.get_mapping(entity_type), entities)

    def query(self, entity_type: Type[TEntity], alias: str):
        return SessionEntityQuery[TEntity](self, self.get_mapping(entity_type), alias)

    def raw(self, query: str, **params):
        return SessionRawQuery(self, query, params)

    @asynccontextmanager
    async def tx(self):
        await self._start_tx()
        prev_idm = {k: dict(v) for k, v in self._idm.items()}
        try:
            yield
            await self._end_tx()
        except Exception:
            await self._rollback_tx()
            self._idm = prev_idm
            raise

    async def _start_tx(self):
        if self._tx_depth == 0:
            await self._backend.begin()
        else:
            await self._backend.savepoint(f"tx_{self._tx_depth}")
        self._tx_depth += 1

    async def _end_tx(self):
        self._tx_depth -= 1
        if self._tx_depth == 0:
            await self._backend.commit()
        else:
            await self._backend.release(f"tx_{self._tx_depth}")

    async def _rollback_tx(self):
        self._tx_depth -= 1
        if self._tx_depth == 0:
            await self._backend.rollback()
        else:
            await self._backend.rollback_to(f"tx_{self._tx_depth}")

    async def _get(self, mapping: EntityMapping, where: list[str], values: list[Rec]):
        select_stmt = SQLSelect(
            select=[sql_qn("t", c) for c in mapping.selectable_cols],
            from_table=sql_qn(mapping.schema, mapping.table),
            from_alias=sql_n("t"),
            where=sql_all([sql_eq(sql_qn("t", c), sql_param(i)) for i, c in enumerate(where)]),
        )
        param_lists = (ParamMap((i, rec[c]) for i, c in enumerate(where)) for rec in values)
        recs = await self._backend.select(select_stmt, *param_lists)
        ent_map: dict[Key, Entity] = {}
        for rec in recs:
            id = mapping.id_from_record(rec)
            parental_key = mapping.parental_key_from_record(rec)
            entity = self._get_tracked(mapping, parental_key, id) or mapping.entity_factory()
            mapping.write_to_entity(entity, rec)
            self._track(entity)
            ent_map[id] = entity

        for child in mapping.children.values():
            child_mapping = self.get_mapping(child.target)
            parental_keys = [child_mapping.parental_key_to_record(k) for k in ent_map]
            child_ent_map = await self._get(child_mapping, child_mapping.parental_cols, parental_keys)

            child_groups = dict[Key, list[Entity]]()
            for child_entity in child_ent_map.values():
                key = child_mapping.parental_key_from_entity(child_entity)
                child_groups.setdefault(key, []).append(child_entity)

            for id, entity in ent_map.items():
                child.setter(entity, child_groups.get(id, ()))

        return ent_map

    async def _save(self, mapping: EntityMapping, entities: typing.Iterable[tuple[Key, Entity]]):
        values = list[tuple[Entity, Rec]]()
        for key, entity in entities:
            rec = mapping.format_to_record(entity)
            rec.update(mapping.parental_key_to_record(key))
            values.append((entity, rec))

        if to_update := [(e, r) for e, r in values if self._in_track(e)]:
            update_stmt = SQLUpdate(
                table=sql_qn(mapping.schema, mapping.table),
                sets=[(sql_n(c), sql_param(i)) for i, c in enumerate(mapping.updatable_cols)],
                where=sql_all(
                    [
                        sql_eq(sql_n(c), sql_param(i + len(mapping.updatable_cols)))
                        for i, c in enumerate(mapping.primary_cols)
                    ]
                ),
                returning=[sql_n(c) for c in mapping.selectable_cols],
            )
            columns = [*mapping.updatable_cols, *mapping.primary_cols]
            param_lists = (ParamMap((i, rec[c]) for i, c in enumerate(columns)) for _, rec in to_update)
            updated_recs = await self._backend.update(update_stmt, *param_lists)
            for i in range(len(to_update)):
                mapping.write_to_entity(to_update[i][0], updated_recs[i])

        if to_insert := [(e, r) for e, r in values if not self._in_track(e)]:
            insert_stmt = SQLInsert(
                into_table=sql_qn(mapping.schema, mapping.table),
                insert=[sql_n(c) for c in mapping.insertable_cols],
                values=[sql_param(i) for i in range(len(mapping.insertable_cols))],
                returning=[sql_n(c) for c in mapping.selectable_cols],
            )
            columns = mapping.insertable_cols
            param_lists = (ParamMap((i, rec[c]) for i, c in enumerate(columns)) for _, rec in to_insert)
            inserted_recs = await self._backend.insert(insert_stmt, *param_lists)
            for i in range(len(to_insert)):
                mapping.write_to_entity(to_insert[i][0], inserted_recs[i])
                self._track(to_insert[i][0])

        for child in mapping.children.values():
            child_mapping = self.get_mapping(child.target)
            to_delete = list[Entity]()
            to_save = list[tuple[Key, Entity]]()
            for entity, _ in to_update:
                id = mapping.id_from_entity(entity)
                child_entities = child.getter(entity)
                current_ids = {child_mapping.id_from_entity(e) for e in child_entities}
                previous_ids = self._get_tracked_children(child_mapping, id)
                to_delete.extend(previous_ids[id] for id in previous_ids if id not in current_ids)

            for entity, _ in values:
                id = mapping.id_from_entity(entity)
                child_entities = child.getter(entity)
                to_save.extend((id, child_entity) for child_entity in child_entities)

            if to_delete:
                await self._delete(child_mapping, to_delete)
            if to_save:
                await self._save(child_mapping, to_save)

    async def _delete(self, mapping: EntityMapping, entities: Collection[Entity]):
        ids = [mapping.id_from_entity(entity) for entity in entities]
        for child in mapping.children.values():
            child_mapping = self.get_mapping(child.target)
            child_entities = [
                self._get_tracked(child_mapping, id, child_id)
                for id in ids
                for child_id in self._get_tracked_children(child_mapping, id)
            ]
            await self._delete(child_mapping, child_entities)

        recs = [mapping.id_to_record(mapping.id_from_entity(e)) for e in entities]
        stmt = SQLDelete(
            from_table=sql_qn(mapping.schema, mapping.table),
            where=sql_all([sql_eq(sql_n(c), sql_param(i)) for i, c in enumerate(mapping.primary_cols)]),
            returning=[sql_n(c) for c in mapping.primary_cols + mapping.parental_cols],
        )
        param_maps = (ParamMap((i, rec[c]) for i, c in enumerate(mapping.primary_cols)) for rec in recs)
        deleted_recs = await self._backend.delete(stmt, *param_maps)

        deleted = dict[Key, bool]()
        for rec in deleted_recs:
            id = mapping.id_from_record(rec)
            parental_key = mapping.parental_key_from_record(rec)
            self._untrack(mapping, parental_key, id)
            deleted[id] = True

        return [deleted.get(id, False) for id in ids]

    async def fetch_session_entity_query(self, query: "SessionEntityQuery", limit: int | None, offset: int | None):
        params = ParamMap(query.params)
        limit_ref = query.ctx.new_param_id()
        offset_ref = query.ctx.new_param_id()
        params.update({limit_ref: limit, offset_ref: offset})
        select_stmt = SQLSelect(
            select=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            from_table=sql_qn(query.mapping.schema, query.mapping.table),
            from_alias=sql_n(query.alias),
            joins=query.joins.values(),
            where=sql_all(query.where_conds) if query.where_conds else None,
            group_by=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            having=sql_all(query.having_conds) if query.having_conds else None,
            order_bys=query.order_by_opts,
            limit=sql_param(limit_ref),
            offset=sql_param(offset_ref),
        )
        recs = await self._backend.select(select_stmt, params)
        ids = [query.mapping.id_from_record(rec) for rec in recs]
        return await self.batch_get(query.mapping.entity_type, ids)

    async def count_session_entity_query(self, query: "SessionEntityQuery"):
        select_stmt = SQLSelect(
            select=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            from_table=sql_qn(query.mapping.schema, query.mapping.table),
            from_alias=sql_n(query.alias),
            joins=query.joins.values(),
            where=sql_all(query.where_conds) if query.where_conds else None,
            group_by=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            having=sql_all(query.having_conds) if query.having_conds else None,
        )
        return await self._backend.count(select_stmt, query.params)

    class PageOpts(typing.NamedTuple):
        first: int | None
        after: Key | None
        last: int | None
        before: Key | None
        offset: int | None

    class Page(typing.NamedTuple):
        cursors: list[Key]
        has_previous_page: bool
        has_next_page: bool

    async def paginate_session_entity_query(self, query: "SessionEntityQuery", opts: PageOpts):
        first, after, last, before, offset = opts

        order_by = [*query.order_by_opts]
        for c in query.mapping.primary_cols:
            order_by.append(SQLSelect.OrderBy(sql_qn(query.alias, c)))
        if last is not None:
            order_by = [SQLSelect.OrderBy(o.expr, not o.ascending, not o.nulls_last) for o in order_by]

        params = ParamMap(query.params)
        filters = query.having_conds.copy()

        if after_rec := await self._fetch_cursor_rec(query, order_by, after) if after else None:
            after_params = ParamMap((query.ctx.new_param_id(), v) for v in after_rec.values())
            predicate = self._format_cursor_predicate(order_by, after_params, last is None)
            filters.append(predicate)
            params.update(after_params)
        if before_rec := await self._fetch_cursor_rec(query, order_by, before) if before else None:
            before_params = ParamMap((query.ctx.new_param_id(), v) for v in before_rec.values())
            predicate = self._format_cursor_predicate(order_by, before_params, last is not None)
            filters.append(predicate)
            params.update(before_params)

        limit = first if last is None else last
        limit_ref = query.ctx.new_param_id()
        offset_ref = query.ctx.new_param_id()
        params.update({limit_ref: limit + 1 if limit else None, offset_ref: offset})

        select_stmt = SQLSelect(
            select=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            from_table=sql_qn(query.mapping.schema, query.mapping.table),
            from_alias=sql_n(query.alias),
            joins=query.joins.values(),
            where=sql_all(query.where_conds) if query.where_conds else None,
            group_by=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            having=sql_all(filters) if filters else None,
            order_bys=order_by,
            limit=sql_param(limit_ref),
            offset=sql_param(offset_ref),
        )
        records = await self._backend.select(select_stmt, params)

        keys_records = records[:limit]

        if last is None:
            has_previous_page = bool(after_rec) or bool(offset)
            has_next_page = len(records) > limit if limit else False
        else:
            has_previous_page = len(records) > limit if limit else False
            has_next_page = bool(before_rec) or bool(offset)
            keys_records.reverse()

        ids = [query.mapping.id_from_record(rec) for rec in keys_records]
        return self.Page(ids, has_previous_page, has_next_page)

    async def _fetch_cursor_rec(self, query: "SessionEntityQuery", order_bys: Sequence[SQLSelect.OrderBy], cursor: Key):
        cursor_rec = query.mapping.id_to_record(cursor)
        params = ParamMap(query.params)
        filters = query.having_conds.copy()
        for c in query.mapping.primary_cols:
            param_id = query.ctx.new_param_id()
            params[param_id] = cursor_rec[c]
            filters.append(sql_eq(sql_qn(query.alias, c), sql_param(param_id)))

        select_stmt = SQLSelect(
            select=[o.expr for o in order_bys],
            from_table=sql_qn(query.mapping.schema, query.mapping.table),
            from_alias=sql_n(query.alias),
            joins=query.joins.values(),
            where=sql_all(query.where_conds) if query.where_conds else None,
            group_by=[sql_qn(query.alias, c) for c in query.mapping.primary_cols],
            having=sql_all(filters),
        )
        recs = await self._backend.select(select_stmt, params)
        return recs[0] if recs else None

    def _format_cursor_predicate(self, order_bys: list[SQLSelect.OrderBy], params: ParamMap, is_forward: bool):
        param_refs = [sql_param(i) for i in params]
        or_predicates: list[SQL] = []
        for i, _ in enumerate(order_bys):
            and_predicates: list[SQL] = []
            for j, sort in enumerate(order_bys[: i + 1]):
                v = param_refs[j]
                if i != j:
                    comp = sql_eq(v, sort.expr)
                elif sort.ascending == is_forward:
                    comp = sql_lt(v, sort.expr)
                else:
                    comp = sql_gt(v, sort.expr)
                if i != j:
                    null = sql_all([sql_is_null(v), sql_is_null(sort.expr)])
                elif sort.nulls_last == is_forward:
                    null = sql_all([sql_is_not_null(v), sql_is_null(sort.expr)])
                else:
                    null = sql_all([sql_is_null(v), sql_is_not_null(sort.expr)])
                and_predicates.append(sql_any([comp, null]))
            or_predicates.append(sql_all(and_predicates))
        return sql_any(or_predicates)

    async def fetch_raw_query(self, query: "SessionRawQuery"):
        return await self._backend.fetch_raw(query.fragment, query.params)

    _mappings: dict[type, EntityMapping]

    def get_mapping(self, entity_type: type):
        return self._mappings[entity_type]

    _idm: dict[tuple[type, Key], dict[Key, object]]

    def _track(self, entity: Entity):
        mapping = self.get_mapping(type(entity))
        parental_key = mapping.parental_key_from_entity(entity)
        scope = self._idm.setdefault((mapping.entity_type, parental_key), {})
        scope[mapping.id_from_entity(entity)] = entity

    def _untrack(self, mapping: EntityMapping, parental_key: Key, id: Key):
        self._idm[(mapping.entity_type, parental_key)].pop(id, None)

    def _in_track(self, entity: Entity):
        mapping = self.get_mapping(type(entity))
        parental_key = mapping.parental_key_from_entity(entity)
        id = mapping.id_from_entity(entity)
        return id in self._idm.get((mapping.entity_type, parental_key), {})

    def _get_tracked(self, mapping: EntityMapping, parental_key: Key, id: Key):
        return self._idm.get((mapping.entity_type, parental_key), {}).get(id)

    def _get_tracked_children(self, mapping: EntityMapping, parental_key: Key):
        return self._idm.get((mapping.entity_type, parental_key), {})


class SQLBuildingContext:
    def __init__(self, start_pointer=0):
        self._param_pointer = start_pointer

    _patt_word = re.compile(r"('[^']*'|\"[^\"]*\"|\s+|::|:\w+|\w+|[^\w\s])")
    _patt_param = re.compile(r":(\w+)")

    def parse(self, sql: str, params: dict[str, PostgresCodable]):
        tokens = list[SQL]()
        words: list[str] = self._patt_word.findall(sql)
        param_index_map = dict[str, int]()
        param_map = ParamMap()

        for word in words:
            if matched := self._patt_param.match(word):
                param_name = matched[1]
                assert param_name in params, f"Parameter '{param_name}' not provided"
                param_id = param_index_map.get(param_name)
                if param_id is None:
                    param_id = self.new_param_id()
                    param_index_map[param_name] = param_id
                    param_map[param_id] = params[param_name]
                tokens.append(sql_param(param_id))
            else:
                tokens.append(sql_text(word))

        return sql_fragment(tokens), param_map

    def new_param_id(self):
        param_id = self._param_pointer
        self._param_pointer += 1
        return param_id


class SQLRenderingContext:
    def __init__(self):
        self._param_locs = dict[typing.Hashable, int]()

    def locate_param(self, id: typing.Hashable):
        return self._param_locs.setdefault(id, len(self._param_locs))

    def format_params(self, params: ParamMap):
        return [params[id] for id in self._param_locs]


class SessionEntityQuery(typing.Generic[TEntity]):
    def __init__(self, session: Session, mapping: EntityMapping, alias: str):
        self._session = session
        self.mapping = mapping
        self.alias = alias
        self.params = ParamMap()
        self.joins = dict[str, SQLSelect.Join]()
        self.where_conds = list[SQL]()
        self.having_conds = list[SQL]()
        self.order_by_opts = tuple[SQLSelect.OrderBy, ...]()
        self.ctx = SQLBuildingContext()

    def join(self, target: type | str, alias: str, on: str, **params):
        self.joins[alias] = SQLSelect.Join(
            type="JOIN",
            table=self._get_target(target, params),
            alias=sql_n(alias),
            on=self._parse(on, params),
        )
        return self

    def left_join(self, target: type | str, alias: str, on: str, **params):
        self.joins[alias] = SQLSelect.Join(
            type="LEFT JOIN",
            table=self._get_target(target, params),
            alias=sql_n(alias),
            on=self._parse(on, params),
        )
        return self

    def where(self, condition: str, **params):
        self.where_conds.append(self._parse(f"({condition})", params))
        return self

    def having(self, condition: str, **params):
        self.having_conds.append(self._parse(f"({condition})", params))
        return self

    def order_by(self, *order_by: SQLSelect.OrderBy):
        self.order_by_opts = order_by
        return self

    async def fetch(self, limit: int | None = None, offset: int | None = None):
        entities = await self._session.fetch_session_entity_query(self, limit, offset)
        return typing.cast(list[TEntity], entities)

    async def fetch_one(self):
        results = await self.fetch(limit=1, offset=0)
        return results[0] if results else None

    async def count(self):
        return await self._session.count_session_entity_query(self)

    async def paginate(
        self,
        first: int | None = None,
        after: Key | None = None,
        last: int | None = None,
        before: Key | None = None,
        offset: int | None = None,
    ):
        opts = Session.PageOpts(first, after, last, before, offset)
        return await self._session.paginate_session_entity_query(self, opts)

    def asc(self, expr: str, nulls_last=True, **params):
        return SQLSelect.OrderBy(self._parse(expr, params), True, nulls_last)

    def desc(self, expr: str, nulls_last=True, **params):
        return SQLSelect.OrderBy(self._parse(expr, params), False, nulls_last)

    def _get_target(self, target: type | str, params: dict):
        if isinstance(target, str):
            return self._parse(target, params)
        target_mapping = self._session.get_mapping(target)
        return sql_qn(target_mapping.schema, target_mapping.table)

    def _parse(self, sql: str, params: dict):
        fragment, params = self.ctx.parse(sql, params)
        self.params.update(params)
        return fragment


class SessionRawQuery:
    def __init__(self, session: "Session", query: str, params: dict[str, PostgresCodable]):
        self._session = session
        self.fragment, self.params = SQLBuildingContext().parse(query, params)

    async def fetch(self):
        return await self._session.fetch_raw_query(self)

    async def fetch_one(self):
        results = await self.fetch()
        return results[0] if results else None


class AsyncPGSessionBackend(SessionBackend):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._active: asyncpg.Pool | asyncpg.Connection = pool
        self._tx: asyncpg.transaction.Transaction | None = None

    async def select(self, stmt: SQLSelect, *param_maps: ParamMap):
        query, param_lists = self.Renderer().render_select(stmt, *param_maps)
        records = await self._active.fetchmany(query, param_lists)
        return [dict(rec.items()) for rec in records]

    async def insert(self, stmt: SQLInsert, *param_maps: ParamMap):
        query, param_lists = self.Renderer().render_insert(stmt, *param_maps)
        records = await self._active.fetchmany(query, param_lists)
        return [dict(rec.items()) for rec in records]

    async def update(self, stmt: SQLUpdate, *param_maps: ParamMap):
        query, param_lists = self.Renderer().render_update(stmt, *param_maps)
        records = await self._active.fetchmany(query, param_lists)
        return [dict(rec.items()) for rec in records]

    async def delete(self, stmt: SQLDelete, *param_maps: ParamMap):
        query, param_lists = self.Renderer().render_delete(stmt, *param_maps)
        records = await self._active.fetchmany(query, param_lists)
        return [dict(rec.items()) for rec in records]

    async def count(self, stmt: SQLSelect, param_map: ParamMap):
        query, param_list = self.Renderer().render_count(stmt, param_map)
        records = await self._active.fetch(query, *param_list)
        return records[0]["count"]

    async def fetch_raw(self, raw: SQL, param_map: ParamMap):
        query, param_list = self.Renderer().render_raw(raw, param_map)
        records = await self._active.fetch(query, *param_list)
        return [dict(rec.items()) for rec in records]

    async def begin(self):
        assert not self._tx
        conn: asyncpg.Connection = await self._pool.acquire()
        tx: asyncpg.transaction.Transaction = conn.transaction()
        await tx.start()
        self._active = conn
        self._tx = tx

    async def commit(self):
        assert self._tx
        try:
            await self._tx.commit()
        finally:
            await self._pool.release(self._active)
            self._active = self._pool
            self._tx = None

    async def rollback(self):
        assert self._tx
        try:
            await self._tx.rollback()
        finally:
            await self._pool.release(self._active)
            self._active = self._pool
            self._tx = None

    async def savepoint(self, name: str):
        await self._active.execute(f"SAVEPOINT {name}")

    async def release(self, name: str):
        await self._active.execute(f"RELEASE SAVEPOINT {name}")

    async def rollback_to(self, name: str):
        await self._active.execute(f"ROLLBACK TO SAVEPOINT {name}")

    class Renderer:
        def __init__(self):
            self._ctx = SQLRenderingContext()

        def render_select(self, stmt: SQLSelect, *param_maps: ParamMap):
            parts = ["SELECT"]
            parts.append(", ".join(self._el(i) for i in stmt.select))
            parts.append(f"FROM {self._el(stmt.from_table)} AS {self._el(stmt.from_alias)}")
            for join in stmt.joins:
                parts.append(self._sql_join_opt(join))
            if stmt.where:
                parts.append(f"WHERE {self._el(stmt.where)}")
            if stmt.group_by:
                parts.append(f"GROUP BY {', '.join(self._el(i) for i in stmt.group_by)}")
            if stmt.having:
                parts.append(f"HAVING {self._el(stmt.having)}")
            if stmt.order_bys:
                parts.append(f"ORDER BY {', '.join(self._sql_order_opt(opt) for opt in stmt.order_bys)}")
            if stmt.limit:
                parts.append(f"LIMIT {self._el(stmt.limit)}")
            if stmt.offset:
                parts.append(f"OFFSET {self._el(stmt.offset)}")
            query = " ".join(parts)
            param_lists = [self._ctx.format_params(param_map) for param_map in param_maps]
            return query, param_lists

        def render_insert(self, stmt: SQLInsert, *param_maps: ParamMap):
            parts = ["INSERT INTO", self._el(stmt.into_table)]
            parts.append(f"({', '.join(self._el(i) for i in stmt.insert)})")
            parts.append(f"VALUES ({', '.join(self._el(i) for i in stmt.values)})")
            parts.append(f"RETURNING {', '.join(self._el(i) for i in stmt.returning)}")
            query = " ".join(parts)
            param_lists = [self._ctx.format_params(param_map) for param_map in param_maps]
            return query, param_lists

        def render_update(self, stmt: SQLUpdate, *param_maps: ParamMap):
            parts = ["UPDATE", self._el(stmt.table)]
            parts.append(f"SET {', '.join(f'{self._el(k)} = {self._el(v)}' for k, v in stmt.sets)}")
            parts.append(f"WHERE {self._el(stmt.where)}")
            parts.append(f"RETURNING {', '.join(self._el(i) for i in stmt.returning)}")
            query = " ".join(parts)
            param_lists = [self._ctx.format_params(param_map) for param_map in param_maps]
            return query, param_lists

        def render_delete(self, stmt: SQLDelete, *param_maps: ParamMap):
            parts = ["DELETE FROM", self._el(stmt.from_table)]
            parts.append(f"WHERE {self._el(stmt.where)}")
            parts.append(f"RETURNING {', '.join(self._el(i) for i in stmt.returning)}")
            query = " ".join(parts)
            param_lists = [self._ctx.format_params(param_map) for param_map in param_maps]
            return query, param_lists

        def render_count(self, stmt: SQLSelect, param_map: ParamMap):
            select_query, params = self.render_select(stmt, param_map)
            return f"SELECT COUNT(*) FROM ({select_query}) AS _", params[0]

        def render_raw(self, raw: SQL, param_map: ParamMap):
            return self._el(raw), self._ctx.format_params(param_map)

        def _sql_join_opt(self, opt: SQLSelect.Join):
            return f"{opt.type} {self._el(opt.table)} {self._el(opt.alias)} ON {self._el(opt.on)}"

        def _sql_order_opt(self, opt: SQLSelect.OrderBy):
            direction = "ASC" if opt.ascending else "DESC"
            nulls = "NULLS LAST" if opt.nulls_last else "NULLS FIRST"
            return f"{self._el(opt.expr)} {direction} {nulls}"

        def _el(self, el: SQL):
            match el:
                case sql_n(part1):
                    return f'"{part1.replace(".", '"."')}"'
                case sql_qn(part1, part2):
                    return f'"{part1.replace(".", '"."')}"."{part2.replace('"', '""')}"'
                case sql_text(text):
                    return text
                case sql_param(id):
                    return f"${self._ctx.locate_param(id) + 1}"
                case sql_all(els):
                    return f"({' AND '.join(self._el(e) for e in els)})"
                case sql_any(els):
                    return f"({' OR '.join(self._el(e) for e in els)})"
                case sql_eq(left, right):
                    return f"({self._el(left)} = {self._el(right)})"
                case sql_lt(left, right):
                    return f"({self._el(left)} < {self._el(right)})"
                case sql_gt(left, right):
                    return f"({self._el(left)} > {self._el(right)})"
                case sql_is_null(expr):
                    return f"({self._el(expr)} IS NULL)"
                case sql_is_not_null(expr):
                    return f"({self._el(expr)} IS NOT NULL)"
                case sql_fragment(elements):
                    return "".join(self._el(e) for e in elements)


class AutoMappingBuilder:
    class FieldConfig(typing.TypedDict, total=False):
        column: str
        skip_on_insert: bool
        skip_on_update: bool

    class ChildConfig(typing.TypedDict):
        kind: typing.Literal["singular", "plural"]
        target: type | typing.Callable[[], type]

    class EntityMappingConfig(typing.TypedDict, total=False):
        schema: str
        table: str
        primary_key: str | Collection[str]
        parental_key: str | Collection[str]
        fields: dict[str, "AutoMappingBuilder.FieldConfig"]
        children: dict[str, "AutoMappingBuilder.ChildConfig"]
        factory: EntityFactory

    def __init__(self):
        self._configs = dict[type, self.EntityMappingConfig]()
        self._mappings = dict[type, EntityMapping]()

    def map(self, entity_type: type[TEntity], **kwargs: typing.Unpack[EntityMappingConfig]):
        self._configs[entity_type] = kwargs
        return entity_type

    def mapped(self, **kwargs: typing.Unpack[EntityMappingConfig]):
        return lambda entity_type: self.map(entity_type, **kwargs)

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

    def _build_entity_mapping(self, entity_type: type, opts: EntityMappingConfig):
        field_configs = dict(opts.get("fields", ()))
        child_configs = dict(opts.get("children", ()))

        primary = opts.get("primary_key", ["id"])
        primary = [primary] if isinstance(primary, str) else list(primary)
        parental = opts.get("parental_key", [])
        parental = [parental] if isinstance(parental, str) else list(parental)

        for name, type_hint in typing.get_type_hints(entity_type).items():
            origin = typing.get_origin(type_hint)
            args = typing.get_args(type_hint)
            # skip private fields
            if name.startswith("_"):
                continue
            # skip registered fields
            elif name in field_configs or name in child_configs:
                continue
            # list of registered entity
            elif origin is list and args[0] in self._configs:
                child_configs[name] = self.ChildConfig(kind="plural", target=args[0])
            # registered entity
            elif type_hint in self._configs:
                child_configs[name] = self.ChildConfig(kind="singular", target=type_hint)
            # optional of registered entity
            elif (
                origin in (types.UnionType, typing.Union)
                and len(args) == 2
                and args[1] is type(None)
                and args[0] in self._configs
            ):
                child_configs[name] = self.ChildConfig(kind="singular", target=args[0])
            else:
                field_configs[name] = self.FieldConfig(column=self._column_name(name))

        fields = {name: self._build_field(name, config, primary, parental) for name, config in field_configs.items()}
        children = {name: self._build_child(name, config) for name, config in child_configs.items()}

        return EntityMapping(
            entity_type=entity_type,
            schema=opts.get("schema", "public"),
            table=opts.get("table", self._table_name(entity_type.__name__)),
            fields=fields,
            primary_key_fields=primary,
            parental_key_fields=parental,
            children=children,
            entity_factory=opts.get("factory", lambda: object.__new__(entity_type)),
        )

    def _build_field(self, name: str, config: FieldConfig, primary: list[str], parental: list[str]):
        column = config["column"] if "column" in config else self._column_name(name)
        is_primary = name in primary
        is_parental = name in parental
        skip_on_insert = config.get("skip_on_insert", False)
        skip_on_update = config.get("skip_on_update", False)
        return Field(
            column=column,
            insertable=is_parental or not skip_on_insert,
            updatable=not (is_primary or is_parental or skip_on_update),
            getter=lambda entity, name=name: getattr(entity, name, None),
            setter=lambda entity, value, name=name: setattr(entity, name, value),
        )

    def _build_child(self, name: str, config: ChildConfig):
        target = config["target"] if isinstance(config["target"], type) else config["target"]()
        if config["kind"] == "singular":
            return Child(
                target=target,
                getter=lambda entity: tuple(i for i in (getattr(entity, name),) if i is not None),
                setter=lambda entity, value: setattr(entity, name, next(iter(value), None)),
            )
        else:
            return Child(
                target=target,
                getter=lambda entity, name=name: getattr(entity, name) or (),
                setter=lambda entity, value, name=name: setattr(entity, name, list(value)),
            )

    _name_patt = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

    def _column_name(self, s: str):
        return self._name_patt.sub("_", s).lower()

    def _table_name(self, s: str):
        return self._name_patt.sub("_", s).lower()


auto = AutoMappingBuilder()
