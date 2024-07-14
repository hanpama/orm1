import argparse
import asyncio
import glob
import os
import re
import types
import typing

import asyncpg

from . import definition_registry

_mod = __package__


async def main():
    """
    Code generator for database schema and entity mappings.

    Example usage:

        ```shell
        python -m orm1 -d postgresql://postgres:8800bc84f23af727f4e9@localhost:3200/postgres -e 'tests/entities/*.py' -o tests/database
        ```
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dsn", required=True)
    parser.add_argument("-e", "--entities", required=True)
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    for filename in glob.glob(args.entities):
        if filename.endswith(".py"):
            mod_name = filename.replace(os.path.sep, ".").removesuffix(".py")
            __import__(mod_name)

    entities = _build_definitions()
    introspection = await _get_introspection(args.dsn, entities.values())

    _render_database_spec(introspection, args.output)
    EntityMappingRenderer(entities).render(args.output)


class EntityDefinition(typing.NamedTuple):
    entity_type: type
    schema: str
    table: str
    primary: typing.Sequence[str]
    fields: dict[str, "EntityFieldDefinition"]
    children: dict[str, "EntityChildDefinition"]


class EntityFieldDefinition(typing.NamedTuple):
    name: str
    column: str
    is_primary: bool


class EntityChildDefinition(typing.NamedTuple):
    name: str
    kind: typing.Literal["plural", "singular"]
    target_entity: type
    columns: list[str]


def _build_definitions():

    entities = dict[type, EntityDefinition]()

    for cls, opts in definition_registry.items():
        field_configs = opts.get("fields", lambda: {})()
        child_configs = opts.get("children", lambda: {})()
        maybe_str_or_list = opts.get("primary", "id")
        if isinstance(maybe_str_or_list, str):
            primary = [maybe_str_or_list]
        else:
            primary = maybe_str_or_list

        fields = {
            name: EntityFieldDefinition(
                name=name,
                column=column,
                is_primary=name in primary,
            )
            for name, column in field_configs.items()
        }
        children = {
            name: EntityChildDefinition(
                name=name,
                kind=config["kind"],
                target_entity=config["target"],
                columns=config["columns"],
            )
            for name, config in child_configs.items()
        }

        definition = EntityDefinition(
            entity_type=cls,
            schema=opts.get("schema", "public"),
            primary=primary,
            table=opts.get("table", _ensure_snake_case(cls.__name__)),
            fields=fields,
            children=children,
        )

        entities[cls] = definition

    for cls, opts in definition_registry.items():
        definition = entities[cls]
        annotations = typing.get_type_hints(cls)
        opts = definition_registry[cls]

        fields = definition.fields
        children = definition.children

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
            elif origin is list and args[0] in definition_registry:
                # definition.id
                target = entities[args[0]]
                default_column_names = _default_parental_key_name(definition.entity_type.__name__, definition.primary)
                children[name] = EntityChildDefinition(
                    name=name,
                    kind="plural",
                    target_entity=target.entity_type,
                    columns=default_column_names,
                )
            # registered entity
            elif type_hint in definition_registry:
                target = entities[args[0]]
                default_column_names = _default_parental_key_name(definition.entity_type.__name__, definition.primary)
                children[name] = EntityChildDefinition(
                    name=name,
                    kind="singular",
                    target_entity=target.entity_type,
                    columns=default_column_names,
                )
            # optional of registered entity
            elif (
                origin is types.UnionType
                and len(args) == 2
                and args[1] is type(None)
                and args[0] in definition_registry
            ):
                target = entities[args[0]]
                default_column_names = _default_parental_key_name(definition.entity_type.__name__, definition.primary)
                children[name] = EntityChildDefinition(
                    name=name,
                    kind="singular",
                    target_entity=target.entity_type,
                    columns=default_column_names,
                )
            else:
                fields[name] = EntityFieldDefinition(
                    name=name,
                    column=_default_column_name(name),
                    is_primary=name in definition.primary,
                )

    return entities


def _default_parental_key_name(s: str, field_names: typing.Sequence[str]) -> list[str]:
    return [_ensure_snake_case(s) + "_" + col for col in field_names]


def _default_column_name(s: str) -> str:
    return _ensure_snake_case(s)


class Introspection(typing.NamedTuple):
    schemas: dict[str, "Schema"]

    class Schema(typing.NamedTuple):
        oid: int
        name: str
        tables: dict[str, "Introspection.Table"]

    class Table(typing.NamedTuple):
        oid: int
        schema: str
        name: str
        columns: dict[str, "Introspection.Column"]

    class Column(typing.NamedTuple):
        name: str
        default: str
        data_type: str
        is_updatable: bool
        is_primary: bool


async def _get_introspection(dsn: str, entities: typing.Iterable[EntityDefinition]):
    table_names = [(opts.schema, opts.table) for opts in entities]

    conn = await asyncpg.connect(dsn)

    schemas = await conn.fetch(
        """
        SELECT
            n.oid,
            n.nspname
                AS name
        FROM pg_namespace n
        WHERE n.nspname = ANY($1);
        """,
        list({name for name, _ in table_names}),
    )

    introspection = Introspection(schemas={})
    for schema in schemas:
        introspection.schemas[schema["name"]] = Introspection.Schema(
            oid=schema["oid"],
            name=schema["name"],
            tables={},
        )

    tables = await conn.fetch(
        """
        SELECT
            c.oid,
            nc.nspname
                AS schema,
            c.relname
                AS name
        FROM pg_class c
            JOIN pg_namespace nc ON nc.oid = c.relnamespace
            JOIN UNNEST($1::TEXT[], $2::TEXT[]) k(s, t) ON nc.nspname = k.s AND c.relname = k.t;
        """,
        [schema for schema, _ in table_names],
        [table for _, table in table_names],
    )

    for table in tables:
        schema = introspection.schemas[table["schema"]]
        schema.tables[table["name"]] = Introspection.Table(
            oid=table["oid"],
            schema=table["schema"],
            name=table["name"],
            columns={},
        )

    columns = await conn.fetch(
        """
        SELECT 
            nc.nspname 
                AS "schema",
            c.relname
                AS "table",
            c.oid
                AS "table_oid",
            a.attname
                AS "name",
            COALESCE( CASE WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid) ELSE null END, '')
                AS "column_default",
            a.attnotnull OR t.typtype = 'd' AND t.typnotnull
                AS "not_null",
            CASE
                WHEN t.typelem <> 0 THEN (SELECT eltype.typname FROM pg_type eltype WHERE eltype.oid = t.typelem) 
                ELSE t.typname
            END 
            ||
            CASE WHEN t.typelem <> 0 THEN '[]' ELSE '' END
                AS "data_type",
            pg_column_is_updatable(c.oid::regclass, a.attnum, false)
                AS "is_updatable",
            COALESCE(i.indisprimary, FALSE)
                AS "is_primary"
        FROM pg_attribute a
            LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
            LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary
            JOIN (pg_class c JOIN pg_namespace nc ON c.relnamespace = nc.oid) ON a.attrelid = c.oid
            JOIN (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid) ON a.atttypid = t.oid
            JOIN UNNEST($1::TEXT[], $2::TEXT[]) k(s, t) ON nc.nspname = k.s AND c.relname = k.t
        WHERE a.attnum > 0
        ORDER BY a.attnum;
        """,
        [schema for schema, _ in table_names],
        [table for _, table in table_names],
    )

    for column in columns:
        schema = introspection.schemas[column["schema"]]
        table = schema.tables[column["table"]]
        table.columns[column["name"]] = Introspection.Column(
            name=column["name"],
            default=column["column_default"],
            data_type=column["data_type"],
            is_updatable=column["is_updatable"],
            is_primary=column["is_primary"],
        )

    return introspection


def _render_database_spec(introspection: Introspection, output):
    for schema in introspection.schemas.values():
        filename = os.path.join(output, schema.name + ".py")

        with open(filename, "w") as f:
            f.write("# GENERATED CODE, DO NOT MODIFY\n")
            f.write(f"from {_mod} import Table, Column\n\n\n")

            for table in schema.tables.values():
                f.write(f"class _{table.name}(Table):\n")
                f.write(f"    def __init__(self):\n")
                f.write(f"        super().__init__('{table.schema}', '{table.name}')\n")
                for column in table.columns.values():
                    field_args = [f'"{column.name}"', f'"{column.data_type}"']
                    if column.default:
                        field_args.append(f'default="{column.default}"')
                    if not column.is_updatable:
                        field_args.append(f"updatable=False")
                    if column.is_primary:
                        field_args.append(f"primary=True")

                    f.write(f'        self.{column.name} = Column({", ".join(field_args)})\n')
                f.write("\n")
            f.write("\n")

            for table in schema.tables.values():
                f.write(f"{table.name} = _{table.name}()\n")

            f.write("\n")
            f.write("__all__ = [\n")
            for table in schema.tables.values():
                f.write(f"    '{table.name}',\n")
            f.write("]\n")


class EntityMappingRenderer:
    def __init__(self, entities: dict[type, EntityDefinition]):
        self.entities = entities

    def render(self, output):
        filename = os.path.join(output, f"__init__.py")
        with open(filename, "w") as f:
            for line in self._render_entity_mappings():
                f.write(line + "\n")

    def _render_entity_mappings(self):
        lines = list[str]()

        schemas = {entity_def.schema: True for entity_def in self.entities.values()}

        lines.append("# GENERATED CODE, DO NOT MODIFY")
        lines.append(f"import {_mod}")
        lines.append(f"")
        if schemas:
            lines.append(f"from . import {', '.join(schema for schema in schemas)}")
            lines.append(f"")

        modules = set()
        for entity_def in self.entities.values():
            modules.add(entity_def.entity_type.__module__)

        for module in modules:
            lines.append(f"import {module}")

        lines.append("\n")

        for entity_def in self.entities.values():
            lines.append(f"{_mod}.register_mapping(")
            for line in self._render_entity_mapping(entity_def):
                lines.append(f"    {line}")
            lines.append(")")

        return lines

    def _render_entity_mapping(self, entity_def: EntityDefinition):
        lines = [
            f"{_mod}.EntityMapping(",
            f"    entity={entity_def.entity_type.__module__}.{entity_def.entity_type.__name__},",
            f"    table={entity_def.schema}.{entity_def.table},",
        ]

        if entity_def.fields:
            lines.append(f"    fields=[")
            for field in entity_def.fields.values():
                lines.append(f"        {self._render_field(entity_def, field)},")
            lines.append(f"    ],")

        if entity_def.children:
            lines.append(f"    children=[")
            for child in entity_def.children.values():
                lines.append(f"        {self._render_child(child)},")
            lines.append(f"    ],")

        lines.append(f")")

        return lines

    def _render_field(self, entity: EntityDefinition, field: EntityFieldDefinition):
        if field.is_primary:
            return (
                f"{_mod}.Field('{field.name}', "
                f"{self._render_column_references(entity.schema, entity.table, field.column)}, "
                f"is_primary={field.is_primary})"
            )
        else:
            return (
                f"{_mod}.Field('{field.name}', "
                f"{self._render_column_references(entity.schema, entity.table, field.column)})"
            )

    def _render_child(self, child: EntityChildDefinition):
        if child.kind == "plural":
            return self._render_plural_child(child)
        elif child.kind == "singular":
            return self._render_singular_child(child)
        raise Exception("unreachable")

    def _render_plural_child(self, child: EntityChildDefinition):
        target_entity = self.entities[child.target_entity]
        return (
            f"{_mod}.Plural('{child.name}', "
            f"lambda: {_mod}.lookup_mapping({child.target_entity.__module__}.{child.target_entity.__name__}), "
            f"{self._render_column_references(target_entity.schema, target_entity.table, *child.columns)})"
        )

    def _render_singular_child(self, child: EntityChildDefinition):
        target_entity = self.entities[child.target_entity]
        return (
            f"{_mod}.Singular('{child.name}', "
            f"lambda: {_mod}.lookup_mapping({child.target_entity.__module__}.{child.target_entity.__name__}), "
            f"{self._render_column_references(target_entity.schema, target_entity.table, *child.columns)})"
        )

    def _render_column_references(self, schema: str, table: str, *columns: str):
        return ", ".join(schema + "." + table + "." + column for column in columns)


_snake_patt = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _ensure_snake_case(s: str) -> str:
    return _snake_patt.sub("_", s).lower()


if __name__ == "__main__":
    asyncio.run(main())
