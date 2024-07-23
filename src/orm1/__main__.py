import argparse
import asyncio
import glob
import os
import re
import types
import typing

from . import definition_registry

_mod = __package__


async def main():
    """
    Code generator for entity mappings.

    Example usage:

        ```shell
        python -m orm1 -e 'tests/entities/*.py' -o tests/database
        ```
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--entities", required=True)
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    for filename in glob.glob(args.entities):
        if filename.endswith(".py"):
            mod_name = filename.replace(os.path.sep, ".").removesuffix(".py")
            __import__(mod_name)

    entities = _build_definitions()
    _render_entity_mapping_file(args.output, entities.values())


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


def _render_entity_mapping_file(output, entities: typing.Collection[EntityDefinition]):
    filename = os.path.join(output, f"__init__.py")
    with open(filename, "w") as f:
        for line in _render_entity_mappings(entities):
            f.write(line + "\n")


def _render_entity_mappings(entities: typing.Collection[EntityDefinition]):
    lines = list[str]()

    lines.append("# GENERATED CODE, DO NOT MODIFY")
    lines.append(f"import {_mod}")
    lines.append(f"")

    modules = set()
    for entity_def in entities:
        modules.add(entity_def.entity_type.__module__)

    for module in modules:
        lines.append(f"import {module}")

    lines.append("\n")

    for entity_def in entities:
        lines.append(f"{_mod}.register_mapping(")
        for line in _render_entity_mapping(entity_def):
            lines.append(f"    {line}")
        lines.append(")")

    return lines


def _render_entity_mapping(entity_def: EntityDefinition):
    lines = [
        f"{_mod}.EntityMapping.define(",
        f"    entity={entity_def.entity_type.__module__}.{entity_def.entity_type.__name__},",
        f'    schema="{entity_def.schema}",',
        f'    table="{entity_def.table}",',
    ]

    if entity_def.fields:
        lines.append(f"    fields=[")
        for field in entity_def.fields.values():
            lines.append(f"        {_render_field(field)},")
        lines.append(f"    ],")

    if entity_def.children:
        lines.append(f"    children=[")
        for child in entity_def.children.values():
            lines.append(f"        {_render_child(child)},")
        lines.append(f"    ],")

    lines.append(f")")

    return lines


def _render_field(field: EntityFieldDefinition):
    if field.is_primary:
        return (
            f'{_mod}.Field("{field.name}", '
            f"{_render_column_references(field.column)}, "
            f"is_primary={field.is_primary})"
        )
    else:
        return f"{_mod}.Field('{field.name}', " f"{_render_column_references(field.column)})"


def _render_child(child: EntityChildDefinition):
    if child.kind == "plural":
        return _render_plural_child(child)
    elif child.kind == "singular":
        return _render_singular_child(child)
    raise Exception("unreachable")


def _render_plural_child(child: EntityChildDefinition):
    return (
        f'{_mod}.Plural("{child.name}", '
        f"lambda: {_mod}.lookup_mapping({child.target_entity.__module__}.{child.target_entity.__name__}), "
        f"{_render_column_references(*child.columns)})"
    )


def _render_singular_child(child: EntityChildDefinition):
    return (
        f'{_mod}.Singular("{child.name}", '
        f"lambda: {_mod}.lookup_mapping({child.target_entity.__module__}.{child.target_entity.__name__}), "
        f"{_render_column_references(*child.columns)})"
    )


def _render_column_references(*columns: str):
    return ", ".join(f'"{c}"' for c in columns)


_snake_patt = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _ensure_snake_case(s: str) -> str:
    return _snake_patt.sub("_", s).lower()


if __name__ == "__main__":
    asyncio.run(main())
