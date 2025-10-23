from dataclasses import dataclass
from orm1 import auto
from .base import AutoRollbackTestCase

schema = """
    DO $$ BEGIN
        -- Create schema with dots in name
        CREATE SCHEMA "test.unusual";

        -- Create table with various edge cases in identifiers
        CREATE TABLE "test.unusual"."my""table" (
            id INT PRIMARY KEY,
            "my.field" TEXT NOT NULL,
            "with space" TEXT NOT NULL,
            "with""quotes" TEXT NOT NULL,
            "SELECT" TEXT NOT NULL,
            "CamelCase" TEXT NOT NULL,
            "with$special#chars" TEXT NOT NULL,
            "한글이름" TEXT NOT NULL,
            "123numeric" TEXT NOT NULL
        );
    END $$;
"""


@dataclass
@auto.mapped(
    schema="test.unusual",
    table='my"table',
    fields={
        "my_field": {"column": "my.field"},
        "with_space": {"column": "with space"},
        "with_quotes": {"column": 'with"quotes'},
        "select_col": {"column": "SELECT"},
        "camel_case": {"column": "CamelCase"},
        "with_special": {"column": "with$special#chars"},
        "korean_name": {"column": "한글이름"},
        "numeric_start": {"column": "123numeric"}
    }
)
class Item:
    id: int
    my_field: str
    with_space: str
    with_quotes: str
    select_col: str
    camel_case: str
    with_special: str
    korean_name: str
    numeric_start: str


class UnusualIdentifiersTestCase(AutoRollbackTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        session = self.session()
        await session.raw(schema).fetch()

    async def test_unusual_identifiers(self):
        """Test that unusual identifiers are properly escaped"""
        session = self.session()

        # Test dots in identifiers
        item = Item(
            id=1,
            my_field="dot_value",
            with_space="space value",
            with_quotes='quote"value',
            select_col="reserved_word",
            camel_case="CamelValue",
            with_special="special$#value",
            korean_name="한글값",
            numeric_start="123value"
        )
        await session.save(item)

        # Retrieve it back
        retrieved = await session.get(Item, 1)

        assert retrieved is not None
        assert retrieved.id == 1
        assert retrieved.my_field == "dot_value"
        assert retrieved.with_space == "space value"
        assert retrieved.with_quotes == 'quote"value'
        assert retrieved.select_col == "reserved_word"
        assert retrieved.camel_case == "CamelValue"
        assert retrieved.with_special == "special$#value"
        assert retrieved.korean_name == "한글값"
        assert retrieved.numeric_start == "123value"

        # Update it
        retrieved.my_field = "updated_dot"
        retrieved.with_space = "updated space"
        retrieved.with_quotes = 'updated"quote'
        await session.save(retrieved)

        # Query with various column references
        results = await session.query(Item, "i").where(
            'i."my.field" = :val',
            val="updated_dot"
        ).fetch()

        assert len(results) == 1
        assert results[0].my_field == "updated_dot"
        assert results[0].with_space == "updated space"
        assert results[0].with_quotes == 'updated"quote'

        # Query with reserved word column
        results2 = await session.query(Item, "i").where(
            'i."SELECT" = :val',
            val="reserved_word"
        ).fetch()

        assert len(results2) == 1

        # Delete
        await session.delete(retrieved)
        deleted = await session.get(Item, 1)
        assert deleted is None
