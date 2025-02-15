import unittest

import asyncpg

from orm1 import AsyncPGSessionBackend, Session, auto

from .base import get_database_uri


schema_set_up = """
    DO $$ BEGIN
        CREATE SCHEMA test_transaction;
        CREATE TABLE test_transaction.blog_post (
            id INT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL
        );
    END $$;
"""

schema_tear_down = """
    DO $$ BEGIN
        DROP SCHEMA test_transaction CASCADE;
    END $$;
"""


@auto.mapped(schema="test_transaction")
class BlogPost:
    id: int
    title: str
    content: str


class TransactionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        pool = await asyncpg.pool.create_pool(
            get_database_uri(),
            min_size=1,
            max_size=2,
        )
        assert pool

        self._pool = pool
        self._backend = AsyncPGSessionBackend(self._pool)

        await self.session().raw(schema_set_up).fetch()
        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await self.session().raw(schema_tear_down).fetch()
        await self._pool.close()
        return await super().asyncTearDown()

    def session(self):
        return Session(self._backend, auto.build())

    async def test_commit(self) -> None:
        session = self.session()

        async with session.transaction():
            await session.raw(
                "INSERT INTO test_transaction.blog_post (id, title, content) VALUES (:id, :title, :content)",
                id=1,
                title="First post",
                content="Content C",
            ).fetch()

        got = await session.get(BlogPost, 1)
        assert got
        assert got.id == 1
        assert got.title == "First post"
        assert got.content == "Content C"

        await session.delete(got)

    async def test_rollback(self) -> None:
        session = self.session()
        try:
            async with session.transaction():
                await session.raw(
                    "INSERT INTO test_transaction.blog_post (id, title, content) VALUES (:id, :title, :content)",
                    id=1,
                    title="First post",
                    content="Content C",
                ).fetch()

                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        got = await session.get(BlogPost, 1)
        assert not got
