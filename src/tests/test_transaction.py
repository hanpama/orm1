import unittest
from dataclasses import dataclass

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


@dataclass
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

    async def test_insert_commit_get(self) -> None:
        session = self.session()

        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        async with session.tx():
            await session.save(blog_post)

        got = await session.get(BlogPost, 1)
        assert got is not None
        assert got is blog_post
        assert got.id == 1
        assert got.title == "First post"
        assert got.content == "Content A"

        await session.save(blog_post)

    async def test_insert_commit_save(self) -> None:
        session = self.session()

        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        async with session.tx():
            await session.save(blog_post)

        await session.save(blog_post)

    async def test_insert_commit_delete(self) -> None:
        session = self.session()

        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        async with session.tx():
            await session.save(blog_post)

        await session.delete(blog_post)

    async def test_insert_rollback_get(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        try:
            async with session.tx():
                await session.save(blog_post)
                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        got = await session.get(BlogPost, 1)
        assert got is None

    async def test_insert_rollback_save(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        try:
            async with session.tx():
                await session.save(blog_post)
                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        await session.save(blog_post)

    async def test_insert_rollback_delete(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        try:
            async with session.tx():
                await session.save(blog_post)
                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        deleted = await session.delete(blog_post)
        assert not deleted

    async def test_delete_commit_get(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        await session.save(blog_post)

        async with session.tx():
            assert await session.delete(blog_post)

        got = await session.get(BlogPost, 1)
        assert got is None

    async def test_delete_commit_save(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        await session.save(blog_post)

        async with session.tx():
            await session.delete(blog_post)

        await session.save(blog_post)

    async def test_delete_rollback_get(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        await session.save(blog_post)

        try:
            async with session.tx():
                assert await session.delete(blog_post)
                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        got = await session.get(BlogPost, 1)
        assert got is not None
        assert got is blog_post

    async def test_delete_rollback_save(self) -> None:
        session = self.session()
        blog_post = BlogPost(
            id=1,
            title="First post",
            content="Content A",
        )
        await session.save(blog_post)

        try:
            async with session.tx():
                assert await session.delete(blog_post)
                raise Exception("rollback")
        except Exception as e:
            assert str(e) == "rollback"

        await session.save(blog_post)
