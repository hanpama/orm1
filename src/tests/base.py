import os
import unittest

import asyncpg

from orm1 import AsyncPGSessionBackend, Session, auto


class AutoRollbackTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        pool = await asyncpg.pool.create_pool(
            get_database_uri(),
            min_size=1,
            max_size=2,
        )
        assert pool

        self._pool = pool
        self._backend = AsyncPGSessionBackend(self._pool)

        await self._backend.begin()

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await self._backend.rollback()
        await self._pool.close()
        return await super().asyncTearDown()

    def session(self):
        return Session(self._backend, auto.build())


def get_database_uri():
    return os.getenv("DATABASE_URI", "postgresql://postgres:8800bc84f23af727f4e9@localhost:3200/postgres")
