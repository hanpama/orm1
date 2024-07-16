from datetime import datetime
from unittest import IsolatedAsyncioTestCase

import asyncpg

from orm1 import Session, SessionBackend

from . import database as _
from .entities.course import Course, CourseAttachment, CourseModule, CourseModuleMaterial


class CompositeTest(IsolatedAsyncioTestCase):

    course1: Course = Course(
        semester_id="2021-01",
        subject_id="CS-3011",
        created_at=datetime.now(),
        modules=[
            CourseModule(
                id="f4578462-26c4-4f2b-8ce1-fb1810d325b5",
                title="1. Introduction",
                created_at=datetime.now(),
                materials=[
                    CourseModuleMaterial(
                        id="84fae7a2-e041-4091-88d7-c9114ac2d8ec",
                        media_uri="https://example.com/attachment1",
                        created_at=datetime.now(),
                    ),
                    CourseModuleMaterial(
                        id="67f66562-5a0e-4ff3-8895-05bfd8159d0b",
                        media_uri="https://example.com/attachment2",
                        created_at=datetime.now(),
                    ),
                ],
            ),
            CourseModule(
                id="72c8a184-0215-43ad-9999-c2c82856decf",
                title="2. Basics",
                created_at=datetime.now(),
                materials=[
                    CourseModuleMaterial(
                        id="5aba3b4f-fc79-46b1-91af-195ae66cc30b",
                        media_uri="https://example.com/attachment3",
                        created_at=datetime.now(),
                    ),
                    CourseModuleMaterial(
                        id="0efbd447-be20-4bc5-a6a3-9b9748143b26",
                        media_uri="https://example.com/attachment4",
                        created_at=datetime.now(),
                    ),
                ],
            ),
        ],
        attachments=[
            CourseAttachment(
                id="fd5400f0-f490-45a9-8629-91bb66a474c3",
                media_uri="https://example.com/attachment5",
                created_at=datetime.now(),
            ),
        ],
    )

    async def test_update_composite_root_scalar(self):
        session = self._session()
        course = await session.get(Course, (self.course1.semester_id, self.course1.subject_id))
        assert course
        course.created_at = datetime.now()
        await session.save(self.course1)

    async def asyncSetUp(self) -> None:
        self._conn: asyncpg.Connection = await asyncpg.connect(self.dsn)
        self._backend = SessionBackend(self._conn)
        self._tx = self._conn.transaction()

        session = Session(self._backend)

        await self._tx.start()
        await session.save(self.course1)

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await self._tx.rollback()
        await self._conn.close()
        return await super().asyncTearDown()

    def _session(self):
        return Session(self._backend)

    dsn = "postgresql://postgres:8800bc84f23af727f4e9@localhost:3200/postgres"
