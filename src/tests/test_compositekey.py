from datetime import datetime

from . import base
from .entities.course import Course, CourseAttachment, CourseModule, CourseModuleMaterial


class CompositeTest(base.AutoRollbackTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        session = self.session()
        await session.save(self.course1)

    course1: Course = Course(
        semester_id="2101",
        subject_id="CS3011",
        created_at=datetime.now(),
        modules=[
            CourseModule(
                id="CS3011210101",
                title="1. Introduction",
                created_at=datetime.now(),
                materials=[
                    CourseModuleMaterial(
                        id="CS3011210101A",
                        media_uri="https://example.com/attachment1",
                        created_at=datetime.now(),
                    ),
                    CourseModuleMaterial(
                        id="CS3011210101B",
                        media_uri="https://example.com/attachment2",
                        created_at=datetime.now(),
                    ),
                ],
            ),
            CourseModule(
                id="CS3011210102",
                title="2. Basics",
                created_at=datetime.now(),
                materials=[
                    CourseModuleMaterial(
                        id="CS3011210102A",
                        media_uri="https://example.com/attachment3",
                        created_at=datetime.now(),
                    ),
                    CourseModuleMaterial(
                        id="CS3011210102B",
                        media_uri="https://example.com/attachment4",
                        created_at=datetime.now(),
                    ),
                ],
            ),
        ],
        attachments=[
            CourseAttachment(
                id="CS30112101-1",
                media_uri="https://example.com/attachment5",
                created_at=datetime.now(),
            ),
        ],
    )

    async def test_update_composite_root_scalar(self):
        session = self.session()
        course = await session.get(Course, (self.course1.semester_id, self.course1.subject_id))
        assert course
        course.created_at = datetime.now()
        await session.save(self.course1)
