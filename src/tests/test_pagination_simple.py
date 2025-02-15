from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from orm1 import auto

from . import base


schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_pagination_simple;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

        CREATE TABLE test_pagination_simple.blog_post (
            id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            rating INT,
            published_at TIMESTAMPTZ
        );
    END $$;
"""


@auto.mapped(schema="test_pagination_simple")
@dataclass(eq=False)
class BlogPost:
    id: UUID
    title: str
    rating: int | None
    published_at: datetime | None


class PaginationTest(base.AutoRollbackTestCase):
    blog_post1 = BlogPost(
        id=UUID("346505b0-ed49-45dc-9857-ab23a98b2c9f"),
        title="First blog post",
        rating=3,
        published_at=datetime(2021, 1, 1, 12, 0, 0),
    )
    blog_post2 = BlogPost(
        id=UUID("d31ae0a3-8ae6-442d-bc92-9b03cd1f434f"),
        title="Second blog post",
        rating=None,
        published_at=datetime(2021, 1, 2, 13, 0, 0),
    )
    blog_post3 = BlogPost(
        id=UUID("e2c94ff2-d0e0-419e-b5cc-1f990716ed95"),
        title="Third blog post",
        rating=4,
        published_at=None,
    )
    blog_post4 = BlogPost(
        id=UUID("fb207b31-9058-404f-b38d-9d02d68d2fd1"),
        title="Fourth blog post",
        rating=None,
        published_at=None,
    )

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        session = self.session()
        await session.raw(schema).fetch()

        await session.batch_save(
            BlogPost,
            self.blog_post1,
            self.blog_post2,
            self.blog_post3,
            self.blog_post4,
        )

    async def test_asc_forward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(first=2)

        self.assertEqual(page.cursors, [self.blog_post1.id, self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_asc_backward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(last=2)

        self.assertEqual(page.cursors, [self.blog_post3.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

    async def test_asc_forward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(first=2)

        self.assertEqual(page.cursors, [self.blog_post2.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_asc_backward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.rating", nulls_last=False))

        page = await q.paginate(last=2)
        self.assertEqual(page.cursors, [self.blog_post1.id, self.blog_post3.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])
        self.assertEqual(page.cursors, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])
        self.assertEqual(page.cursors, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

    async def test_desc_forward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.published_at"))

        page = await q.paginate(first=2)

        self.assertEqual(page.cursors, [self.blog_post2.id, self.blog_post1.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_desc_backward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.published_at"))

        page = await q.paginate(last=2)

        self.assertEqual(page.cursors, [self.blog_post3.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

    async def test_desc_forward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(first=2)

        self.assertEqual(page.cursors, [self.blog_post2.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_desc_backward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(last=2)
        self.assertEqual(page.cursors, [self.blog_post3.id, self.blog_post1.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)
