from datetime import datetime
from decimal import Decimal
from uuid import UUID

from . import base
from .entities.blog_post import BlogPost


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
        await session.batch_save(
            BlogPost,
            self.blog_post1,
            self.blog_post2,
            self.blog_post3,
            self.blog_post4,
        )

    async def test_pagination_forward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(first=2)

        self.assertEqual(page.ids, [self.blog_post1.id, self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.ids[1])

        self.assertEqual(page.ids, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.ids[0])

        self.assertEqual(page.ids, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_pagination_forward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(first=2)

        self.assertEqual(page.ids, [self.blog_post2.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.ids[1])

        self.assertEqual(page.ids, [self.blog_post3.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.ids[0])

        self.assertEqual(page.ids, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_pagination_backward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(last=2)

        self.assertEqual(page.ids, [self.blog_post3.id, self.blog_post4.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.ids[0])

        self.assertEqual(page.ids, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.ids[0])

        self.assertEqual(page.ids, [self.blog_post1.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

    async def test_pagination_backward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(last=4)
        self.assertEqual(
            page.ids,
            [
                self.blog_post2.id,
                self.blog_post4.id,
                self.blog_post3.id,
                self.blog_post1.id,
            ],
        )

        page = await q.paginate(last=2)
        self.assertEqual(page.ids, [self.blog_post3.id, self.blog_post1.id])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.ids[0])
        self.assertEqual(page.ids, [self.blog_post4.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.ids[0])
        self.assertEqual(page.ids, [self.blog_post2.id])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)
