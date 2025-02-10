from dataclasses import dataclass
from datetime import datetime

from orm1 import auto

from . import base


schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_pagination_composite;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

        CREATE TABLE test_pagination_composite.blog_post (
            key1 INT NOT NULL,
            key2 INT NOT NULL,
            title TEXT NOT NULL,
            rating INT,
            published_at TIMESTAMPTZ,
            PRIMARY KEY (key1, key2)
        );
    END $$;
"""


@auto.mapped(
    schema="test_pagination_composite",
    primary_key=("key1", "key2"),
)
@dataclass(eq=False)
class BlogPost:
    key1: int
    key2: int
    title: str
    rating: int | None
    published_at: datetime | None


class PaginationTest(base.AutoRollbackTestCase):
    blog_post1 = BlogPost(
        key1=1,
        key2=2,
        title="First blog post",
        rating=3,
        published_at=datetime(2021, 1, 1, 12, 0, 0),
    )
    blog_post2 = BlogPost(
        key1=3,
        key2=4,
        title="Second blog post",
        rating=None,
        published_at=datetime(2021, 1, 2, 13, 0, 0),
    )
    blog_post3 = BlogPost(
        key1=5,
        key2=6,
        title="Third blog post",
        rating=4,
        published_at=None,
    )
    blog_post4 = BlogPost(
        key1=7,
        key2=8,
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

    async def test_pagination_forward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(first=2)

        self.assertEqual(
            page.cursors,
            [
                (self.blog_post1.key1, self.blog_post1.key2),
                (self.blog_post2.key1, self.blog_post2.key2),
            ],
        )
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [(self.blog_post3.key1, self.blog_post3.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [(self.blog_post4.key1, self.blog_post4.key2)])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_pagination_forward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(first=2)

        self.assertEqual(
            page.cursors,
            [
                (self.blog_post2.key1, self.blog_post2.key2),
                (self.blog_post4.key1, self.blog_post4.key2),
            ],
        )
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

        page = await q.paginate(first=1, after=page.cursors[1])

        self.assertEqual(page.cursors, [(self.blog_post3.key1, self.blog_post3.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(first=1, after=page.cursors[0])

        self.assertEqual(page.cursors, [(self.blog_post1.key1, self.blog_post1.key2)])
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

    async def test_pagination_backward_nulls_last(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.asc("bp.published_at"))

        page = await q.paginate(last=2)

        self.assertEqual(
            page.cursors,
            [
                (self.blog_post3.key1, self.blog_post3.key2),
                (self.blog_post4.key1, self.blog_post4.key2),
            ],
        )
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [(self.blog_post2.key1, self.blog_post2.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])

        self.assertEqual(page.cursors, [(self.blog_post1.key1, self.blog_post1.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)

    async def test_pagination_backward_nulls_first(self) -> None:
        session = self.session()

        q = session.query(BlogPost, "bp")
        q.order_by(q.desc("bp.rating", nulls_last=False))

        page = await q.paginate(last=2)
        self.assertEqual(
            page.cursors,
            [
                (self.blog_post3.key1, self.blog_post3.key2),
                (self.blog_post1.key1, self.blog_post1.key2),
            ],
        )
        self.assertEqual(page.has_next_page, False)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])
        self.assertEqual(page.cursors, [(self.blog_post4.key1, self.blog_post4.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, True)

        page = await q.paginate(last=1, before=page.cursors[0])
        self.assertEqual(page.cursors, [(self.blog_post2.key1, self.blog_post2.key2)])
        self.assertEqual(page.has_next_page, True)
        self.assertEqual(page.has_previous_page, False)
