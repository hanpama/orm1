from dataclasses import dataclass
from orm1 import auto
from .base import AutoRollbackTestCase

schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_query;
        CREATE TABLE test_query.blog_post (
            id INT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL
        );
        CREATE TABLE test_query.blog_post_comment (
            id INT PRIMARY KEY,
            blog_post_id INTEGER NOT NULL REFERENCES test_query.blog_post(id),
            rating INTEGER NOT NULL,
            content TEXT NOT NULL
        );
        CREATE TABLE test_query.blog_post_tag (
            id INT PRIMARY KEY,
            blog_post_id INTEGER NOT NULL REFERENCES test_query.blog_post(id),
            tag TEXT NOT NULL
        );
    END $$;
"""


@dataclass
@auto.mapped(schema="test_query")
class BlogPost:
    id: int
    title: str
    content: str


@dataclass
@auto.mapped(schema="test_query")
class BlogPostComment:
    id: int
    blog_post_id: int
    rating: int
    content: str


@dataclass
@auto.mapped(schema="test_query")
class BlogPostTag:
    id: int
    blog_post_id: int
    tag: str


class QueryTestCase(AutoRollbackTestCase):
    blog_post_1 = BlogPost(
        id=1,
        title="First post",
        content="Content C",
    )
    blog_post_2 = BlogPost(
        id=2,
        title="Second post",
        content="Content B",
    )
    blog_post_3 = BlogPost(
        id=3,
        title="Third post",
        content="Content B",
    )

    blog_post_comment_1 = BlogPostComment(
        id=1,
        blog_post_id=1,
        rating=5,
        content="First comment",
    )
    blog_post_comment_2 = BlogPostComment(
        id=2,
        blog_post_id=1,
        rating=4,
        content="Second comment",
    )
    blog_post_comment_3 = BlogPostComment(
        id=3,
        blog_post_id=2,
        rating=3,
        content="Third comment",
    )
    blog_post_tag_1 = BlogPostTag(
        id=1,
        blog_post_id=1,
        tag="tag1",
    )
    blog_post_tag_2 = BlogPostTag(
        id=2,
        blog_post_id=1,
        tag="tag2",
    )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        session = self.session()
        await session.raw(schema).fetch()
        await session.batch_save(
            BlogPost,
            self.blog_post_1,
            self.blog_post_2,
            self.blog_post_3,
        )
        await session.batch_save(
            BlogPostComment,
            self.blog_post_comment_1,
            self.blog_post_comment_2,
            self.blog_post_comment_3,
        )
        await session.batch_save(
            BlogPostTag,
            self.blog_post_tag_1,
            self.blog_post_tag_2,
        )

    async def test_empty_query(self):
        session = self.session()
        result = await session.query(BlogPost, "bp").fetch()

        assert len(result) == 3
        assert {bp.id for bp in result} == {1, 2, 3}

    async def test_filter(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.where("LENGTH(bp.title) = 10")
        query.where("bp.id > 1")
        result = await query.fetch()

        assert len(result) == 1
        assert result[0].id == 3

    async def test_filter_fetch_one(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.where("LENGTH(bp.title) = 10")
        query.where("bp.id > 1")
        result = await query.fetch_one()

        assert result
        assert result.id == 3

    async def test_filter_fetch_one_null(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.where("FALSE")
        result = await query.fetch_one()

        assert result is None

    async def test_join(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.join(BlogPostComment, "bpc", "bp.id = bpc.blog_post_id")
        query.left_join("test_query.blog_post_tag", "bpt", "bp.id = bpt.blog_post_id")
        query.where("bpt.tag IS NULL")
        query.having("every(bpc.rating <= 3)")
        result = await query.fetch()

        assert len(result) == 1
        assert result[0].id == 2

    async def test_order_by(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.order_by(query.asc("bp.content"), query.desc("bp.id"))
        result = await query.fetch()

        assert [bp.id for bp in result] == [3, 2, 1]

    async def test_count(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.where("bp.content = 'Content B'")
        result = await query.count()

        assert result == 2

    async def test_complex_query(self):
        session = self.session()
        query = session.query(BlogPost, "bp")
        query.join(BlogPostComment, "bpc", "bp.id = bpc.blog_post_id")
        query.left_join(BlogPostTag, "bpt", "bp.id = bpt.blog_post_id")
        query.where("bpc.rating <= 3")
        query.where("bpt.tag IS NULL")
        query.order_by(query.asc("bp.content"), query.desc("bp.id"))
        result = await query.fetch()

        assert len(result) == 1
        assert result[0].id == 2

    async def test_raw_query(self):
        session = self.session()
        result = await session.raw(
            """
            SELECT bp.id, COUNT(bpc.id) AS comment_count
            FROM test_query.blog_post bp
            LEFT JOIN test_query.blog_post_comment bpc ON bp.id = bpc.blog_post_id
            GROUP BY bp.id
            HAVING COUNT(bpc.id) = :count
            """,
            count=0,
        ).fetch()

        assert len(result) == 1
        assert result[0][0] == 3

    async def test_raw_query_fetch_one(self):
        session = self.session()
        result = await session.raw(
            """
            SELECT bp.id, COUNT(bpc.id) AS comment_count
            FROM test_query.blog_post bp
            LEFT JOIN test_query.blog_post_comment bpc ON bp.id = bpc.blog_post_id
            GROUP BY bp.id
            HAVING COUNT(bpc.id) = :count
            """,
            count=0,
        ).fetch_one()

        assert result
