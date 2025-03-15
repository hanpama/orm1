import unittest
import typing
from orm1 import (
    AutoMappingBuilder,
    EntityMapping,
    Field,
    Child,
    DefaultFieldPropertyAccessor,
    DefaultPluralChildAccessor,
    DefaultSingularChildAccessor,
    DefaultEntityFactory,
)

auto = AutoMappingBuilder()


@auto.mapped()
class BlogPost:
    id: int
    title: str
    content: str
    _private: str = "private"


@auto.mapped(primary_key=["user_id", "post_id"])
class UserPostMeta:
    user_id: int
    post_id: int
    note: str


@auto.mapped()
class Article:
    id: int
    title: str
    subtitle: str
    comments: "list[ArticleComment]"


@auto.mapped(parental_key=["article_id"])
class ArticleComment:
    id: int
    message: str
    article_id: int


@auto.mapped()
class Payment:
    id: int
    amount: float
    refund: "PaymentRefund"


@auto.mapped(parental_key=["payment_id"])
class PaymentRefund:
    id: int
    payment_id: int
    amount: float


@auto.mapped()
class User:
    id: int
    name: str
    profile: "typing.Optional[UserProfile]"


@auto.mapped(parental_key=["user_id"])
class UserProfile:
    id: int
    user_id: int
    bio: str


class AutomapperTest(unittest.TestCase):
    def test_entity_and_fields(self):
        mappings = auto.build()
        got = next(m for m in mappings if m.entity_type == BlogPost)
        expected = EntityMapping(
            entity_type=BlogPost,
            entity_factory=DefaultEntityFactory(BlogPost),
            schema="public",
            table="blog_post",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "title": Field(column="title", accessor=DefaultFieldPropertyAccessor("title")),
                "content": Field(column="content", accessor=DefaultFieldPropertyAccessor("content")),
            },
            children={},
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "title", "content"],
            updatable=["title", "content"],
            key=["id"],
            full=["id", "title", "content"],
        )

        assert got == expected

    def test_composite_primary_key(self):
        mappings = auto.build()
        got = next(m for m in mappings if m.entity_type == UserPostMeta)
        expected = EntityMapping(
            entity_type=UserPostMeta,
            entity_factory=DefaultEntityFactory(UserPostMeta),
            schema="public",
            table="user_post_meta",
            fields={
                "user_id": Field(column="user_id", accessor=DefaultFieldPropertyAccessor("user_id")),
                "post_id": Field(column="post_id", accessor=DefaultFieldPropertyAccessor("post_id")),
                "note": Field(column="note", accessor=DefaultFieldPropertyAccessor("note")),
            },
            children={},
            primary_key=["user_id", "post_id"],
            parental_key=[],
            insertable=["user_id", "post_id", "note"],
            updatable=["note"],
            key=["user_id", "post_id"],
            full=["user_id", "post_id", "note"],
        )

        assert got == expected

    def test_list_ref_to_plural(self):
        mappings = auto.build()

        got = next(m for m in mappings if m.entity_type == Article)
        expected = EntityMapping(
            entity_type=Article,
            entity_factory=DefaultEntityFactory(Article),
            schema="public",
            table="article",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "title": Field(column="title", accessor=DefaultFieldPropertyAccessor("title")),
                "subtitle": Field(column="subtitle", accessor=DefaultFieldPropertyAccessor("subtitle")),
            },
            children={
                "comments": Child(target=ArticleComment, accessor=DefaultPluralChildAccessor("comments")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "title", "subtitle"],
            updatable=["title", "subtitle"],
            key=["id"],
            full=["id", "title", "subtitle"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == ArticleComment)
        expected = EntityMapping(
            entity_type=ArticleComment,
            entity_factory=DefaultEntityFactory(ArticleComment),
            schema="public",
            table="article_comment",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "message": Field(column="message", accessor=DefaultFieldPropertyAccessor("message")),
                "article_id": Field(column="article_id", accessor=DefaultFieldPropertyAccessor("article_id")),
            },
            children={},
            primary_key=["id"],
            parental_key=["article_id"],
            insertable=["id", "message", "article_id"],
            updatable=["message"],
            key=["id", "article_id"],
            full=["id", "article_id", "message"],
        )
        assert got == expected

    def test_ref_to_singular(self):
        mappings = auto.build()

        got = next(m for m in mappings if m.entity_type == Payment)
        expected = EntityMapping(
            entity_type=Payment,
            entity_factory=DefaultEntityFactory(Payment),
            schema="public",
            table="payment",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "amount": Field(column="amount", accessor=DefaultFieldPropertyAccessor("amount")),
            },
            children={
                "refund": Child(target=PaymentRefund, accessor=DefaultSingularChildAccessor("refund")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "amount"],
            updatable=["amount"],
            key=["id"],
            full=["id", "amount"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == PaymentRefund)
        expected = EntityMapping(
            entity_type=PaymentRefund,
            entity_factory=DefaultEntityFactory(PaymentRefund),
            schema="public",
            table="payment_refund",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "payment_id": Field(column="payment_id", accessor=DefaultFieldPropertyAccessor("payment_id")),
                "amount": Field(column="amount", accessor=DefaultFieldPropertyAccessor("amount")),
            },
            children={},
            primary_key=["id"],
            parental_key=["payment_id"],
            insertable=["id", "payment_id", "amount"],
            updatable=["amount"],
            key=["id", "payment_id"],
            full=["id", "payment_id", "amount"],
        )
        assert got == expected

    def test_optional_to_singular(self):
        mappings = auto.build()

        got = next(m for m in mappings if m.entity_type == User)
        expected = EntityMapping(
            entity_type=User,
            entity_factory=DefaultEntityFactory(User),
            schema="public",
            table="user",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "name": Field(column="name", accessor=DefaultFieldPropertyAccessor("name")),
            },
            children={
                "profile": Child(target=UserProfile, accessor=DefaultSingularChildAccessor("profile")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "name"],
            updatable=["name"],
            key=["id"],
            full=["id", "name"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == UserProfile)
        expected = EntityMapping(
            entity_type=UserProfile,
            entity_factory=DefaultEntityFactory(UserProfile),
            schema="public",
            table="user_profile",
            fields={
                "id": Field(column="id", accessor=DefaultFieldPropertyAccessor("id")),
                "user_id": Field(column="user_id", accessor=DefaultFieldPropertyAccessor("user_id")),
                "bio": Field(column="bio", accessor=DefaultFieldPropertyAccessor("bio")),
            },
            children={},
            primary_key=["id"],
            parental_key=["user_id"],
            insertable=["id", "user_id", "bio"],
            updatable=["bio"],
            key=["id", "user_id"],
            full=["id", "user_id", "bio"],
        )
        assert got == expected
