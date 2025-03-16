import unittest
import typing
from orm1 import (
    AutoMappingBuilder,
    EntityMapping,
    Field,
    Child,
    FieldAttributeAccessor,
    PluralChildAttributeAccessor,
    SingularChildAttributeAccessor,
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
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "title": Field(name="title", column="title", accessor=FieldAttributeAccessor("title")),
                "content": Field(name="content", column="content", accessor=FieldAttributeAccessor("content")),
            },
            children={},
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "title", "content"],
            updatable=["title", "content"],
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
                "user_id": Field(name="user_id", column="user_id", accessor=FieldAttributeAccessor("user_id")),
                "post_id": Field(name="post_id", column="post_id", accessor=FieldAttributeAccessor("post_id")),
                "note": Field(name="note", column="note", accessor=FieldAttributeAccessor("note")),
            },
            children={},
            primary_key=["user_id", "post_id"],
            parental_key=[],
            insertable=["user_id", "post_id", "note"],
            updatable=["note"],
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
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "title": Field(name="title", column="title", accessor=FieldAttributeAccessor("title")),
                "subtitle": Field(name="subtitle", column="subtitle", accessor=FieldAttributeAccessor("subtitle")),
            },
            children={
                "comments": Child(target=ArticleComment, accessor=PluralChildAttributeAccessor("comments")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "title", "subtitle"],
            updatable=["title", "subtitle"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == ArticleComment)
        expected = EntityMapping(
            entity_type=ArticleComment,
            entity_factory=DefaultEntityFactory(ArticleComment),
            schema="public",
            table="article_comment",
            fields={
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "message": Field(name="message", column="message", accessor=FieldAttributeAccessor("message")),
                "article_id": Field(name="article_id", column="article_id", accessor=FieldAttributeAccessor("article_id")),
            },
            children={},
            primary_key=["id"],
            parental_key=["article_id"],
            insertable=["id", "message", "article_id"],
            updatable=["message"],
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
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "amount": Field(name="amount", column="amount", accessor=FieldAttributeAccessor("amount")),
            },
            children={
                "refund": Child(target=PaymentRefund, accessor=SingularChildAttributeAccessor("refund")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "amount"],
            updatable=["amount"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == PaymentRefund)
        expected = EntityMapping(
            entity_type=PaymentRefund,
            entity_factory=DefaultEntityFactory(PaymentRefund),
            schema="public",
            table="payment_refund",
            fields={
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "payment_id": Field(name="payment_id", column="payment_id", accessor=FieldAttributeAccessor("payment_id")),
                "amount": Field(name="amount", column="amount", accessor=FieldAttributeAccessor("amount")),
            },
            children={},
            primary_key=["id"],
            parental_key=["payment_id"],
            insertable=["id", "payment_id", "amount"],
            updatable=["amount"],
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
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "name": Field(name="name", column="name", accessor=FieldAttributeAccessor("name")),
            },
            children={
                "profile": Child(target=UserProfile, accessor=SingularChildAttributeAccessor("profile")),
            },
            primary_key=["id"],
            parental_key=[],
            insertable=["id", "name"],
            updatable=["name"],
        )
        assert got == expected

        got = next(m for m in mappings if m.entity_type == UserProfile)
        expected = EntityMapping(
            entity_type=UserProfile,
            entity_factory=DefaultEntityFactory(UserProfile),
            schema="public",
            table="user_profile",
            fields={
                "id": Field(name="id", column="id", accessor=FieldAttributeAccessor("id")),
                "user_id": Field(name="user_id", column="user_id", accessor=FieldAttributeAccessor("user_id")),
                "bio": Field(name="bio", column="bio", accessor=FieldAttributeAccessor("bio")),
            },
            children={},
            primary_key=["id"],
            parental_key=["user_id"],
            insertable=["id", "user_id", "bio"],
            updatable=["bio"],
        )
        assert got == expected
