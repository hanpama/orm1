import unittest
import typing
from orm1 import AutoMappingBuilder

test_entity_and_fields = AutoMappingBuilder()


@test_entity_and_fields.mapped()
class BlogPost:
    id: int
    title: str
    content: str
    _private: str = "private"


auto_test_composite_primary_key = AutoMappingBuilder()


@auto_test_composite_primary_key.mapped(primary_key=["user_id", "post_id"])
class UserPostMeta:
    user_id: int
    post_id: int
    note: str


auto_test_list_ref_to_plural = AutoMappingBuilder()


@auto_test_list_ref_to_plural.mapped()
class Article:
    id: int
    title: str
    subtitle: str
    comments: "list[ArticleComment]"


@auto_test_list_ref_to_plural.mapped(parental_key=["article_id"])
class ArticleComment:
    id: int
    message: str
    article_id: int


auto_test_ref_to_singular = AutoMappingBuilder()


@auto_test_ref_to_singular.mapped()
class Payment:
    id: int
    amount: float
    refund: "PaymentRefund"


@auto_test_ref_to_singular.mapped(parental_key=["payment_id"])
class PaymentRefund:
    id: int
    payment_id: int
    amount: float


auto_test_optional_to_singluar = AutoMappingBuilder()


@auto_test_optional_to_singluar.mapped()
class User:
    id: int
    name: str
    profile: "typing.Optional[UserProfile]"


@auto_test_optional_to_singluar.mapped(parental_key=["user_id"])
class UserProfile:
    id: int
    user_id: int
    bio: str


class AutomapperTest(unittest.TestCase):
    def test_entity_and_fields(self):
        mapping = test_entity_and_fields.build()[0]
        assert mapping.entity_type == BlogPost
        assert mapping.table == "blog_post"
        assert mapping.schema == "public"

        assert len(mapping.fields) == 3
        assert mapping.fields["id"].column == "id"
        assert mapping.fields["id"].insertable is True
        assert mapping.fields["id"].updatable is False
        assert mapping.fields["title"].column == "title"
        assert mapping.fields["title"].insertable is True
        assert mapping.fields["title"].updatable is True
        assert mapping.fields["content"].column == "content"
        assert mapping.fields["content"].insertable is True
        assert mapping.fields["content"].updatable is True

        assert len(mapping.children) == 0

        assert mapping.primary_key_fields == ["id"]
        assert mapping.parental_key_fields == []

    def test_composite_primary_key(self):
        mappings = auto_test_composite_primary_key.build()
        assert len(mappings) == 1

        mapping = mappings[0]
        assert mapping.entity_type == UserPostMeta
        assert mapping.primary_key_fields == ["user_id", "post_id"]
        assert mapping.parental_key_fields == []

        assert len(mapping.fields) == 3
        assert mapping.fields["user_id"].insertable is True
        assert mapping.fields["user_id"].updatable is False
        assert mapping.fields["post_id"].insertable is True
        assert mapping.fields["post_id"].updatable is False
        assert mapping.fields["note"].insertable is True
        assert mapping.fields["note"].updatable is True

    def test_list_ref_to_plural(self):
        mappings = auto_test_list_ref_to_plural.build()
        assert len(mappings) == 2

        article_mapping = next(m for m in mappings if m.entity_type == Article)
        assert article_mapping.table == "article"
        assert len(article_mapping.fields) == 3
        assert article_mapping.primary_key_fields == ["id"]
        assert len(article_mapping.children) == 1
        assert "comments" in article_mapping.children
        assert article_mapping.children["comments"].target == ArticleComment

        comment_mapping = next(m for m in mappings if m.entity_type == ArticleComment)
        assert comment_mapping.table == "article_comment"
        assert len(comment_mapping.fields) == 3
        assert comment_mapping.primary_key_fields == ["id"]
        assert comment_mapping.parental_key_fields == ["article_id"]
        assert len(comment_mapping.children) == 0

    def test_ref_to_singular(self):
        mappings = auto_test_ref_to_singular.build()
        assert len(mappings) == 2

        payment_mapping = next(m for m in mappings if m.entity_type == Payment)
        assert payment_mapping.table == "payment"
        assert len(payment_mapping.fields) == 2
        assert payment_mapping.primary_key_fields == ["id"]
        assert len(payment_mapping.children) == 1
        assert "refund" in payment_mapping.children
        assert payment_mapping.children["refund"].target == PaymentRefund

        refund_mapping = next(m for m in mappings if m.entity_type == PaymentRefund)
        assert refund_mapping.table == "payment_refund"
        assert len(refund_mapping.fields) == 3
        assert refund_mapping.primary_key_fields == ["id"]
        assert refund_mapping.parental_key_fields == ["payment_id"]
        assert len(refund_mapping.children) == 0

    def test_optional_to_singular(self):
        mappings = auto_test_optional_to_singluar.build()
        assert len(mappings) == 2

        user_mapping = next(m for m in mappings if m.entity_type == User)
        assert user_mapping.table == "user"
        assert len(user_mapping.fields) == 2
        assert user_mapping.primary_key_fields == ["id"]
        assert len(user_mapping.children) == 1
        assert "profile" in user_mapping.children
        assert user_mapping.children["profile"].target == UserProfile

        profile_mapping = next(m for m in mappings if m.entity_type == UserProfile)
        assert profile_mapping.table == "user_profile"
        assert len(profile_mapping.fields) == 3
        assert profile_mapping.primary_key_fields == ["id"]
        assert profile_mapping.parental_key_fields == ["user_id"]
        assert len(profile_mapping.children) == 0
