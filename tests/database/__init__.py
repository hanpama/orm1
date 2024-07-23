# GENERATED CODE, DO NOT MODIFY
import orm1

import tests.entities.purchase
import tests.entities.course


orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.Course,
        schema="public",
        table="course",
        fields=[
            orm1.Field("semester_id", "semester_id", is_primary=True),
            orm1.Field("subject_id", "subject_id", is_primary=True),
            orm1.Field('created_at', "created_at"),
        ],
        children=[
            orm1.Plural("modules", lambda: orm1.lookup_mapping(tests.entities.course.CourseModule), "course_semester_id", "course_subject_id"),
            orm1.Plural("attachments", lambda: orm1.lookup_mapping(tests.entities.course.CourseAttachment), "course_semester_id", "course_subject_id"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseAttachment,
        schema="public",
        table="course_attachment",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('media_uri', "media_uri"),
            orm1.Field('created_at', "created_at"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseModule,
        schema="public",
        table="course_module",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('title', "title"),
            orm1.Field('created_at', "created_at"),
        ],
        children=[
            orm1.Plural("materials", lambda: orm1.lookup_mapping(tests.entities.course.CourseModuleMaterial), "course_module_id"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseModuleMaterial,
        schema="public",
        table="course_module_material",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('media_uri', "media_uri"),
            orm1.Field('created_at', "created_at"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.Purchase,
        schema="public",
        table="purchase",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('code', "code"),
            orm1.Field('user_id', "user_id"),
        ],
        children=[
            orm1.Plural("line_items", lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseLineItem), "purchase_id"),
            orm1.Plural("bank_transfers", lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseBankTransfer), "purchase_id"),
            orm1.Singular("coupon_usage", lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseCouponUsage), "purchase_id"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseLineItem,
        schema="public",
        table="purchase_line_item",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('product_id', "product_id"),
            orm1.Field('quantity', "quantity"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseBankTransfer,
        schema="public",
        table="purchase_bank_transfer",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('sender_name', "sender_name"),
            orm1.Field('transfer_time', "transfer_time"),
            orm1.Field('amount', "amount"),
        ],
        children=[
            orm1.Plural("attachments", lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseBankTransferAttachment), "purchase_bank_transfer_id"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseBankTransferAttachment,
        schema="public",
        table="purchase_bank_transfer_attachment",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('media_uri', "media_uri"),
            orm1.Field('created_at', "created_at"),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseCouponUsage,
        schema="public",
        table="purchase_coupon_usage",
        fields=[
            orm1.Field("id", "id", is_primary=True),
            orm1.Field('coupon_id', "coupon_id"),
        ],
    )
)
