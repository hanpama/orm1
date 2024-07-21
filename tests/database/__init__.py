# GENERATED CODE, DO NOT MODIFY
import orm1

from . import public

import tests.entities.purchase
import tests.entities.course


orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.Course,
        table=public.course,
        fields=[
            orm1.Field('semester_id', public.course.semester_id, is_primary=True),
            orm1.Field('subject_id', public.course.subject_id, is_primary=True),
            orm1.Field('created_at', public.course.created_at),
        ],
        children=[
            orm1.Plural('modules', lambda: orm1.lookup_mapping(tests.entities.course.CourseModule), public.course_module.course_semester_id, public.course_module.course_subject_id),
            orm1.Plural('attachments', lambda: orm1.lookup_mapping(tests.entities.course.CourseAttachment), public.course_attachment.course_semester_id, public.course_attachment.course_subject_id),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseAttachment,
        table=public.course_attachment,
        fields=[
            orm1.Field('id', public.course_attachment.id, is_primary=True),
            orm1.Field('media_uri', public.course_attachment.media_uri),
            orm1.Field('created_at', public.course_attachment.created_at),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseModule,
        table=public.course_module,
        fields=[
            orm1.Field('id', public.course_module.id, is_primary=True),
            orm1.Field('title', public.course_module.title),
            orm1.Field('created_at', public.course_module.created_at),
        ],
        children=[
            orm1.Plural('materials', lambda: orm1.lookup_mapping(tests.entities.course.CourseModuleMaterial), public.course_module_material.course_module_id),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.course.CourseModuleMaterial,
        table=public.course_module_material,
        fields=[
            orm1.Field('id', public.course_module_material.id, is_primary=True),
            orm1.Field('media_uri', public.course_module_material.media_uri),
            orm1.Field('created_at', public.course_module_material.created_at),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.Purchase,
        table=public.purchase,
        fields=[
            orm1.Field('id', public.purchase.id, is_primary=True),
            orm1.Field('code', public.purchase.code),
            orm1.Field('user_id', public.purchase.user_id),
        ],
        children=[
            orm1.Plural('line_items', lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseLineItem), public.purchase_line_item.purchase_id),
            orm1.Plural('bank_transfers', lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseBankTransfer), public.purchase_bank_transfer.purchase_id),
            orm1.Singular('coupon_usage', lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseCouponUsage), public.purchase_coupon_usage.purchase_id),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseLineItem,
        table=public.purchase_line_item,
        fields=[
            orm1.Field('id', public.purchase_line_item.id, is_primary=True),
            orm1.Field('product_id', public.purchase_line_item.product_id),
            orm1.Field('quantity', public.purchase_line_item.quantity),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseBankTransfer,
        table=public.purchase_bank_transfer,
        fields=[
            orm1.Field('id', public.purchase_bank_transfer.id, is_primary=True),
            orm1.Field('sender_name', public.purchase_bank_transfer.sender_name),
            orm1.Field('transfer_time', public.purchase_bank_transfer.transfer_time),
            orm1.Field('amount', public.purchase_bank_transfer.amount),
        ],
        children=[
            orm1.Plural('attachments', lambda: orm1.lookup_mapping(tests.entities.purchase.PurchaseBankTransferAttachment), public.purchase_bank_transfer_attachment.purchase_bank_transfer_id),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseBankTransferAttachment,
        table=public.purchase_bank_transfer_attachment,
        fields=[
            orm1.Field('id', public.purchase_bank_transfer_attachment.id, is_primary=True),
            orm1.Field('media_uri', public.purchase_bank_transfer_attachment.media_uri),
            orm1.Field('created_at', public.purchase_bank_transfer_attachment.created_at),
        ],
    )
)
orm1.register_mapping(
    orm1.EntityMapping.define(
        entity=tests.entities.purchase.PurchaseCouponUsage,
        table=public.purchase_coupon_usage,
        fields=[
            orm1.Field('id', public.purchase_coupon_usage.id, is_primary=True),
            orm1.Field('coupon_id', public.purchase_coupon_usage.coupon_id),
        ],
    )
)
