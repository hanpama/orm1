# GENERATED CODE, DO NOT MODIFY
from orm1 import Table, Column


class _course_module_material(Table):
    def __init__(self):
        super().__init__('public', 'course_module_material')
        self.id = Column("id", "varchar", primary=True)
        self.course_module_id = Column("course_module_id", "varchar")
        self.media_uri = Column("media_uri", "text")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _purchase(Table):
    def __init__(self):
        super().__init__('public', 'purchase')
        self.id = Column("id", "uuid", default="uuid_generate_v4()", primary=True)
        self.code = Column("code", "varchar")
        self.user_id = Column("user_id", "uuid")
        self.created_at = Column("created_at", "timestamp", default="now()")
        self.receipt = Column("receipt", "jsonb")

class _purchase_line_item(Table):
    def __init__(self):
        super().__init__('public', 'purchase_line_item')
        self.id = Column("id", "uuid", default="uuid_generate_v4()", primary=True)
        self.purchase_id = Column("purchase_id", "uuid")
        self.product_id = Column("product_id", "uuid")
        self.quantity = Column("quantity", "int4")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _purchase_bank_transfer(Table):
    def __init__(self):
        super().__init__('public', 'purchase_bank_transfer')
        self.id = Column("id", "uuid", default="uuid_generate_v4()", primary=True)
        self.purchase_id = Column("purchase_id", "uuid")
        self.sender_name = Column("sender_name", "varchar")
        self.transfer_time = Column("transfer_time", "timestamp")
        self.amount = Column("amount", "numeric")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _purchase_bank_transfer_attachment(Table):
    def __init__(self):
        super().__init__('public', 'purchase_bank_transfer_attachment')
        self.id = Column("id", "uuid", default="uuid_generate_v4()", primary=True)
        self.purchase_bank_transfer_id = Column("purchase_bank_transfer_id", "uuid")
        self.media_uri = Column("media_uri", "text")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _purchase_coupon_usage(Table):
    def __init__(self):
        super().__init__('public', 'purchase_coupon_usage')
        self.id = Column("id", "uuid", default="uuid_generate_v4()", primary=True)
        self.purchase_id = Column("purchase_id", "uuid")
        self.coupon_id = Column("coupon_id", "uuid")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _course(Table):
    def __init__(self):
        super().__init__('public', 'course')
        self.semester_id = Column("semester_id", "varchar", primary=True)
        self.subject_id = Column("subject_id", "varchar", primary=True)
        self.created_at = Column("created_at", "timestamp", default="now()")

class _course_attachment(Table):
    def __init__(self):
        super().__init__('public', 'course_attachment')
        self.id = Column("id", "varchar", primary=True)
        self.course_semester_id = Column("course_semester_id", "varchar")
        self.course_subject_id = Column("course_subject_id", "varchar")
        self.media_uri = Column("media_uri", "text")
        self.created_at = Column("created_at", "timestamp", default="now()")

class _course_module(Table):
    def __init__(self):
        super().__init__('public', 'course_module')
        self.id = Column("id", "varchar", primary=True)
        self.course_semester_id = Column("course_semester_id", "varchar")
        self.course_subject_id = Column("course_subject_id", "varchar")
        self.title = Column("title", "varchar")
        self.created_at = Column("created_at", "timestamp", default="now()")


course_module_material = _course_module_material()
purchase = _purchase()
purchase_line_item = _purchase_line_item()
purchase_bank_transfer = _purchase_bank_transfer()
purchase_bank_transfer_attachment = _purchase_bank_transfer_attachment()
purchase_coupon_usage = _purchase_coupon_usage()
course = _course()
course_attachment = _course_attachment()
course_module = _course_module()

__all__ = [
    'course_module_material',
    'purchase',
    'purchase_line_item',
    'purchase_bank_transfer',
    'purchase_bank_transfer_attachment',
    'purchase_coupon_usage',
    'course',
    'course_attachment',
    'course_module',
]
