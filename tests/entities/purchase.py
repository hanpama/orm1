from uuid import UUID
from datetime import datetime
from decimal import Decimal
from orm1 import auto


@auto.mapped()
class Purchase:
    id: UUID | None
    code: str
    user_id: UUID | None
    line_items: list["PurchaseLineItem"]
    bank_transfers: list["PurchaseBankTransfer"]
    coupon_usage: "PurchaseCouponUsage | None"

    def __init__(
        self,
        id: UUID | None = None,
        code: str | None = None,
        user_id: UUID | None = None,
        line_items: list["PurchaseLineItem"] = [],
        bank_transfers: list["PurchaseBankTransfer"] = [],
        coupon_usage: "PurchaseCouponUsage | None" = None,
    ):
        self.id = id
        self.code = code or ""
        self.user_id = user_id or None
        self.line_items = line_items
        self.bank_transfers = bank_transfers
        self.coupon_usage = coupon_usage

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@auto.mapped()
class PurchaseLineItem:
    id: UUID | None
    product_id: UUID | None
    quantity: int

    def __init__(
        self,
        id: UUID | None = None,
        product_id: UUID | None = None,
        quantity: int | None = None,
    ):
        self.id = id
        self.product_id = product_id or None
        self.quantity = quantity or 0

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@auto.mapped()
class PurchaseBankTransfer:
    id: UUID | None
    sender_name: str
    transfer_time: datetime
    amount: Decimal | None
    attachments: list["PurchaseBankTransferAttachment"]

    def __init__(
        self,
        id: UUID | None = None,
        sender_name: str | None = None,
        transfer_time: datetime | None = None,
        amount: Decimal | None = None,
        attachments: list["PurchaseBankTransferAttachment"] = [],
    ):
        self.id = id
        self.sender_name = sender_name or ""
        self.transfer_time = transfer_time or datetime.min
        self.amount = amount or Decimal(0)
        self.attachments = attachments

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@auto.mapped()
class PurchaseBankTransferAttachment:
    id: UUID | None
    media_uri: str
    created_at: datetime

    def __init__(
        self,
        id: UUID | None = None,
        media_uri: str | None = None,
        created_at: datetime | None = None,
    ):
        self.id = id
        self.media_uri = media_uri or ""
        self.created_at = created_at or datetime.min

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@auto.mapped()
class PurchaseCouponUsage:
    id: UUID | None
    coupon_id: UUID | None

    def __init__(
        self,
        id: UUID | None = None,
        coupon_id: UUID | None = None,
    ):
        self.id = id
        self.coupon_id = coupon_id or None

    def __eq__(self, other):
        return self.__dict__ == other.__dict__
