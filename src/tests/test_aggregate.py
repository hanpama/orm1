import typing
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from orm1 import auto

from .base import AutoRollbackTestCase

schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_aggregate;
        SET search_path TO test_aggregate;
        
        CREATE TABLE purchase (
            id INT PRIMARY KEY,
            customer_id INT,
            price DECIMAL(10, 2) NOT NULL
        );

        CREATE TABLE purchase_line_item (
            purchase_id INT NOT NULL REFERENCES purchase(id),
            index INT NOT NULL,
            product_id INT,
            quantity INT NOT NULL,
            PRIMARY KEY (purchase_id, index)
        );

        CREATE TABLE purchase_billing (
            id INT PRIMARY KEY,
            purchase_id INT NOT NULL REFERENCES purchase(id),
            payment_method VARCHAR(64) NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            billing_time TIMESTAMP NOT NULL
        );

        CREATE TABLE purchase_billing_attachment (
            id INT PRIMARY KEY,
            purchase_billing_id INT NOT NULL REFERENCES purchase_billing(id),
            media_uri TEXT NOT NULL
        );

        CREATE TABLE purchase_billing_payment (
            purchase_billing_id INT PRIMARY KEY REFERENCES purchase_billing(id),
            payment_time TIMESTAMP NOT NULL,
            amount DECIMAL(10, 2) NOT NULL
        );

        CREATE TABLE purchase_withdrawal (
            id INT PRIMARY KEY,
            purchase_id INT NOT NULL REFERENCES purchase(id),
            created_at TIMESTAMP,
            remark TEXT,

            UNIQUE (purchase_id)
        );

        CREATE TABLE purchase_withdrawal_attachment (
            id INT PRIMARY KEY,
            purchase_withdrawal_id INT NOT NULL REFERENCES purchase_withdrawal(id),
            media_uri TEXT NOT NULL
        );
    END $$;
"""


@dataclass
@auto.mapped(schema="test_aggregate")
class Purchase:
    id: int
    customer_id: int | None
    price: Decimal
    line_items: "typing.List[PurchaseLineItem]"
    billings: list["PurchaseBilling"]
    withdrawal: "typing.Optional[PurchaseWithdrawal]"

    _private_field = None


@dataclass
@auto.mapped(
    schema="test_aggregate",
    primary_key=("purchase_id", "index"),
    parental_key="purchase_id",
)
class PurchaseLineItem:
    purchase_id: int
    index: int
    product_id: int | None
    quantity: int


@dataclass
@auto.mapped(
    schema="test_aggregate",
    parental_key="purchase_id",
)
class PurchaseBilling:
    id: int
    purchase_id: int
    payment_method: str
    amount: Decimal
    billing_time: datetime
    attachments: "list[PurchaseBillingAttachment]"
    payment: "typing.Optional[PurchaseBillingPayment]"


@dataclass
@auto.mapped(
    schema="test_aggregate",
    parental_key="purchase_billing_id",
)
class PurchaseBillingAttachment:
    id: int
    purchase_billing_id: int
    media_uri: str


@dataclass
@auto.mapped(
    schema="test_aggregate",
    primary_key="purchase_billing_id",
    parental_key="purchase_billing_id",
)
class PurchaseBillingPayment:
    purchase_billing_id: int
    payment_time: datetime
    amount: Decimal


@dataclass
@auto.mapped(schema="test_aggregate", parental_key="purchase_id")
class PurchaseWithdrawal:
    id: int
    purchase_id: int
    created_at: datetime | None
    remark: str | None
    attachments: "list[PurchaseWithdrawalAttachment]"


@dataclass
@auto.mapped(
    schema="test_aggregate",
    parental_key="purchase_withdrawal_id",
)
class PurchaseWithdrawalAttachment:
    id: int
    purchase_withdrawal_id: int
    media_uri: str


class AggregateTestCase(AutoRollbackTestCase):
    purchase1 = Purchase(
        id=1,
        customer_id=1,
        price=Decimal("100.00"),
        line_items=[
            PurchaseLineItem(purchase_id=1, index=1, product_id=1, quantity=2),
            PurchaseLineItem(purchase_id=1, index=2, product_id=2, quantity=1),
        ],
        billings=[
            PurchaseBilling(
                id=1,
                purchase_id=1,
                payment_method="credit_card",
                amount=Decimal("100.00"),
                billing_time=datetime(2021, 1, 1, 12, 0, 0),
                attachments=[
                    PurchaseBillingAttachment(id=1, purchase_billing_id=1, media_uri="http://example.com/1"),
                    PurchaseBillingAttachment(id=2, purchase_billing_id=1, media_uri="http://example.com/2"),
                ],
                payment=PurchaseBillingPayment(
                    purchase_billing_id=1,
                    payment_time=datetime(2021, 1, 1, 12, 0, 0),
                    amount=Decimal("100.00"),
                ),
            ),
        ],
        withdrawal=None,
    )
    purchase2 = Purchase(
        id=2,
        customer_id=2,
        price=Decimal("200.00"),
        line_items=[
            PurchaseLineItem(purchase_id=2, index=1, product_id=3, quantity=3),
        ],
        billings=[
            PurchaseBilling(
                id=2,
                purchase_id=2,
                payment_method="credit_card",
                amount=Decimal("200.00"),
                billing_time=datetime(2021, 1, 2, 12, 0, 0),
                attachments=[
                    PurchaseBillingAttachment(id=3, purchase_billing_id=2, media_uri="http://example.com/3"),
                    PurchaseBillingAttachment(id=4, purchase_billing_id=2, media_uri="http://example.com/4"),
                ],
                payment=None,
            ),
        ],
        withdrawal=PurchaseWithdrawal(
            id=1,
            purchase_id=2,
            created_at=datetime(2021, 1, 3, 12, 0, 0),
            remark="Withdrawal remark",
            attachments=[
                PurchaseWithdrawalAttachment(id=1, purchase_withdrawal_id=1, media_uri="http://example.com/5"),
                PurchaseWithdrawalAttachment(id=2, purchase_withdrawal_id=1, media_uri="http://example.com/6"),
            ],
        ),
    )

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        session = self.session()
        await session.raw(schema).fetch()

        await session.batch_save(Purchase, self.purchase1, self.purchase2)

    async def test_get(self):
        session = self.session()
        found = await session.get(Purchase, 1)

        assert found
        assert found == self.purchase1

    async def test_save_plural_adding_child(self):
        session = self.session()
        entity = await session.get(Purchase, 1)
        assert entity

        entity.billings.append(
            PurchaseBilling(
                id=3,
                purchase_id=1,
                payment_method="credit_card",
                amount=Decimal("300.00"),
                billing_time=datetime(2021, 1, 3, 12, 0, 0),
                attachments=[
                    PurchaseBillingAttachment(id=5, purchase_billing_id=3, media_uri="http://example.com/7"),
                    PurchaseBillingAttachment(id=6, purchase_billing_id=3, media_uri="http://example.com/8"),
                ],
                payment=PurchaseBillingPayment(
                    purchase_billing_id=3,
                    payment_time=datetime(2021, 1, 3, 12, 0, 0),
                    amount=Decimal("300.00"),
                ),
            )
        )

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 1)

    async def test_save_plural_editing_child(self):
        session = self.session()
        entity = await session.get(Purchase, 1)
        assert entity
        assert entity.billings[0].payment

        entity.billings[0].payment.amount = Decimal("200.00")

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 1)

    async def test_save_plural_removing_child(self):
        session = self.session()
        entity = await session.get(Purchase, 1)
        assert entity

        entity.billings.pop()

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 1)

    async def test_save_singular_adding_child(self):
        session = self.session()
        entity = await session.get(Purchase, 1)
        assert entity

        entity.withdrawal = PurchaseWithdrawal(
            id=2,
            purchase_id=1,
            created_at=datetime(2021, 1, 4, 12, 0, 0),
            remark="Withdrawal remark",
            attachments=[
                PurchaseWithdrawalAttachment(id=3, purchase_withdrawal_id=2, media_uri="http://example.com/9"),
                PurchaseWithdrawalAttachment(id=4, purchase_withdrawal_id=2, media_uri="http://example.com/10"),
            ],
        )

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 1)

    async def test_save_singular_editing_child(self):
        session = self.session()
        entity = await session.get(Purchase, 2)
        assert entity
        assert entity.withdrawal

        entity.withdrawal.remark = "New remark"

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 2)

    async def test_save_singular_removing_child(self):
        session = self.session()
        entity = await session.get(Purchase, 2)
        assert entity

        entity.withdrawal = None

        await session.save(entity)
        assert entity == await self.session().get(Purchase, 2)

    async def test_delete(self):
        session = self.session()
        entity = await session.get(Purchase, 1)
        assert entity

        await session.delete(entity)
        assert not await session.get(Purchase, 1)

    async def test_delete_unpersisted_root(self):
        session = self.session()
        entity = Purchase(
            id=3,
            customer_id=3,
            price=Decimal("300.00"),
            line_items=[],
            billings=[],
            withdrawal=None,
        )
        assert await session.get(Purchase, 3) is None
        await session.delete(entity)
        assert await session.get(Purchase, 3) is None

    async def test_delete_unpersisted_child(self):
        session = self.session()
        entity = Purchase(
            id=3,
            customer_id=3,
            price=Decimal("300.00"),
            line_items=[],
            billings=[],
            withdrawal=PurchaseWithdrawal(
                id=3,
                purchase_id=3,
                created_at=datetime(2021, 1, 4, 12, 0, 0),
                remark="Withdrawal remark",
                attachments=[
                    PurchaseWithdrawalAttachment(id=3, purchase_withdrawal_id=3, media_uri="http://example.com/9"),
                    PurchaseWithdrawalAttachment(id=4, purchase_withdrawal_id=3, media_uri="http://example.com/10"),
                ],
            ),
        )
        assert await session.get(Purchase, 3) is None
        await session.delete(entity)
        assert await session.get(Purchase, 3) is None
