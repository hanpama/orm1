from datetime import datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase
from uuid import UUID

import asyncpg

from orm1 import Session, AsyncPGSessionBackend, auto

from .entities.purchase import (
    Purchase,
    PurchaseBankTransfer,
    PurchaseBankTransferAttachment,
    PurchaseCouponUsage,
    PurchaseLineItem,
)


class SimpleTest(IsolatedAsyncioTestCase):

    purchase1 = Purchase(
        id=UUID("a028f36d-4cba-476f-9143-3bbe8e0b5f8d"),
        code="CP-00032",
        user_id=UUID("50dc79f1-06d7-44d3-b1d4-e8db7d982a59"),
        line_items=[
            PurchaseLineItem(
                product_id=UUID("853868c7-570c-4e65-8d6d-ebeb185e4eb7"),
                quantity=8,
            ),
            PurchaseLineItem(
                product_id=UUID("e57b3a2d-037d-49e7-a5bf-d76977dcc625"),
                quantity=2,
            ),
        ],
        bank_transfers=[
            PurchaseBankTransfer(
                sender_name="John Doe",
                transfer_time=datetime.fromisoformat("2021-08-01T12:00:00"),
                amount=Decimal(100.0),
                attachments=[
                    PurchaseBankTransferAttachment(
                        media_uri="https://example.com/attachment1.jpg",
                    ),
                    PurchaseBankTransferAttachment(
                        media_uri="https://example.com/attachment2.jpg",
                    ),
                ],
            ),
        ],
        coupon_usage=PurchaseCouponUsage(
            coupon_id=UUID("b8c8c6c5-4f8e-4c4b-8f9d-1d7d2a8e5a0a"),
        ),
    )

    purchase2 = Purchase(
        code="CP-00100",
        user_id=UUID("50dc79f1-06d7-44d3-b1d4-e8db7d982a59"),
        line_items=[
            PurchaseLineItem(
                product_id=UUID("853868c7-570c-4e65-8d6d-ebeb185e4eb7"),
                quantity=7,
            ),
        ],
        bank_transfers=[
            PurchaseBankTransfer(
                sender_name="John Doe",
                transfer_time=datetime.fromisoformat("2021-08-01T12:00:00"),
                amount=Decimal(40.0),
                attachments=[
                    PurchaseBankTransferAttachment(
                        media_uri="https://example.com/attachment1.jpg",
                    ),
                ],
            ),
        ],
        coupon_usage=PurchaseCouponUsage(
            coupon_id=UUID("9ea9fea6-2c10-4a8c-87d2-425c5dcfd671"),
        ),
    )

    purchase3 = Purchase(
        code="CP-00120",
        user_id=UUID("50dc79f1-06d7-44d3-b1d4-e8db7d982a59"),
        line_items=[
            PurchaseLineItem(
                product_id=UUID("e57b3a2d-037d-49e7-a5bf-d76977dcc625"),
                quantity=2,
            ),
        ],
        bank_transfers=[
            PurchaseBankTransfer(
                sender_name="John Doe",
                transfer_time=datetime.fromisoformat("2021-08-01T12:00:00"),
                amount=Decimal(129.99),
                attachments=[],
            ),
        ],
    )

    purchase4 = Purchase(
        code="CP-00121",
        user_id=UUID("d319dd20-8622-4602-bb4e-eed7cc80bef1"),
        line_items=[
            PurchaseLineItem(
                product_id=UUID("e57b3a2d-037d-49e7-a5bf-d76977dcc625"),
                quantity=2,
            ),
        ],
        bank_transfers=[
            PurchaseBankTransfer(
                sender_name="John Doe",
                transfer_time=datetime.fromisoformat("2021-08-01T12:00:00"),
                amount=Decimal(129.99),
                attachments=[],
            ),
        ],
    )

    async def test_update_root_scalar(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.code = "CP-00033"
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert p2.code == "CP-00033"

    async def test_set_null_root_scalar(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.user_id = None
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert p2.user_id is None

    async def test_delete_root(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        await s1.delete(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert not p2

    async def test_plural_append(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.line_items.append(PurchaseLineItem(product_id=UUID("853868c7-570c-4e65-8d6d-ebeb185e4eb7"), quantity=3))
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert len(p2.line_items) == 3

    async def test_plural_scalar_update(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.line_items[0].quantity = 10
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert p2.line_items[0].quantity == 10

    async def test_plural_delete(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        del p1.line_items[0]
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert len(p2.line_items) == 1

    async def test_plural_null(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        setattr(p1, "line_items", None)  # p1.line_items = None
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert p2.line_items == []

    async def test_singular_set(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.coupon_usage = PurchaseCouponUsage(coupon_id=UUID("f5d8b5ea-c7cb-407d-9595-273eb1a87a6b"))
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert p2.coupon_usage
        assert p2.coupon_usage.coupon_id == UUID("f5d8b5ea-c7cb-407d-9595-273eb1a87a6b")

    async def test_singular_delete(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        p1.coupon_usage = None
        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)
        assert p2
        assert not p2.coupon_usage

    async def test_nested_complex_update(self):
        s1 = self._session()
        p1 = await s1.get(Purchase, self.purchase1.id)
        assert p1

        # root scalar update
        p1.code = "CP-00033"

        # root scalar set null
        p1.user_id = None

        # line item add
        p1.line_items.append(
            PurchaseLineItem(
                product_id=UUID("85ba2b41-4433-4d6d-9b63-b1d1c011f23e"),
                quantity=3,
            )
        )
        # line item update
        p1.line_items[0].quantity = 10

        # line item remove
        del p1.line_items[1]

        # bank transfer add
        p1.bank_transfers.append(
            PurchaseBankTransfer(
                sender_name="Jane Doe",
                transfer_time=datetime.fromisoformat("2021-08-02T12:00:00"),
                amount=Decimal(200.0),
                attachments=[
                    PurchaseBankTransferAttachment(
                        media_uri="https://example.com/attachment3.jpg",
                    ),
                ],
            )
        )

        # bank transfer update > add attachment
        p1.bank_transfers[0].attachments.append(
            PurchaseBankTransferAttachment(
                media_uri="https://example.com/attachment3.jpg",
            )
        )

        # bank transfer update > update attachment
        p1.bank_transfers[0].attachments[0].media_uri = "https://example.com/attachment4.jpg"

        # bank transfer update > remove attachment
        del p1.bank_transfers[0].attachments[1]

        # Singular set
        p1.coupon_usage = PurchaseCouponUsage(coupon_id=UUID("5a93b543-3784-48eb-b5ea-69a4a7f40967"))

        await s1.save(p1)

        s2 = self._session()
        p2 = await s2.get(Purchase, self.purchase1.id)

        assert p2
        assert p1 is not p2
        assert p1 == p2

    async def test_raw(self):
        s = self._session()
        query = s.raw(
            """
            SELECT 'this is :string' as "a:a", created_at::TIMESTAMPTZ
            FROM purchase AS p
            WHERE p.code = :code
            ORDER BY p.id DESC
            OFFSET 0
            LIMIT 10;
            """,
            code="CP-00032",
        )

        results = await query.fetch()

        assert len(results) == 1
        assert results[0]["a:a"] == "this is :string"
        assert results[0]["created_at"]

    async def test_root_find_forward(self):
        s = self._session()
        q = s.query(Purchase, "p").filter(
            "p.user_id = :value",
            value="50dc79f1-06d7-44d3-b1d4-e8db7d982a59",
        )
        results = await q.order_by(
            q.desc("p.code"),
            q.asc("p.id"),
        ).fetch(limit=2)

        assert len(results) == 2

        assert results[0].code == self.purchase3.code
        assert results[1].code == self.purchase2.code

    async def asyncSetUp(self) -> None:
        self._conn: asyncpg.Connection = await asyncpg.connect(self.dsn)
        self._backend = AsyncPGSessionBackend(self._conn)
        self._tx = self._conn.transaction()

        session = Session(self._backend)

        await self._tx.start()
        await session.batch_save(
            Purchase,
            self.purchase1,
            self.purchase2,
            self.purchase3,
        )

        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        await self._tx.rollback()
        await self._conn.close()
        return await super().asyncTearDown()

    def _session(self):
        return Session(self._backend)

    dsn = "postgresql://postgres:8800bc84f23af727f4e9@localhost:3200/postgres"
