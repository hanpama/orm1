from uuid import UUID

from orm1 import auto

from .base import AutoRollbackTestCase

schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_crud_simple_assigned;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        CREATE TABLE test_crud_simple_assigned.person (
            id UUID PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            name VARCHAR(100) NOT NULL,
            "AGE" INTEGER NOT NULL
        );
    END $$;
"""


@auto.mapped(
    schema="test_crud_simple_assigned",
    table="person",
    fields={
        "created_at": {
            "skip_on_insert": True,
            "skip_on_update": True,
        },
        "age": {
            "column": "AGE",
        },
    },
)
class Person:
    id: UUID
    created_at: str
    name: str
    age: int

    def __init__(self, id: UUID, name: str, age: int):
        self.id = id
        self.name = name
        self.age = age


class CrudSimpleGeneratedTestCase(AutoRollbackTestCase):
    obj1 = Person(
        id=UUID("9566a0c7-cc51-4f84-8304-62b4b651084e"),
        name="Alice",
        age=20,
    )
    obj2 = Person(
        id=UUID("91f896b1-0c21-4a58-913b-d95d81d1d09f"),
        name="Bob",
        age=30,
    )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        session = self.session()
        await session.raw(schema).fetch()
        await session.batch_save(Person, self.obj1, self.obj2)

    async def test_get(self):
        session = self.session()
        found = await session.get(Person, self.obj1.id)

        assert found is not None
        assert isinstance(found, Person)
        assert found.id == self.obj1.id

    async def test_batch_get(self):
        session = self.session()
        keys = (self.obj1.id, UUID("00000000-0000-0000-0000-000000000000"), self.obj2.id)
        objs = await session.batch_get(Person, keys)

        assert len(objs) == 3
        assert len([o for o in objs if o is not None]) == 2
        assert {o.id for o in objs if o is not None} == {self.obj1.id, self.obj2.id}

    async def test_save_insert(self):
        session = self.session()

        obj = Person(
            id=UUID("233d52b1-674f-400e-a569-ab1ae21f79eb"),
            name="Charlie",
            age=40,
        )
        await session.save(obj)

        assert obj.id is not None

        found = await session.get(Person, obj.id)
        assert found is not None
        assert found.id == obj.id
        assert found.name == obj.name
        assert found.age == obj.age

    async def test_save_update(self):
        session = self.session()

        obj1 = await session.get(Person, self.obj1.id)
        assert obj1 is not None

        obj1.name = "Alice2"
        obj1.age = 21
        await session.save(obj1)

        found = await session.get(Person, self.obj1.id)
        assert found is not None
        assert found.id == obj1.id
        assert found.name == "Alice2"
        assert found.age == 21

    async def test_batch_save(self):
        session = self.session()

        obj1 = await session.get(Person, self.obj1.id)
        obj2 = await session.get(Person, self.obj2.id)
        assert obj1 is not None
        assert obj2 is not None

        obj1.name = "Alice2"
        obj2.name = "Bob2"
        obj1.age = 21
        obj2.age = 31

        obj3 = Person(
            id=UUID("f88ba1c9-cddb-4694-8f86-97178200d429"),
            name="Charlie",
            age=40,
        )
        obj4 = Person(
            id=UUID("902ffb0d-efee-4155-9cfd-1c880dd52a76"),
            name="David",
            age=50,
        )

        await session.batch_save(Person, obj1, obj2, obj3, obj4)

        found1 = await session.get(Person, self.obj1.id)
        found2 = await session.get(Person, self.obj2.id)
        found3 = await session.get(Person, obj3.id)
        found4 = await session.get(Person, obj4.id)

        assert found1 is not None
        assert found2 is not None
        assert found3 is not None
        assert found4 is not None

        assert found1.id == obj1.id
        assert found1.name == "Alice2"
        assert found1.age == 21

        assert found2.id == obj2.id
        assert found2.name == "Bob2"
        assert found2.age == 31

        assert found3.id == obj3.id
        assert found3.name == "Charlie"
        assert found3.age == 40

        assert found4.id == obj4.id
        assert found4.name == "David"
        assert found4.age == 50

    async def test_delete(self):
        session = self.session()

        obj1 = await session.get(Person, self.obj1.id)
        assert obj1 is not None

        await session.delete(obj1)

        found = await session.get(Person, self.obj1.id)
        assert found is None

    async def test_batch_delete(self):
        session = self.session()

        obj1 = await session.get(Person, self.obj1.id)
        obj2 = await session.get(Person, self.obj2.id)
        assert obj1 is not None
        assert obj2 is not None

        await session.batch_delete(Person, obj1, obj2)

        found1 = await session.get(Person, self.obj1.id)
        found2 = await session.get(Person, self.obj2.id)

        assert found1 is None
        assert found2 is None
