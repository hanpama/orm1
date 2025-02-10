from orm1 import auto

from .base import AutoRollbackTestCase

schema = """
    DO $$ BEGIN
        CREATE SCHEMA test_crud_composite;
        CREATE TABLE test_crud_composite.person (
            key1 INT,
            key2 INT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            name VARCHAR(100) NOT NULL,
            "AGE" INTEGER NOT NULL,

            PRIMARY KEY (key1, key2)
        );
    END $$;
"""


@auto.mapped(
    schema="test_crud_composite",
    table="person",
    primary_key=("key1", "key2"),
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
    key1: int
    key2: int
    created_at: str
    name: str
    age: int

    def __init__(self, key1: int, key2: int, name: str, age: int):
        self.key1 = key1
        self.key2 = key2
        self.name = name
        self.age = age


class CrudCompositeTestCase(AutoRollbackTestCase):
    obj1 = Person(
        key1=1,
        key2=2,
        name="Alice",
        age=20,
    )
    obj2 = Person(
        key1=3,
        key2=4,
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
        found = await session.get(Person, (self.obj1.key1, self.obj1.key2))

        assert found is not None
        assert isinstance(found, Person)
        assert found.key1 == self.obj1.key1
        assert found.key2 == self.obj1.key2

    async def test_batch_get(self):
        session = self.session()
        keys = (
            (self.obj1.key1, self.obj1.key2),
            (-1, -1),
            (self.obj2.key1, self.obj2.key2),
        )
        objs = await session.batch_get(Person, keys)

        assert len(objs) == 3
        assert len([o for o in objs if o is not None]) == 2
        assert {(o.key1, o.key2) for o in objs if o is not None} == {
            (self.obj1.key1, self.obj1.key2),
            (self.obj2.key1, self.obj2.key2),
        }

    async def test_save_insert(self):
        session = self.session()

        obj = Person(
            key1=5,
            key2=6,
            name="Charlie",
            age=40,
        )
        await session.save(obj)

        assert obj.key1 is not None
        assert obj.key2 is not None

        found = await session.get(Person, (obj.key1, obj.key2))
        assert found is not None
        assert found.key1 == obj.key1
        assert found.key2 == obj.key2
        assert found.name == obj.name
        assert found.age == obj.age

    async def test_save_update(self):
        session = self.session()

        obj1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        assert obj1 is not None

        obj1.name = "Alice2"
        obj1.age = 21
        await session.save(obj1)

        found = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        assert found is not None
        assert found.key1 == obj1.key1
        assert found.key2 == obj1.key2
        assert found.name == "Alice2"
        assert found.age == 21

    async def test_batch_save(self):
        session = self.session()

        obj1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        obj2 = await session.get(Person, (self.obj2.key1, self.obj2.key2))
        assert obj1 is not None
        assert obj2 is not None

        obj1.name = "Alice2"
        obj2.name = "Bob2"
        obj1.age = 21
        obj2.age = 31

        obj3 = Person(
            key1=7,
            key2=8,
            name="Charlie",
            age=40,
        )
        obj4 = Person(
            key1=9,
            key2=10,
            name="David",
            age=50,
        )

        await session.batch_save(Person, obj1, obj2, obj3, obj4)

        found1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        found2 = await session.get(Person, (self.obj2.key1, self.obj2.key2))
        found3 = await session.get(Person, (obj3.key1, obj3.key2))
        found4 = await session.get(Person, (obj4.key1, obj4.key2))

        assert found1 is not None
        assert found2 is not None
        assert found3 is not None
        assert found4 is not None

        assert found1.key1 == obj1.key1
        assert found1.key2 == obj1.key2
        assert found1.name == "Alice2"
        assert found1.age == 21

        assert found2.key1 == obj2.key1
        assert found2.key2 == obj2.key2
        assert found2.name == "Bob2"
        assert found2.age == 31

        assert found3.key1 == obj3.key1
        assert found3.key2 == obj3.key2
        assert found3.name == "Charlie"
        assert found3.age == 40

        assert found4.key1 == obj4.key1
        assert found4.key2 == obj4.key2
        assert found4.name == "David"
        assert found4.age == 50

    async def test_delete(self):
        session = self.session()

        obj1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        assert obj1 is not None

        await session.delete(obj1)

        found = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        assert found is None

    async def test_batch_delete(self):
        session = self.session()

        obj1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        obj2 = await session.get(Person, (self.obj2.key1, self.obj2.key2))
        assert obj1 is not None
        assert obj2 is not None

        await session.batch_delete(Person, obj1, obj2)

        found1 = await session.get(Person, (self.obj1.key1, self.obj1.key2))
        found2 = await session.get(Person, (self.obj2.key1, self.obj2.key2))

        assert found1 is None
        assert found2 is None
