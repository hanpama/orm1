# orm1

orm1 is a minimal asynchronous Object-Relational Mapping (ORM) library for Python that makes it easy to map your Python classes to PostgreSQL database tables. Built on top of [asyncpg](https://github.com/MagicStack/asyncpg), orm1 provides a minimal yet powerful API for CRUD operations, complex query building, and relationship management – all with full async support.

---

## Features

- **Asynchronous by Design**  
  Leverage Python’s `async`/`await` syntax with an asyncpg backend for high-performance, non-blocking database operations.

- **Auto-mapping with Type Hints**  
  Automatically generate entity mappings from your class type hints. orm1 can infer column names and even detect relationships (both singular and plural) based on your annotations.

- **Aggregate Support (Parent-Child Relationship)**  
  Define aggregates naturally by specifying child entities that are part of an aggregate. For instance, a `Post` aggregate can include multiple `PostAttachment` instances.

- **Fluent Query Builder**  
  Construct complex SQL queries using a Pythonic API. orm1 supports filtering, joins, ordering, and pagination, all while keeping your code readable.

- **Raw SQL Queries**  
  When you need full control, execute raw SQL queries with safe parameter binding.

- **Flexible Transaction Management**  
  Manage database transactions with simple `begin()`, `commit()`, and `rollback()` methods.

---

## Installation

Install orm1 from PyPI with pip:

```bash
pip install orm1
```

---

## Getting Started

### Defining Your Entities

Define your entity classes with type hints to describe fields and relationships. Use the `auto.mapped` decorator from the AutoMappingBuilder to register your classes with orm1. Column names are automatically generated (using snake_case conversion) unless overridden in the configuration.

Below is an example using a `Post` aggregate with its child `PostAttachment`:

```python
from orm1 import auto

@auto.mapped(
    table="posts",
    primary_key="id"
)
class Post:
    id: int
    title: str
    content: str
    # Define the aggregate relationship: a post has many attachments.
    attachments: list["PostAttachment"]

@auto.mapped(
    table="post_attachments",
    primary_key="id",
    parental_key="post_id"  # Indicates this entity belongs to a Post aggregate.
)
class PostAttachment:
    id: int
    post_id: int
    file_name: str
    url: str
```

The mappings above assume that you have a PostgreSQL database with tables `posts` and `post_attachments`. The `PostAttachment` table has a foreign key `post_id` that references the `id` column of the `posts` table.

```sql
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL
);

CREATE TABLE post_attachments (
    id SERIAL PRIMARY KEY,
    post_id INT NOT NULL REFERENCES posts(id),
    file_name TEXT NOT NULL,
    url TEXT NOT NULL
);
```

### Building Mappings

After decorating your entity classes, build the mappings. This process collects the configuration and type hints to create the internal mapping definitions used by orm1.

```python
from orm1 import auto

mappings = auto.build()
```

### Creating a Session

Create a session to interact with your database. The session uses a backend (such as `AsyncPGSessionBackend`) for executing SQL statements.

```python
import asyncpg
from orm1 import Session, AsyncPGSessionBackend

async def main():
    # Create an asyncpg connection pool.
    pool = await asyncpg.create_pool(dsn="postgresql://user:password@localhost/dbname")
    
    # Initialize the backend and session.
    backend = AsyncPGSessionBackend(pool)
    session = Session(backend, mappings)
    
    # Now you can perform CRUD operations with your session.
```

---

## CRUD Operations

### Inserting and Saving Entities

To insert a new entity or update an existing one, simply call `save` on your session. orm1 will determine whether to insert or update based on whether the entity is already tracked.

```python
# Create a new post with an attachment.
post = Post()
post.title = "Introducing orm1"
post.content = "orm1 is a lightweight asynchronous ORM for Python."

attachment = PostAttachment()
attachment.file_name = "diagram.png"
attachment.url = "http://example.com/diagram.png"

post.attachments = [attachment]

# Save the post (and cascade save the attachment as part of the aggregate).
await session.save(post)
```

### Querying Entities

Use the query builder to fetch entities with filtering, joins, and ordering.

```python
# Query for a post by title.
query = session.query(Post, alias="p")
query = query.filter('p."title" = :title', title="Introducing orm1")
posts = await query.fetch()

# Get a single post.
post = await query.fetch_one()
```

### Updating Entities

Modify the attributes of an entity and call `save` again. orm1 will automatically update the corresponding database record.

```python
# Update the post's title.
post.title = "Introducing orm1 - A Lightweight Async ORM"
await session.save(post)
```

### Deleting Entities

Remove an entity by calling `delete` on the session. Deleting an aggregate root will cascade and remove its child entities as needed.

```python
await session.delete(post)
```

---

## Advanced Querying

### Joining Related Entities

Join related tables by using the `join` or `left_join` methods on a query. This is useful when you need to filter or retrieve data based on relationships.

```python
# Join post attachments with their parent posts.
query = session.query(PostAttachment, "a")
query = query.join(Post, "p", 'a.post_id = p.id')
query = query.filter('p.title = :title', title="Introducing orm1")
attachments = await query.fetch()
```

### Ordering and Pagination

Sort your query results and paginate them easily.

```python
# Ordering: specify ascending or descending order.
query = session.query(PostAttachment, "a")
query = query.order_by(query.asc('a.file_name'))
attachments = await query.fetch(limit=10, offset=0)

# Pagination: use paginate to get page information.
page = await query.paginate(first=10)
print("Attachment IDs on current page:", page.ids)
print("Has next page?", page.has_next_page)
```

### Executing Raw SQL Queries

If you need more control, execute raw SQL statements while still benefiting from parameter binding.

```python
raw_query = session.raw("SELECT * FROM post_attachments WHERE file_name LIKE :pattern", pattern="%diagram%")
results = await raw_query.fetch()
```

---

## API Reference

### Session

- **`get(entity_type, id)`**  
  Retrieve a single entity by its primary key.

- **`save(entity)`**  
  Insert or update an entity (and its children, if applicable).

- **`delete(entity)`**  
  Delete an entity (and cascade delete its children).

- **`query(entity_type, alias)`**  
  Start building a query for the given entity type.

- **`raw(query, **params)`**  
  Create a raw SQL query.

- **Transaction Management**:  
  - **`begin()`**: Start a transaction.  
  - **`commit()`**: Commit the current transaction.  
  - **`rollback()`**: Roll back the current transaction.

### SessionEntityQuery

- **`filter(condition, **params)`**  
  Add a filtering condition to the query.

- **`join(target, alias, on, **params)`** / **`left_join(target, alias, on, **params)`**  
  Join another table (or entity) into the query.

- **`order_by(...)`**  
  Specify ordering for the query results.

- **`fetch(limit, offset)`**  
  Execute the query and retrieve a list of entities.

- **`fetch_one()`**  
  Retrieve a single entity.

- **`count()`**  
  Count the number of matching entities.

- **`paginate(first, after, last, before, offset)`**  
  Paginate the query results.

### AutoMappingBuilder

- **`@auto.mapped(**kwargs)`**  
  Decorator to register and configure an entity’s mapping. Configuration options include specifying the table name, primary key, parental key, field configurations, and child (relationship) configurations.

- **`auto.build()`**  
  Build and return all entity mappings based on the registered configurations.

---

## License

This project is licensed under the Apache License.
