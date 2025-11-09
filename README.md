# orm1

A lightweight, high-performance ORM for Go, designed to cleanly persist plain Go structs and support Domain-Driven Design.

## Why orm1?

`orm1` is built for developers who need the productivity of an ORM without sacrificing performance or architectural cleanliness. It provides a simple, powerful persistence engine for your domain models.

  * **Supports DDD Aggregates:** `orm1` is designed to support the Aggregate pattern. It naturally handles loading and saving parent-child relationships, allowing you to focus on your business logic, not on persistence coordination.
  * **Minimalist & Predictable:** It's a persistence engine for plain Go structs. There is no complex boilerplate or code generation. Its behavior is explicit and its overhead is minimal and predictable.
  * **High Performance:** `orm1` is designed to be fast. Its features add the minimum overhead necessary for their function. This has been verified by benchmarks against major Go libraries (GORM, Ent, Bun, pgx, sqlx), proving `orm1` to be a highly performant and efficient choice.

## Philosophy

`orm1` lets you focus on your domain logic by providing straightforward persistence for your Go data structures.

The library is designed for:

  - **Clean domain models**: Your business entities are just plain Go structs.
  - **DDD Aggregates**: Parent-child relationships are a core concept.
  - **Predictable performance**: A lightweight design with minimal, predictable overhead.
  - **Simplicity**: No external dependencies in the core package.

## Quick Start

```go
import "github.com/hanpama/orm1"

// 1. Define your domain entities
type Order struct {
    ID         int64  `orm1:"auto"`  // Auto-increment primary key
    CustomerID int64
    Total      float64
    Items      []*OrderItem  // Aggregate children
}

type OrderItem struct {
    ID       int64   `orm1:"auto"`  // Auto-increment primary key
    OrderID  int64   `orm1:"parental"`  // Foreign key to parent
    Product  string
    Quantity int
    Price    float64
}

// 2. Register entities and create factory
factory := orm1.NewSessionFactory()
factory.RegisterEntity(&Order{})
factory.RegisterEntity(&OrderItem{})
factory.SetDriver(orm1.NewPostgreSQLDriver(db))

// 3. Use in your application
session := factory.CreateSession()

// Start a transaction
if err := session.Begin(ctx); err != nil {
    log.Fatal(err)
}
// Defer rollback in case of error or panic
defer session.Rollback(ctx)

// Load an aggregate root; children are loaded automatically
var order *Order
if err := session.Get(ctx, &order, orm1.NewKey(42)); err != nil {
    // ... handle "not found" or other errors
    return
}

// Modify the aggregate
order.Total = 159.99 // Update a value
order.Items = append(order.Items, &OrderItem{ // Add a new child
    Product:  "Widget",
    Quantity: 5,
    Price:    9.99,
})

// Save handles all changes to the aggregate
// (updates Order, inserts new OrderItem)
if err := session.Save(ctx, order); err != nil {
    // Rollback will be triggered by the defer
    log.Fatal(err)
}

// Commit the transaction
if err := session.Commit(ctx); err != nil {
    log.Fatal(err)
}
```

## Core Features

### DDD Aggregate Support

`orm1` treats aggregates as a first-class concept. An aggregate defines a transactional consistency boundary—all entities within it are loaded and saved together, maintaining invariants across the entire structure.

When you call `session.Get()` on an aggregate root, all its children are loaded. When you call `session.Save()`, `orm1` cascades changes throughout the aggregate, handling inserts, updates, and deletes automatically.

```go
// The Order aggregate represents a single transaction boundary.
// Order totals must stay consistent with line items—they belong in one aggregate.
type Order struct {
    ID         int64  `orm1:"auto"`
    CustomerID int64
    Status     string  // "pending", "paid", "shipped"
    Total      float64
    Items      []*OrderItem
}

type OrderItem struct {
    ID        int64   `orm1:"auto"`
    OrderID   int64   `orm1:"parental"`
    ProductID int64   // Reference to Product aggregate (different boundary)
    Quantity  int
    Price     float64  // Price at time of order (immutable)
}

// Load the entire order aggregate
var order *Order
if err := session.Get(ctx, &order, orm1.NewKey(123)); err != nil {
    log.Fatal(err)
}

// Business logic: Add an item and recalculate total
order.Items = append(order.Items, &OrderItem{
    ProductID: 456,
    Quantity:  2,
    Price:     29.99,
})

// Maintain invariant: total must equal sum of items
order.Total = 0
for _, item := range order.Items {
    order.Total += item.Price * float64(item.Quantity)
}

// Save persists all changes atomically
// (updates Order, inserts new OrderItem)
if err := session.Save(ctx, order); err != nil {
    log.Fatal(err)
}
```

### Type-Safe Queries

Use `EntityQuery` for complex queries with compile-time type safety:

```go
query := orm1.NewEntityQuery[User](session, "u")
users, err := query.
    Where("u.age > ?", 18).
    OrderBy(query.Desc("u.created_at")).
    FetchAll(ctx)
```

### Raw SQL Escape Hatch

When you need full control, raw SQL is straightforward:

```go
type Result struct {
    Category string
    Total    float64
}

var results []*Result
raw := orm1.NewRawQuery(session,
    "SELECT category, SUM(amount) as total FROM transactions GROUP BY category")
raw.ScanAll(ctx, &results)
```

## Entity Mapping

`orm1` uses struct tags and simple conventions to map structs to tables:

```go
type User struct {
    ID        int64     `orm1:"auto"`             // Auto-managed (e.g., AUTOINCREMENT)
    Email     string    `orm1:"column:user_email"` // Custom column name
    CreatedAt time.Time `orm1:"auto"`             // DB-managed (e.g., DEFAULT NOW())
    UpdatedAt time.Time // Updated by application
    Internal  string    `orm1:"ignore"`           // Not persisted
}
```

**Mapping Rules:**

  - A field named `ID` is the primary key by default (no tag needed).
  - Use `orm1:"auto"` for auto-increment/database-managed columns (e.g., AUTOINCREMENT, DEFAULT NOW()).
  - Slice/pointer fields of registered types are treated as children.
  - Field names are automatically mapped to `snake_case` columns.
  - Parental (foreign) keys **must** be explicitly tagged with `orm1:"parental"`.

See `go doc orm1.SessionFactory.RegisterEntity` for a complete tag reference.

## Performance

`orm1` is designed with performance as a primary goal. It adds **minimal, predictable overhead** on top of your database operations, ensuring your application remains fast.

This has been **verified by benchmarks** against other major Go database libraries (including GORM, Ent, Bun, and pgx), proving `orm1` to be a highly efficient and performant choice for data access.

See the [sample results](/benchmark/SAMPLE_RESULTS.txt) for detailed performance comparisons.

## Documentation

Complete documentation is available via `go doc`:

```bash
go doc -all github.com/hanpama/orm1
```

Or view online at [pkg.go.dev/github.com/hanpama/orm1](https://pkg.go.dev/github.com/hanpama/orm1)

## Installation

```bash
go get github.com/hanpama/orm1
```

Requires Go 1.18 or later (uses generics).

## License

MIT
