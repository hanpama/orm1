# Benchmark Results

This directory contains benchmark comparisons between orm1 and other popular Go ORMs.

## Disclaimer

**These benchmarks are synthetic and have inherent limitations:**

- Run on a single machine configuration (Apple M1 Pro)
- Use simplified scenarios that may not reflect production workloads
- PostgreSQL running locally, which doesn't account for network latency
- Results can vary based on data volume, query complexity, and database configuration
- Benchmark design choices may favor certain implementations

Use these results as rough guidance rather than definitive proof of superiority.

## Compared Libraries

- **orm1**: This library
- **GORM**: Popular full-featured ORM
- **Ent**: Facebook's entity framework with code generation
- **Bun**: Modern SQL-first ORM
- **sqlx**: Extensions to database/sql
- **Raw**: Hand-written SQL with database/sql (baseline)

## Test Scenarios

### Entity Models

**Simple Entity: User**
```go
type User struct {
    ID    int64  `orm1:"auto"`  // Auto-increment primary key
    Name  string
    Email string
    Age   int
}
```

**Aggregate Entity: Order (Parent-Child Relationship)**
```go
type Order struct {
    ID       int64          `orm1:"auto"`
    Customer string
    Total    float64
    Items    []*OrderItem   // Child collection
    Notes    []*OrderNote   // Child collection
}

type OrderItem struct {
    ID       int64 `orm1:"auto"`
    OrderID  int64 `orm1:"parental"`  // Foreign key to parent
    Product  string
    Quantity int
    Price    float64
}

type OrderNote struct {
    ID      int64  `orm1:"auto"`
    OrderID int64  `orm1:"parental"`
    Content string
}
```

### Benchmark Requirements

Each benchmark operation has specific initial state and target outcomes:

#### Simple_Insert
**Initial State:** Empty database
**Operation:** Create and save a single User entity
**Target Outcome:**
- 1 new User row inserted in database
- User.ID populated with auto-generated value

#### Simple_Select
**Initial State:** 100 pre-populated User rows (IDs 1-100)
**Operation:** Load a single User by primary key (rotating through IDs 1-100)
**Target Outcome:**
- Retrieved User entity with all fields populated
- ID matches requested key

#### Simple_Update
**Initial State:** 100 pre-populated User rows
**Operation:**
1. Load User by ID
2. Modify User.Age field (20 → 30)
3. Save updated User

**Target Outcome:**
- User.Age updated to 30 in database
- Other fields unchanged

#### Simple_BatchSelect
**Initial State:** 200 pre-populated User rows
**Operation:** Load 100 Users by primary keys (IDs 1-100) in a single batch operation
**Target Outcome:**
- 100 User entities retrieved
- All requested IDs present in results
- Results ordered in the same sequence as the requested IDs

#### Simple_ReadSlice
**Initial State:** 100 pre-populated User rows
**Operation:** Execute query `WHERE id > 0 LIMIT 100` to fetch all users
**Target Outcome:**
- 100 User entities loaded
- All entities match query criteria

#### Simple_Delete
**Initial State:** New User created each iteration (within b.StopTimer)
**Operation:** Delete the User entity
**Target Outcome:**
- User row removed from database
- No orphaned data

#### Aggregate_Insert
**Initial State:** Empty database
**Operation:** Create and save an Order with:
- 3 OrderItems
- 2 OrderNotes
- Within a transaction

**Target Outcome:**
- 1 Order row inserted
- 3 OrderItem rows inserted with correct OrderID foreign keys
- 2 OrderNote rows inserted with correct OrderID foreign keys
- All inserts atomic (transaction committed)

#### Aggregate_Select
**Initial State:** 10 pre-populated Orders, each with 3 Items and 2 Notes
**Operation:** Load a single Order by ID (rotating through IDs 1-10)
**Target Outcome:**
- Order entity loaded with all fields
- Order.Items collection populated with 3 OrderItem entities
- Order.Notes collection populated with 2 OrderNote entities
- Cascade loading complete in single operation

#### Aggregate_Update
**Initial State:** 10 pre-populated Orders with 3 Items each
**Operation:**
1. Load Order by ID
2. Modify Order.Total (299.97 → 399.96)
3. Modify Item[0].Price (99.99 → 149.99)
4. Modify Item[1].Price (99.99 → 124.99)
5. Modify Item[2].Price (99.99 → 124.98)
6. Save updated Order within transaction

**Target Outcome:**
- Order.Total updated to 399.96
- OrderItem[0].Price updated to 149.99
- OrderItem[1].Price updated to 124.99
- OrderItem[2].Price updated to 124.98
- Updates atomic (transaction committed)
- Cascade update handles parent and children

#### Aggregate_BatchSelect
**Initial State:** 100 pre-populated Orders, each with 3 Items and 2 Notes
**Operation:** Load 10 Orders by IDs (1-10) with cascade loading
**Target Outcome:**
- 10 Order entities retrieved
- Each Order has 3 loaded OrderItems
- Each Order has 2 loaded OrderNotes
- Results ordered in the same sequence as the requested IDs
- Efficient batch + cascade loading

#### Aggregate_ReadSlice
**Initial State:** 100 pre-populated Orders with 3 Items each
**Operation:** Execute query `WHERE id > 0 LIMIT 100` with cascade loading
**Target Outcome:**
- 100 Order entities loaded
- Each Order has 3 loaded OrderItems (300 items total)
- Cascade loading for all children

#### Aggregate_Delete
**Initial State:** New Order with 3 Items created each iteration (within b.StopTimer)
**Operation:** Delete Order entity with cascade delete within transaction
**Target Outcome:**
- Order row deleted
- All 3 OrderItem rows deleted (cascade)
- Atomic deletion (transaction committed)
- No orphaned child records

## Sample Results

See `SAMPLE_RESULTS.txt` for detailed output from one benchmark run.

### Key Observations

- orm1 generally performs between Raw SQL and other ORMs
- Memory allocations are competitive, often lower than full-featured ORMs
- Aggregate operations show the benefits of identity map and cascade logic

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./tests

# Run specific pattern
go test -bench=ORM1 -benchmem ./tests

# Generate comparison report
go run main.go
```

## Environment

- Machine: Apple M1 Pro (8-core)
- OS: macOS
- Go: 1.25.3
- PostgreSQL: 14+ (via Docker)
