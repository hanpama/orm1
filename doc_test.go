package orm1_test

import (
	"context"
	"database/sql"
	"log"

	"github.com/hanpama/orm1"
	_ "github.com/mattn/go-sqlite3"
)

// Domain entities for examples
type Order struct {
	ID         int64  `orm1:"primary,auto"`
	CustomerID int64
	Total      float64
	Items      []*OrderItem // Aggregate children
}

type OrderItem struct {
	ID       int64   `orm1:"primary,auto"`
	OrderID  int64   `orm1:"parental"` // Foreign key to parent
	Product  string
	Quantity int
	Price    float64
}

type Post struct {
	ID       int64  `orm1:"primary,auto"`
	Title    string
	Comments []*Comment
}

type Comment struct {
	ID     int64 `orm1:"primary,auto"`
	PostID int64 `orm1:"parental"`
	Text   string
}

type User struct {
	ID        int64 `orm1:"primary,auto"`
	Age       int
	CreatedAt string `orm1:"auto"`
}

// Example from Quick Start in README
func Example_quickStart() {
	// Setup test database
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	// Create tables
	db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		customer_id INTEGER,
		total REAL
	)`)
	db.Exec(`CREATE TABLE order_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		order_id INTEGER,
		product TEXT,
		quantity INTEGER,
		price REAL
	)`)

	// 2. Register entities and create factory
	factory := orm1.NewSessionFactory()
	factory.RegisterEntity(&Order{})
	factory.RegisterEntity(&OrderItem{})
	factory.SetDriver(orm1.NewSQLiteDriver(db))

	// Setup: Create an order with items
	setupSession := factory.CreateSession()
	setupSession.Begin(context.Background())
	order := &Order{
		CustomerID: 1,
		Total:      99.99,
		Items: []*OrderItem{
			{Product: "Gadget", Quantity: 2, Price: 49.99},
		},
	}
	setupSession.Save(context.Background(), order)
	setupSession.Commit(context.Background())

	// 3. Use in your application
	session := factory.CreateSession()
	ctx := context.Background()

	// Start a transaction
	if err := session.Begin(ctx); err != nil {
		log.Fatal(err)
	}
	// Defer rollback in case of error or panic
	defer session.Rollback(ctx)

	// Load an aggregate root; children are loaded automatically
	var loadedOrder *Order
	if err := session.Get(ctx, &loadedOrder, orm1.NewKey(order.ID)); err != nil {
		// ... handle "not found" or other errors
		return
	}

	// Modify the aggregate
	loadedOrder.Total = 159.99 // Update a value
	loadedOrder.Items = append(loadedOrder.Items, &OrderItem{ // Add a new child
		Product:  "Widget",
		Quantity: 5,
		Price:    9.99,
	})

	// Save handles all changes to the aggregate
	// (updates Order, inserts new OrderItem)
	if err := session.Save(ctx, loadedOrder); err != nil {
		// Rollback will be triggered by the defer
		log.Fatal(err)
	}

	// Commit the transaction
	if err := session.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	// Verify the result
	verifySession := factory.CreateSession()
	var verifyOrder *Order
	verifySession.Get(context.Background(), &verifyOrder, orm1.NewKey(order.ID))
	log.Printf("Order total: %.2f, Items: %d", verifyOrder.Total, len(verifyOrder.Items))

	// Output:
}

// Example from DDD Aggregate Support in README
func Example_aggregateSupport() {
	// Setup test database
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	db.Exec(`CREATE TABLE posts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT
	)`)
	db.Exec(`CREATE TABLE comments (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		post_id INTEGER,
		text TEXT
	)`)

	factory := orm1.NewSessionFactory()
	factory.RegisterEntity(&Post{}, orm1.WithTable("posts"))
	factory.RegisterEntity(&Comment{}, orm1.WithTable("comments"))
	factory.SetDriver(orm1.NewSQLiteDriver(db))

	// Setup: Create a post
	setupSession := factory.CreateSession()
	setupSession.Begin(context.Background())
	post := &Post{Title: "Hello World"}
	setupSession.Save(context.Background(), post)
	setupSession.Commit(context.Background())

	session := factory.CreateSession()
	ctx := context.Background()
	session.Begin(ctx)
	defer session.Rollback(ctx)

	// Load post with all comments
	var loadedPost *Post
	if err := session.Get(ctx, &loadedPost, orm1.NewKey(post.ID)); err != nil {
		log.Fatal(err)
	}

	// Add a new comment
	loadedPost.Comments = append(loadedPost.Comments, &Comment{Text: "Great!"})

	// Save cascades changes to the Comments slice
	if err := session.Save(ctx, loadedPost); err != nil {
		log.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	// Output:
}

// Example from Type-Safe Queries in README
func Example_typeSafeQueries() {
	// Setup test database
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		age INTEGER,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	db.Exec(`INSERT INTO users (age) VALUES (25), (30), (15)`)

	factory := orm1.NewSessionFactory()
	factory.RegisterEntity(&User{}, orm1.WithTable("users"))
	factory.SetDriver(orm1.NewSQLiteDriver(db))

	session := factory.CreateSession()
	ctx := context.Background()

	query := orm1.NewEntityQuery[User](session, "u")
	users, err := query.
		Where("u.age > ?", 18).
		OrderBy(query.Desc("u.created_at")).
		FetchAll(ctx)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Found %d users over 18", len(users))

	// Output:
}

// Example from Raw SQL in README
func Example_rawSQL() {
	// Setup test database
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()

	db.Exec(`CREATE TABLE transactions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		category TEXT,
		amount REAL
	)`)
	db.Exec(`INSERT INTO transactions (category, amount) VALUES
		('Food', 50.00),
		('Food', 30.00),
		('Transport', 20.00)`)

	factory := orm1.NewSessionFactory()
	factory.SetDriver(orm1.NewSQLiteDriver(db))

	session := factory.CreateSession()
	ctx := context.Background()

	type Result struct {
		Category string
		Total    float64
	}

	var results []*Result
	raw := orm1.NewRawQuery(session,
		"SELECT category, SUM(amount) as total FROM transactions GROUP BY category")
	raw.ScanAll(ctx, &results)

	for _, r := range results {
		log.Printf("%s: %.2f", r.Category, r.Total)
	}

	// Output:
}
