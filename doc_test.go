package orm1_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
	_ "github.com/mattn/go-sqlite3"
)

// Domain entities for examples
type Order struct {
	ID         int64 `orm1:"auto"`
	CustomerID int64
	Total      float64
	Items      []*OrderItem // Aggregate children
}

type OrderItem struct {
	ID       int64   `orm1:"auto"`
	OrderID  int64   `orm1:"parental"` // Foreign key to parent
	Product  string
	Quantity int
	Price    float64
}

type Post struct {
	ID       int64 `orm1:"auto"`
	Title    string
	Comments []*Comment
}

type Comment struct {
	ID     int64 `orm1:"auto"`
	PostID int64 `orm1:"parental"`
	Text   string
}

type User struct {
	ID        int64  `orm1:"auto"`
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
	factory := orm1.NewSessionFactoryWithDriver(driver.NewSQLite(db))
	factory.RegisterEntity(&Order{}, orm1.WithTable("orders"))
	factory.RegisterEntity(&OrderItem{}, orm1.WithTable("order_items"))

	// Setup: Create an order with items
	setupSession := factory.CreateSession()
	setupTx, _ := setupSession.Begin(context.Background(), nil)
	order := &Order{
		CustomerID: 1,
		Total:      99.99,
		Items: []*OrderItem{
			{Product: "Gadget", Quantity: 2, Price: 49.99},
		},
	}
	setupSession.Save(context.Background(), order)
	setupTx.Commit(context.Background())

	// 3. Use in your application
	session := factory.CreateSession()
	ctx := context.Background()

	// Start a transaction
	tx, err := session.Begin(ctx, nil); if err != nil {
		return
	}
	// Defer rollback in case of error or panic
	defer tx.Rollback(ctx)

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
		return
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return
	}

	// Verify the result
	verifySession := factory.CreateSession()
	var verifyOrder *Order
	verifySession.Get(context.Background(), &verifyOrder, orm1.NewKey(order.ID))
	fmt.Printf("Order total: %.2f, Items: %d\n", verifyOrder.Total, len(verifyOrder.Items))

	// Output:
	// Order total: 159.99, Items: 2
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

	factory := orm1.NewSessionFactoryWithDriver(driver.NewSQLite(db))
	factory.RegisterEntity(&Post{}, orm1.WithTable("posts"))
	factory.RegisterEntity(&Comment{}, orm1.WithTable("comments"))

	// Setup: Create a post
	setupSession := factory.CreateSession()
	setupTx, _ := setupSession.Begin(context.Background(), nil)
	post := &Post{Title: "Hello World"}
	setupSession.Save(context.Background(), post)
	setupTx.Commit(context.Background())

	session := factory.CreateSession()
	ctx := context.Background()
	tx, _ := session.Begin(ctx, nil)
	defer tx.Rollback(ctx)

	// Load post with all comments
	var loadedPost *Post
	if err := session.Get(ctx, &loadedPost, orm1.NewKey(post.ID)); err != nil {
		return
	}

	// Add a new comment
	loadedPost.Comments = append(loadedPost.Comments, &Comment{Text: "Great!"})

	// Save cascades changes to the Comments slice
	if err := session.Save(ctx, loadedPost); err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		return
	}

	// Verify
	verifySession := factory.CreateSession()
	var verifyPost *Post
	verifySession.Get(context.Background(), &verifyPost, orm1.NewKey(post.ID))
	fmt.Printf("Post has %d comment(s)\n", len(verifyPost.Comments))

	// Output:
	// Post has 1 comment(s)
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

	factory := orm1.NewSessionFactoryWithDriver(driver.NewSQLite(db))
	factory.RegisterEntity(&User{}, orm1.WithTable("users"))

	session := factory.CreateSession()
	ctx := context.Background()

	query := orm1.NewEntityQuery[User](session, "u")
	users, err := query.
		Where("u.age > ?", 18).
		OrderBy(query.Desc("u.created_at")).
		FetchAll(ctx)

	if err != nil {
		return
	}

	fmt.Printf("Found %d users over 18\n", len(users))

	// Output:
	// Found 2 users over 18
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

	factory := orm1.NewSessionFactoryWithDriver(driver.NewSQLite(db))

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
		fmt.Printf("%s: %.2f\n", r.Category, r.Total)
	}

	// Output:
	// Food: 80.00
	// Transport: 20.00
}
