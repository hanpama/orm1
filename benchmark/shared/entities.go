package shared

// Simple entities for basic CRUD
type User struct {
	ID    int64 `orm1:"auto"`
	Name  string
	Email string
	Age   int
}

// Aggregate entities (parent-child relationship)
type Order struct {
	ID       int64          `orm1:"auto"`
	Customer string
	Total    float64
	Items    []*OrderItem
	Notes    []*OrderNote
}

type OrderItem struct {
	ID       int64 `orm1:"auto"`
	OrderID  int64 `orm1:"parental"`
	Product  string
	Quantity int
	Price    float64
}

type OrderNote struct {
	ID      int64  `orm1:"auto"`
	OrderID int64  `orm1:"parental"`
	Content string
}
