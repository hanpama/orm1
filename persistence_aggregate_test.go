package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
)

// Purchase aggregate root
type Purchase struct {
	ID         int64
	CustomerID *int64
	Price      float64
	LineItems  []*PurchaseLineItem
	Billings   []*PurchaseBilling
	Withdrawal *PurchaseWithdrawal
}

// PurchaseLineItem - simplified with single ID primary key
type PurchaseLineItem struct {
	ID         int64
	PurchaseID int64 `orm1:"parental"`
	ItemIndex  int64
	Product    *int64 // renamed from ProductID to avoid confusion with parental keys
	Quantity   int64
}

// PurchaseBilling with nested children
type PurchaseBilling struct {
	ID            int64
	PurchaseID    int64 `orm1:"parental"`
	PaymentMethod string
	Amount        float64
	BillingTime   int64 // unix timestamp for simplicity with SQLite
	Attachments   []*PurchaseBillingAttachment
	Payment       *PurchaseBillingPayment
}

type PurchaseBillingAttachment struct {
	ID                int64
	PurchaseBillingID int64 `orm1:"parental"`
	MediaURI          string
}

type PurchaseBillingPayment struct {
	PurchaseBillingID int64 `orm1:"primary,parental"`
	PaymentTime       int64 // unix timestamp
	Amount            float64
}

// PurchaseWithdrawal with nullable fields
type PurchaseWithdrawal struct {
	ID          int64
	PurchaseID  int64 `orm1:"parental"`
	CreatedAt   *int64 // unix timestamp
	Remark      *string
	Attachments []*PurchaseWithdrawalAttachment
}

type PurchaseWithdrawalAttachment struct {
	ID                   int64
	PurchaseWithdrawalID int64 `orm1:"parental"`
	MediaURI             string
}

func setupAggregateTestDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend,
		"purchase_billing_payment",
		"purchase_billing_attachment",
		"purchase_billing",
		"purchase_withdrawal_attachment",
		"purchase_withdrawal",
		"purchase_line_item",
		"purchase",
	)

	setup.ExecSchema(t, backend, `
		CREATE TABLE purchase (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			price REAL NOT NULL
		);

		CREATE TABLE purchase_line_item (
			id INTEGER PRIMARY KEY,
			purchase_id INTEGER NOT NULL,
			item_index INTEGER NOT NULL,
			product INTEGER,
			quantity INTEGER NOT NULL,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_billing (
			id INTEGER PRIMARY KEY,
			purchase_id INTEGER NOT NULL,
			payment_method TEXT NOT NULL,
			amount REAL NOT NULL,
			billing_time INTEGER NOT NULL,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_billing_attachment (
			id INTEGER PRIMARY KEY,
			purchase_billing_id INTEGER NOT NULL,
			media_uri TEXT NOT NULL,
			FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
		);

		CREATE TABLE purchase_billing_payment (
			purchase_billing_id INTEGER PRIMARY KEY,
			payment_time INTEGER NOT NULL,
			amount REAL NOT NULL,
			FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
		);

		CREATE TABLE purchase_withdrawal (
			id INTEGER PRIMARY KEY,
			purchase_id INTEGER NOT NULL UNIQUE,
			created_at INTEGER,
			remark TEXT,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_withdrawal_attachment (
			id INTEGER PRIMARY KEY,
			purchase_withdrawal_id INTEGER NOT NULL,
			media_uri TEXT NOT NULL,
			FOREIGN KEY (purchase_withdrawal_id) REFERENCES purchase_withdrawal(id)
		);
	`, `
		CREATE TABLE purchase (
			id BIGINT PRIMARY KEY,
			customer_id BIGINT,
			price DOUBLE PRECISION NOT NULL
		);

		CREATE TABLE purchase_line_item (
			id BIGINT PRIMARY KEY,
			purchase_id BIGINT NOT NULL,
			item_index BIGINT NOT NULL,
			product BIGINT,
			quantity BIGINT NOT NULL,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_billing (
			id BIGINT PRIMARY KEY,
			purchase_id BIGINT NOT NULL,
			payment_method TEXT NOT NULL,
			amount DOUBLE PRECISION NOT NULL,
			billing_time BIGINT NOT NULL,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_billing_attachment (
			id BIGINT PRIMARY KEY,
			purchase_billing_id BIGINT NOT NULL,
			media_uri TEXT NOT NULL,
			FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
		);

		CREATE TABLE purchase_billing_payment (
			purchase_billing_id BIGINT PRIMARY KEY,
			payment_time BIGINT NOT NULL,
			amount DOUBLE PRECISION NOT NULL,
			FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
		);

		CREATE TABLE purchase_withdrawal (
			id BIGINT PRIMARY KEY,
			purchase_id BIGINT NOT NULL UNIQUE,
			created_at BIGINT,
			remark TEXT,
			FOREIGN KEY (purchase_id) REFERENCES purchase(id)
		);

		CREATE TABLE purchase_withdrawal_attachment (
			id BIGINT PRIMARY KEY,
			purchase_withdrawal_id BIGINT NOT NULL,
			media_uri TEXT NOT NULL,
			FOREIGN KEY (purchase_withdrawal_id) REFERENCES purchase_withdrawal(id)
		);
	`)

	return setup
}

func createAggregateTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()

	// Register all entities
	factory.RegisterEntity(&Purchase{}, orm1.WithTable("purchase"))
	factory.RegisterEntity(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
	factory.RegisterEntity(&PurchaseBilling{}, orm1.WithTable("purchase_billing"))
	factory.RegisterEntity(&PurchaseBillingAttachment{}, orm1.WithTable("purchase_billing_attachment"))
	factory.RegisterEntity(&PurchaseBillingPayment{}, orm1.WithTable("purchase_billing_payment"))
	factory.RegisterEntity(&PurchaseWithdrawal{}, orm1.WithTable("purchase_withdrawal"))
	factory.RegisterEntity(&PurchaseWithdrawalAttachment{}, orm1.WithTable("purchase_withdrawal_attachment"))

	return factory.CreateSession()
}

func ptr[T any](v T) *T {
	return &v
}

func TestAggregateGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);

				INSERT INTO purchase_line_item (id, purchase_id, item_index, product, quantity) VALUES (1, 1, 1, 1, 2);
				INSERT INTO purchase_line_item (id, purchase_id, item_index, product, quantity) VALUES (2, 1, 2, 2, 1);

				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (1, 1, 'credit_card', 100.00, 1609502400);

				INSERT INTO purchase_billing_attachment (id, purchase_billing_id, media_uri)
				VALUES (1, 1, 'http://example.com/1');
				INSERT INTO purchase_billing_attachment (id, purchase_billing_id, media_uri)
				VALUES (2, 1, 'http://example.com/2');

				INSERT INTO purchase_billing_payment (purchase_billing_id, payment_time, amount)
				VALUES (1, 1609502400, 100.00);
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Test Get with full aggregate
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if purchase == nil {
				t.Fatal("purchase is nil")
			}

			// Verify root entity
			if purchase.ID != 1 {
				t.Errorf("expected ID 1, got %d", purchase.ID)
			}
			if purchase.CustomerID == nil || *purchase.CustomerID != 1 {
				t.Errorf("expected CustomerID 1, got %v", purchase.CustomerID)
			}
			if purchase.Price != 100.00 {
				t.Errorf("expected Price 100.00, got %f", purchase.Price)
			}

			// Verify line items
			if len(purchase.LineItems) != 2 {
				t.Fatalf("expected 2 line items, got %d", len(purchase.LineItems))
			}

			// Verify billings
			if len(purchase.Billings) != 1 {
				t.Fatalf("expected 1 billing, got %d", len(purchase.Billings))
			}

			billing := purchase.Billings[0]
			if billing.PaymentMethod != "credit_card" {
				t.Errorf("expected payment_method 'credit_card', got '%s'", billing.PaymentMethod)
			}

			// Verify billing attachments (grandchildren)
			if len(billing.Attachments) != 2 {
				t.Fatalf("expected 2 billing attachments, got %d", len(billing.Attachments))
			}

			// Verify billing payment (singular grandchild)
			if billing.Payment == nil {
				t.Fatal("expected billing payment, got nil")
			}
			if billing.Payment.Amount != 100.00 {
				t.Errorf("expected payment amount 100.00, got %f", billing.Payment.Amount)
			}

			// Verify withdrawal is nil
			if purchase.Withdrawal != nil {
				t.Error("expected withdrawal to be nil")
			}
		})
	}
}

func TestAggregateGetWithSingularChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data with withdrawal
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (2, 2, 200.00);

				INSERT INTO purchase_line_item (id, purchase_id, item_index, product, quantity) VALUES (3, 2, 1, 3, 3);

				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (2, 2, 'credit_card', 200.00, 1609588800);

				INSERT INTO purchase_withdrawal (id, purchase_id, created_at, remark)
				VALUES (1, 2, 1609675200, 'Withdrawal remark');

				INSERT INTO purchase_withdrawal_attachment (id, purchase_withdrawal_id, media_uri)
				VALUES (1, 1, 'http://example.com/5');
				INSERT INTO purchase_withdrawal_attachment (id, purchase_withdrawal_id, media_uri)
				VALUES (2, 1, 'http://example.com/6');
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Test Get
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(2)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			// Verify withdrawal (singular child)
			if purchase.Withdrawal == nil {
				t.Fatal("expected withdrawal, got nil")
			}
			if purchase.Withdrawal.Remark == nil || *purchase.Withdrawal.Remark != "Withdrawal remark" {
				t.Errorf("expected remark 'Withdrawal remark', got %v", purchase.Withdrawal.Remark)
			}

			// Verify withdrawal attachments (grandchildren of singular child)
			if len(purchase.Withdrawal.Attachments) != 2 {
				t.Fatalf("expected 2 withdrawal attachments, got %d", len(purchase.Withdrawal.Attachments))
			}
		})
	}
}

func TestAggregateSaveAddingPluralChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);
				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (1, 1, 'credit_card', 100.00, 1609502400);
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			// Add new billing
			purchase.Billings = append(purchase.Billings, &PurchaseBilling{
				ID:            2,
				PurchaseID:    1,
				PaymentMethod: "paypal",
				Amount:        50.00,
				BillingTime:   1609761600, // 2021-01-03
				Attachments:   []*PurchaseBillingAttachment{},
				Payment:       nil,
			})

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if len(reloaded.Billings) != 2 {
				t.Fatalf("expected 2 billings, got %d", len(reloaded.Billings))
			}
		})
	}
}

func TestAggregateSaveRemovingPluralChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);
				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (1, 1, 'credit_card', 100.00, 1609502400);
				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (2, 1, 'paypal', 50.00, 1609588800);
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if len(purchase.Billings) != 2 {
				t.Fatalf("expected 2 billings initially, got %d", len(purchase.Billings))
			}

			// Remove one billing
			purchase.Billings = purchase.Billings[:1]

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify directly in database
			var count int
			err = setup.DB.QueryRow("SELECT COUNT(*) FROM purchase_billing WHERE purchase_id = 1").Scan(&count)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			if count != 1 {
				t.Fatalf("expected 1 billing in database after removal, got %d", count)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if len(reloaded.Billings) != 1 {
				t.Fatalf("expected 1 billing after removal, got %d", len(reloaded.Billings))
			}
		})
	}
}

func TestAggregateSaveAddingSingularChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data without withdrawal
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if purchase.Withdrawal != nil {
				t.Fatal("expected withdrawal to be nil initially")
			}

			// Add withdrawal
			createdAt := int64(1609848000) // 2021-01-04
			purchase.Withdrawal = &PurchaseWithdrawal{
				ID:          1,
				PurchaseID:  1,
				CreatedAt:   &createdAt,
				Remark:      ptr("New withdrawal"),
				Attachments: []*PurchaseWithdrawalAttachment{},
			}

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if reloaded.Withdrawal == nil {
				t.Fatal("expected withdrawal after adding")
			}
			if reloaded.Withdrawal.Remark == nil || *reloaded.Withdrawal.Remark != "New withdrawal" {
				t.Errorf("expected remark 'New withdrawal', got %v", reloaded.Withdrawal.Remark)
			}
		})
	}
}

func TestAggregateSaveRemovingSingularChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data with withdrawal
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (2, 2, 200.00);
				INSERT INTO purchase_withdrawal (id, purchase_id, created_at, remark)
				VALUES (1, 2, 1609675200, 'Withdrawal remark');
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(2)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if purchase.Withdrawal == nil {
				t.Fatal("expected withdrawal initially")
			}

			// Remove withdrawal
			purchase.Withdrawal = nil

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(2)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if reloaded.Withdrawal != nil {
				t.Error("expected withdrawal to be nil after removal")
			}
		})
	}
}

func TestAggregateDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);
				INSERT INTO purchase_line_item (id, purchase_id, item_index, product, quantity) VALUES (1, 1, 1, 1, 2);
				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (1, 1, 'credit_card', 100.00, 1609502400);
				INSERT INTO purchase_billing_attachment (id, purchase_billing_id, media_uri)
				VALUES (1, 1, 'http://example.com/1');
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			// Delete
			err = session.Delete(ctx, purchase)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify deleted
			var reloaded *Purchase
			err = session.Get(ctx, &reloaded, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if reloaded != nil {
				t.Error("expected entity to be deleted")
			}

			// Verify children are also deleted
			var count int
			err = setup.DB.QueryRow("SELECT COUNT(*) FROM purchase_line_item WHERE purchase_id = 1").Scan(&count)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 line items, got %d", count)
			}

			err = setup.DB.QueryRow("SELECT COUNT(*) FROM purchase_billing WHERE purchase_id = 1").Scan(&count)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 billings, got %d", count)
			}

			err = setup.DB.QueryRow("SELECT COUNT(*) FROM purchase_billing_attachment WHERE purchase_billing_id = 1").Scan(&count)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 billing attachments, got %d", count)
			}
		})
	}
}

func TestAggregateDeleteUnpersistedRoot(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Create session
			session := createAggregateTestSession(setup)

			// Create unpersisted entity
			purchase := &Purchase{
				ID:         3,
				CustomerID: ptr(int64(3)),
				Price:      300.00,
				LineItems:  []*PurchaseLineItem{},
				Billings:   []*PurchaseBilling{},
				Withdrawal: nil,
			}

			// Verify not in database
			var found *Purchase
			err := session.Get(ctx, &found, orm1.NewKey(int64(3)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if found != nil {
				t.Fatal("expected entity to not exist")
			}

			// Delete unpersisted entity (should be no-op)
			err = session.Delete(ctx, purchase)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify still not in database
			err = session.Get(ctx, &found, orm1.NewKey(int64(3)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if found != nil {
				t.Error("expected entity to still not exist")
			}
		})
	}
}

func TestAggregateDeleteUnpersistedChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Create session
			session := createAggregateTestSession(setup)

			// Create unpersisted entity with unpersisted child
			createdAt := int64(1609848000)
			purchase := &Purchase{
				ID:         3,
				CustomerID: ptr(int64(3)),
				Price:      300.00,
				LineItems:  []*PurchaseLineItem{},
				Billings:   []*PurchaseBilling{},
				Withdrawal: &PurchaseWithdrawal{
					ID:         3,
					PurchaseID: 3,
					CreatedAt:  &createdAt,
					Remark:     ptr("Unpersisted withdrawal"),
					Attachments: []*PurchaseWithdrawalAttachment{
						{ID: 3, PurchaseWithdrawalID: 3, MediaURI: "http://example.com/9"},
						{ID: 4, PurchaseWithdrawalID: 3, MediaURI: "http://example.com/10"},
					},
				},
			}

			// Verify not in database
			var found *Purchase
			err := session.Get(ctx, &found, orm1.NewKey(int64(3)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if found != nil {
				t.Fatal("expected entity to not exist")
			}

			// Delete unpersisted entity with children (should be no-op)
			err = session.Delete(ctx, purchase)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify still not in database
			err = session.Get(ctx, &found, orm1.NewKey(int64(3)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if found != nil {
				t.Error("expected entity to still not exist")
			}
		})
	}
}

func TestAggregateSaveEditingPluralChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data with payment
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (1, 1, 100.00);
				INSERT INTO purchase_billing (id, purchase_id, payment_method, amount, billing_time)
				VALUES (1, 1, 'credit_card', 100.00, 1609502400);
				INSERT INTO purchase_billing_payment (purchase_billing_id, payment_time, amount)
				VALUES (1, 1609502400, 100.00);
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if purchase.Billings[0].Payment == nil {
				t.Fatal("expected payment to exist")
			}

			// Edit grandchild (payment)
			purchase.Billings[0].Payment.Amount = 200.00

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(1)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if reloaded.Billings[0].Payment == nil {
				t.Fatal("expected payment after save")
			}
			if reloaded.Billings[0].Payment.Amount != 200.00 {
				t.Errorf("expected payment amount 200.00, got %f", reloaded.Billings[0].Payment.Amount)
			}
		})
	}
}

func TestAggregateSaveEditingSingularChild(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAggregateTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data with withdrawal
			_, err := setup.DB.Exec(`
				INSERT INTO purchase (id, customer_id, price) VALUES (2, 2, 200.00);
				INSERT INTO purchase_withdrawal (id, purchase_id, created_at, remark)
				VALUES (1, 2, 1609675200, 'Original remark');
			`)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}

			// Create session
			session := createAggregateTestSession(setup)

			// Get entity
			var purchase *Purchase
			err = session.Get(ctx, &purchase, orm1.NewKey(int64(2)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if purchase.Withdrawal == nil {
				t.Fatal("expected withdrawal to exist")
			}

			// Edit singular child
			purchase.Withdrawal.Remark = ptr("Updated remark")

			// Save
			err = session.Save(ctx, purchase)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify in new session
			session2 := createAggregateTestSession(setup)

			var reloaded *Purchase
			err = session2.Get(ctx, &reloaded, orm1.NewKey(int64(2)))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if reloaded.Withdrawal == nil {
				t.Fatal("expected withdrawal after save")
			}
			if reloaded.Withdrawal.Remark == nil || *reloaded.Withdrawal.Remark != "Updated remark" {
				t.Errorf("expected remark 'Updated remark', got %v", reloaded.Withdrawal.Remark)
			}
		})
	}
}
