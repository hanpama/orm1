//go:build e2e
// +build e2e

package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1"
)

// Helper function to insert purchase with line items test data
func insertPurchaseGroupByTestData(ctx context.Context, session *orm1.Session) error {
	// 4 purchases with varying numbers of line items
	// purchase_gb1: 3 items, purchase_gb2: 2 items, purchase_gb3: 1 item, purchase_gb4: 0 items
	purchases := []*Purchase{
		{ID: "purchase_gb1", CustomerID: strPtr("customer1"), Price: 100.0},
		{ID: "purchase_gb2", CustomerID: strPtr("customer2"), Price: 200.0},
		{ID: "purchase_gb3", CustomerID: strPtr("customer3"), Price: 300.0},
		{ID: "purchase_gb4", CustomerID: strPtr("customer4"), Price: 400.0},
	}

	lineItems := []*PurchaseLineItem{
		{ID: "lineitem_gb1", PurchaseID: "purchase_gb1", ItemIndex: 0, Product: strPtr("Product A"), Quantity: 1},
		{ID: "lineitem_gb2", PurchaseID: "purchase_gb1", ItemIndex: 1, Product: strPtr("Product B"), Quantity: 2},
		{ID: "lineitem_gb3", PurchaseID: "purchase_gb1", ItemIndex: 2, Product: strPtr("Product C"), Quantity: 3},
		{ID: "lineitem_gb4", PurchaseID: "purchase_gb2", ItemIndex: 0, Product: strPtr("Product D"), Quantity: 1},
		{ID: "lineitem_gb5", PurchaseID: "purchase_gb2", ItemIndex: 1, Product: strPtr("Product E"), Quantity: 2},
		{ID: "lineitem_gb6", PurchaseID: "purchase_gb3", ItemIndex: 0, Product: strPtr("Product F"), Quantity: 1},
	}

	for _, p := range purchases {
		if err := session.Save(ctx, p); err != nil {
			return err
		}
	}

	for _, li := range lineItems {
		if err := session.Save(ctx, li); err != nil {
			return err
		}
	}

	return nil
}

func cleanupPurchaseGroupByTestData(ctx context.Context, session *orm1.Session) {
	// Delete in reverse dependency order
	lineItemIDs := []string{"lineitem_gb1", "lineitem_gb2", "lineitem_gb3", "lineitem_gb4", "lineitem_gb5", "lineitem_gb6"}
	for _, id := range lineItemIDs {
		rawQuery := orm1.NewRawQuery(session, "DELETE FROM purchase_line_item WHERE id = ?", id)
		rawQuery.Exec(ctx)
	}

	purchaseIDs := []string{"purchase_gb1", "purchase_gb2", "purchase_gb3", "purchase_gb4"}
	for _, id := range purchaseIDs {
		rawQuery := orm1.NewRawQuery(session, "DELETE FROM purchase WHERE id = ?", id)
		rawQuery.Exec(ctx)
	}
}

// GroupBy Pagination Tests

func TestPaginationGroupByForward(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertPurchaseGroupByTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupPurchaseGroupByTestData(ctx, session)

			// Query with GROUP BY and HAVING
			// This tests pagination.go:78-83 where cursor filters go to HAVING clause
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(pli.id) >= ?", 2) // Filters to purchase_gb1, purchase_gb2
			query.OrderBy(query.Asc("p.id"))

			// First page: first 1 item
			first := 1
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb1
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb1")},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor purchase_gb1, get 1 item
			// CRITICAL: Cursor filter "p.id > 'purchase_gb1'" MUST go to HAVING clause
			// because we have GROUP BY. This exercises pagination.go:78-83
			page2, err := query.Paginate(ctx, page1.Cursors[0], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb2
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb2")},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationGroupByBackward(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertPurchaseGroupByTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupPurchaseGroupByTestData(ctx, session)

			// Query with GROUP BY and HAVING
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(pli.id) >= ?", 2) // Filters to purchase_gb1, purchase_gb2
			query.OrderBy(query.Asc("p.id"))

			// Backward pagination: last 1 item
			last := 1
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb2 (last item)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb2")},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before cursor purchase_gb2, get 1 item
			// CRITICAL: Cursor filter "p.id < 'purchase_gb2'" MUST go to HAVING clause
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb1
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb1")},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationGroupByMultiplePages(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert 6 purchases with 2+ line items each
			purchases := []*Purchase{
				{ID: "purchase_mp1", CustomerID: strPtr("c1"), Price: 100.0},
				{ID: "purchase_mp2", CustomerID: strPtr("c2"), Price: 200.0},
				{ID: "purchase_mp3", CustomerID: strPtr("c3"), Price: 300.0},
				{ID: "purchase_mp4", CustomerID: strPtr("c4"), Price: 400.0},
			}

			lineItems := []*PurchaseLineItem{
				{ID: "lineitem_mp1", PurchaseID: "purchase_mp1", ItemIndex: 0, Product: strPtr("A"), Quantity: 1},
				{ID: "lineitem_mp2", PurchaseID: "purchase_mp1", ItemIndex: 1, Product: strPtr("B"), Quantity: 1},
				{ID: "lineitem_mp3", PurchaseID: "purchase_mp2", ItemIndex: 0, Product: strPtr("C"), Quantity: 1},
				{ID: "lineitem_mp4", PurchaseID: "purchase_mp2", ItemIndex: 1, Product: strPtr("D"), Quantity: 1},
				{ID: "lineitem_mp5", PurchaseID: "purchase_mp3", ItemIndex: 0, Product: strPtr("E"), Quantity: 1},
				{ID: "lineitem_mp6", PurchaseID: "purchase_mp3", ItemIndex: 1, Product: strPtr("F"), Quantity: 1},
				{ID: "lineitem_mp7", PurchaseID: "purchase_mp4", ItemIndex: 0, Product: strPtr("G"), Quantity: 1},
				{ID: "lineitem_mp8", PurchaseID: "purchase_mp4", ItemIndex: 1, Product: strPtr("H"), Quantity: 1},
			}

			for _, p := range purchases {
				if err := session.Save(ctx, p); err != nil {
					t.Fatalf("Save purchase failed: %v", err)
				}
			}
			for _, li := range lineItems {
				if err := session.Save(ctx, li); err != nil {
					t.Fatalf("Save line item failed: %v", err)
				}
			}

			defer func() {
				for _, li := range lineItems {
					session.Delete(ctx, li)
				}
				for _, p := range purchases {
					session.Delete(ctx, p)
				}
			}()

			// Query with GROUP BY and HAVING
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(pli.id) >= ?", 2)
			query.OrderBy(query.Asc("p.id"))

			// First page: 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate page1 failed: %v", err)
			}

			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_mp1"), orm1.NewKey("purchase_mp2")},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: 2 items
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate page2 failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_mp3"), orm1.NewKey("purchase_mp4")},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}

			// Backward from end: 2 items
			last := 2
			page3, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate page3 failed: %v", err)
			}

			want3 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_mp3"), orm1.NewKey("purchase_mp4")},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want3, page3); diff != "" {
				t.Errorf("Page3 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationGroupByDescOrder(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertPurchaseGroupByTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupPurchaseGroupByTestData(ctx, session)

			// Query with GROUP BY, HAVING and DESC order
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(pli.id) >= ?", 2) // purchase_gb1, purchase_gb2
			query.OrderBy(query.Desc("p.id"))     // DESC order

			// First page: first 1 item (highest ID)
			first := 1
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb2 (DESC order)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb2")},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page
			page2, err := query.Paginate(ctx, page1.Cursors[0], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: purchase_gb1
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey("purchase_gb1")},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
