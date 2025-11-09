package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/hanpama/orm1"
)

// BlogPost entity for pagination tests
type BlogPost struct {
	ID          int64
	Title       string
	Rating      *int
	PublishedAt *int64 // unix timestamp, can be NULL
}

func createBlogPostTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))

	return factory.CreateSession()
}

func setupBlogPostTestData(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "blog_posts")

	setup.ExecSchema(t, backend, `
		CREATE TABLE blog_posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at INTEGER
		);
	`, `
		CREATE TABLE blog_posts (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at BIGINT
		);
	`)

	// Insert standard test data
	setup.DB.Exec("INSERT INTO blog_posts (title, rating, published_at) VALUES ('Post1', 3, 1609459200)")
	setup.DB.Exec("INSERT INTO blog_posts (title, rating, published_at) VALUES ('Post2', NULL, 1609545600)")
	setup.DB.Exec("INSERT INTO blog_posts (title, rating, published_at) VALUES ('Post3', 4, NULL)")
	setup.DB.Exec("INSERT INTO blog_posts (title, rating, published_at) VALUES ('Post4', NULL, NULL)")

	return setup
}

func TestPaginationSimpleAscForwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by published_at ASC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.AscNullsLast("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post1, blog_post2 (sorted by published_at asc)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor 2, get 1 item
			first2 := 1
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first2, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post3 (NULLs are last, ordered by PK)
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}

			// Third page: get last item (if there is one)
			page3, err := query.Paginate(ctx, page2.Cursors[0], &first2, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post4 (last item)
			want3 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want3, page3); diff != "" {
				t.Errorf("Page3 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleAscBackwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by published_at ASC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.AscNullsLast("bp.published_at"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (with NULL published_at, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before first cursor of page1, get 1 item
			last2 := 1
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last2)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}

			// Third page: before cursor of page2, get 1 item
			page3, err := query.Paginate(ctx, orm1.NewKey(), nil, page2.Cursors[0], &last2)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Should be the first item in the overall sort order
			want3 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want3, page3); diff != "" {
				t.Errorf("Page3 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleAscForwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by rating ASC with nulls first
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.AscNullsFirst("bp.rating"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post2, blog_post4 (NULL ratings come first, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(4))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescForwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by published_at DESC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.DescNullsLast("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post2, blog_post1 (newest first, DESC order)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(1))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleAscBackwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by rating ASC with nulls first
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.AscNullsFirst("bp.rating"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (with actual rating values: rating=3 and rating=4)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(3))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before first cursor of page1, get 1 item
			last2 := 1
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last2)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescBackwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by published_at DESC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.DescNullsLast("bp.published_at"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (NULLs at the end, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before first cursor of page1, get 1 item
			last2 := 1
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last2)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescForwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by rating DESC with nulls first
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.DescNullsFirst("bp.rating"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: NULL ratings come first (blog_post2, blog_post4, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(4))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor, get 1 item
			first2 := 1
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first2, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescBackwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostTestSession(setup)
			ctx := context.Background()

			// Order by rating DESC with nulls first
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.DescNullsFirst("bp.rating"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (lowest ratings)
			// DESC with NULLS FIRST means: NULLs, then 4, then 3
			// Last 2 should be rating=4 (id=3) and rating=3 (id=1)
			// In DESC order, that's [3, 1]
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before first cursor of page1, get 1 item
			last2 := 1
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last2)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
