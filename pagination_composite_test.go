package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/hanpama/orm1"
)

// BlogPostComposite entity with composite primary key for pagination tests
type BlogPostComposite struct {
	Key1        int64
	Key2        int64
	Title       string
	Rating      *int
	PublishedAt *int64 // unix timestamp, can be NULL
}

func createBlogPostCompositeTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&BlogPostComposite{},
		orm1.WithTable("blog_posts_composite"),
		orm1.WithPrimaryKey("Key1", "Key2"))

	return factory.CreateSession()
}

func setupBlogPostCompositeTestData(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "blog_posts_composite")

	setup.ExecSchema(t, backend, `
		CREATE TABLE blog_posts_composite (
			key1 INTEGER NOT NULL,
			key2 INTEGER NOT NULL,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at INTEGER,
			PRIMARY KEY (key1, key2)
		);
	`, `
		CREATE TABLE blog_posts_composite (
			key1 BIGINT NOT NULL,
			key2 BIGINT NOT NULL,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at BIGINT,
			PRIMARY KEY (key1, key2)
		);
	`)

	// Insert standard test data
	setup.DB.Exec("INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (1, 1, 'Post1', 3, 1609459200)")
	setup.DB.Exec("INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (1, 2, 'Post2', NULL, 1609545600)")
	setup.DB.Exec("INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (2, 1, 'Post3', 4, NULL)")
	setup.DB.Exec("INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (2, 2, 'Post4', NULL, NULL)")

	return setup
}

func TestPaginationCompositeAscForwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostCompositeTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostCompositeTestSession(setup)
			ctx := context.Background()

			// Order by published_at ASC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.AscNullsLast("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,1), blog_post(1,2) (sorted by published_at asc)
			// Composite key cursors: [key1, key2]
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(1)), orm1.NewKey(int64(1), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor (1,2), get 1 item
			first2 := 1
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first2, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(2,1) (NULLs are last, ordered by PK)
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeAscBackwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostCompositeTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostCompositeTestSession(setup)
			ctx := context.Background()

			// Order by published_at ASC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.AscNullsLast("bp.published_at"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (with NULL published_at, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(2), int64(2))},
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
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeAscForwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostCompositeTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostCompositeTestSession(setup)
			ctx := context.Background()

			// Order by rating ASC with nulls first
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.AscNullsFirst("bp.rating"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,2), blog_post(2,2) (NULL ratings come first, ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2)), orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeAscBackwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostCompositeTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostCompositeTestSession(setup)
			ctx := context.Background()

			// Order by rating ASC with nulls first
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.AscNullsFirst("bp.rating"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (with actual rating values: rating=3 and rating=4)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(1)), orm1.NewKey(int64(2), int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
