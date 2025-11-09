package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/hanpama/orm1"
)

// BlogPostAggregate entity for aggregate pagination tests
type BlogPostAggregate struct {
	ID          int64
	Title       string
	Rating      *int
	PublishedAt *int64
}

// BlogPostCommentAggregate entity for aggregate pagination tests
type BlogPostCommentAggregate struct {
	ID        int64
	PostID    int64
	Content   string
	CreatedAt *int64
}

func createBlogPostAggregateTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&BlogPostAggregate{}, orm1.WithTable("blog_posts_agg"))
	factory.RegisterEntity(&BlogPostCommentAggregate{}, orm1.WithTable("blog_post_comments_agg"))

	return factory.CreateSession()
}

func setupBlogPostAggregateTestData(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "blog_post_comments_agg", "blog_posts_agg")

	setup.ExecSchema(t, backend, `
		CREATE TABLE blog_posts_agg (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at INTEGER
		);
		CREATE TABLE blog_post_comments_agg (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			post_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			created_at INTEGER,
			FOREIGN KEY (post_id) REFERENCES blog_posts_agg(id)
		);
	`, `
		CREATE TABLE blog_posts_agg (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			rating INTEGER,
			published_at BIGINT
		);
		CREATE TABLE blog_post_comments_agg (
			id SERIAL PRIMARY KEY,
			post_id BIGINT NOT NULL,
			content TEXT NOT NULL,
			created_at BIGINT,
			FOREIGN KEY (post_id) REFERENCES blog_posts_agg(id)
		);
	`)

	// Insert blog posts
	setup.DB.Exec("INSERT INTO blog_posts_agg (title, rating, published_at) VALUES ('Post1', 3, 1609459200)")
	setup.DB.Exec("INSERT INTO blog_posts_agg (title, rating, published_at) VALUES ('Post2', NULL, 1609545600)")
	setup.DB.Exec("INSERT INTO blog_posts_agg (title, rating, published_at) VALUES ('Post3', 4, NULL)")
	setup.DB.Exec("INSERT INTO blog_posts_agg (title, rating, published_at) VALUES ('Post4', NULL, NULL)")

	// Insert comments
	// Post 1: comments at times 100, 200
	setup.DB.Exec("INSERT INTO blog_post_comments_agg (post_id, content, created_at) VALUES (1, 'Comment1', 100)")
	setup.DB.Exec("INSERT INTO blog_post_comments_agg (post_id, content, created_at) VALUES (1, 'Comment2', 200)")
	// Post 2: comment at time 300
	setup.DB.Exec("INSERT INTO blog_post_comments_agg (post_id, content, created_at) VALUES (2, 'Comment3', 300)")
	// Post 3: no comments (NULL max created_at)
	// Post 4: comment with NULL created_at
	setup.DB.Exec("INSERT INTO blog_post_comments_agg (post_id, content, created_at) VALUES (4, 'Comment4', NULL)")

	return setup
}

func TestPaginationAggregateAscForwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostAggregateTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostAggregateTestSession(setup)
			ctx := context.Background()

			// Query with LEFT JOIN, GROUP BY, and ORDER BY MAX(created_at)
			query := orm1.NewEntityQuery[BlogPostAggregate](session, "bp")
			query.LeftJoin("blog_post_comments_agg", "c", "bp.id = c.post_id")
			query.GroupByPrimaryKey()

			// Order by MAX(created_at) using Asc method with raw SQL
			maxCreatedAt := query.AscNullsLast("MAX(c.created_at)")
			query.OrderBy(maxCreatedAt)

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: ordered by max(created_at) ASC with nulls last
			// Post 1 (max=200), Post 2 (max=300), Post 4 (max=NULL), Post 3 (max=NULL, no comments)
			// First 2: Post 1, Post 2
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationAggregateAscBackwardNullsLast(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostAggregateTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostAggregateTestSession(setup)
			ctx := context.Background()

			// Query with LEFT JOIN, GROUP BY, and ORDER BY MAX(created_at)
			query := orm1.NewEntityQuery[BlogPostAggregate](session, "bp")
			query.LeftJoin("blog_post_comments_agg", "c", "bp.id = c.post_id")
			query.GroupByPrimaryKey()

			// Order by MAX(created_at) using Asc method with raw SQL
			maxCreatedAt := query.AscNullsLast("MAX(c.created_at)")
			query.OrderBy(maxCreatedAt)

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items with NULL max(created_at), ordered by PK
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationAggregateAscForwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostAggregateTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostAggregateTestSession(setup)
			ctx := context.Background()

			// Query with LEFT JOIN, GROUP BY, and ORDER BY MAX(created_at) with NULLS FIRST
			query := orm1.NewEntityQuery[BlogPostAggregate](session, "bp")
			query.LeftJoin("blog_post_comments_agg", "c", "bp.id = c.post_id")
			query.GroupByPrimaryKey()

			// Order by MAX(created_at) with NULLS FIRST using Asc method
			maxCreatedAt := query.AscNullsFirst("MAX(c.created_at)")
			query.OrderBy(maxCreatedAt)

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: NULLs first (ordered by PK), then ordered by max(created_at) ASC
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationAggregateAscBackwardNullsFirst(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupBlogPostAggregateTestData(t, backend)
			defer setup.Cleanup()

			session := createBlogPostAggregateTestSession(setup)
			ctx := context.Background()

			// Query with LEFT JOIN, GROUP BY, and ORDER BY MAX(created_at) with NULLS FIRST
			query := orm1.NewEntityQuery[BlogPostAggregate](session, "bp")
			query.LeftJoin("blog_post_comments_agg", "c", "bp.id = c.post_id")
			query.GroupByPrimaryKey()

			// Order by MAX(created_at) with NULLS FIRST using Asc method
			maxCreatedAt := query.AscNullsFirst("MAX(c.created_at)")
			query.OrderBy(maxCreatedAt)

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (highest max(created_at) values)
			// Should be Post 1 (max=200) and Post 2 (max=300)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(2))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
