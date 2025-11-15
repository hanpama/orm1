package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// Helper function to insert composite blog post test data
func insertBlogPostCompositeTestData(ctx context.Context, session *orm1.Session) error {
	rawQuery1 := orm1.NewRawQuery(session, "INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (?, ?, ?, ?, ?)", int64(1), int64(1), "Post1", 3, int64(1609459200))
	if _, err := rawQuery1.Exec(ctx); err != nil {
		return err
	}
	rawQuery2 := orm1.NewRawQuery(session, "INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (?, ?, ?, ?, ?)", int64(1), int64(2), "Post2", nil, int64(1609545600))
	if _, err := rawQuery2.Exec(ctx); err != nil {
		return err
	}
	rawQuery3 := orm1.NewRawQuery(session, "INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (?, ?, ?, ?, ?)", int64(2), int64(1), "Post3", 4, nil)
	if _, err := rawQuery3.Exec(ctx); err != nil {
		return err
	}
	rawQuery4 := orm1.NewRawQuery(session, "INSERT INTO blog_posts_composite (key1, key2, title, rating, published_at) VALUES (?, ?, ?, ?, ?)", int64(2), int64(2), "Post4", nil, nil)
	if _, err := rawQuery4.Exec(ctx); err != nil {
		return err
	}
	return nil
}

func cleanupBlogPostCompositeTestData(ctx context.Context, session *orm1.Session) {
	rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts_composite")
	rawQuery.Exec(ctx)
}

func TestPaginationCompositeAscForwardNullsLast(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

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

func TestPaginationCompositeDescForwardNullsLast(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at DESC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.DescNullsLast("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,2), blog_post(1,1) (newest first, DESC order)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2)), orm1.NewKey(int64(1), int64(1))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeDescBackwardNullsLast(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at DESC (nulls last by default)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.DescNullsLast("bp.published_at"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (NULLs at the end, ordered by PK)
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
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(1))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeDescForwardNullsFirst(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by rating DESC with nulls first
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.DescNullsFirst("bp.rating"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: NULL ratings come first (blog_post(1,2), blog_post(2,2), ordered by PK)
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2)), orm1.NewKey(int64(2), int64(2))},
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

func TestPaginationCompositeDescBackwardNullsFirst(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by rating DESC with nulls first
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.DescNullsFirst("bp.rating"))

			// First page: last 2 items (backward pagination)
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: last 2 items (lowest ratings)
			// DESC with NULLS FIRST means: NULLs, then 4, then 3
			// Last 2 should be rating=4 (2,1) and rating=3 (1,1)
			// In DESC order, that's [(2,1), (1,1)]
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(1), int64(1))},
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
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: true,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeAscDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at ASC (no explicit NULLS clause)
			// Expected: PostgreSQL default is NULLS LAST for ASC
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.Asc("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,1) (1609459200), blog_post(1,2) (1609545600)
			// NULLs should be last by PostgreSQL default
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(1)), orm1.NewKey(int64(1), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor (1,2), get 2 items
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(2,1) (NULL), blog_post(2,2) (NULL) - NULLs at the end
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeDescDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at DESC (no explicit NULLS clause)
			// Expected: PostgreSQL default is NULLS FIRST for DESC
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.Desc("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(2,1) (NULL), blog_post(2,2) (NULL) - NULLs first by default
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor (2,2), get 2 items
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,2) (1609545600), blog_post(1,1) (1609459200) - DESC order
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2)), orm1.NewKey(int64(1), int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeAscBackwardDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at ASC (default NULLS LAST)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.Asc("bp.published_at"))

			// Backward pagination: last 2 items
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(2,1) (NULL), blog_post(2,2) (NULL) - last items with NULLS LAST
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before cursor (2,1), get 2 items
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,1) (1609459200), blog_post(1,2) (1609545600)
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(1)), orm1.NewKey(int64(1), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationCompositeDescBackwardDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&BlogPostComposite{},
				orm1.WithTable("blog_posts_composite"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostCompositeTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostCompositeTestData(ctx, session)

			// Order by published_at DESC (default NULLS FIRST)
			query := orm1.NewEntityQuery[BlogPostComposite](session, "bp")
			query.OrderBy(query.Desc("bp.published_at"))

			// Backward pagination: last 2 items
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(1,2) (1609545600), blog_post(1,1) (1609459200) - last items in DESC
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1), int64(2)), orm1.NewKey(int64(1), int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before cursor (1,2), get 2 items
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post(2,1) (NULL), blog_post(2,2) (NULL) - NULLs first
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2), int64(1)), orm1.NewKey(int64(2), int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
