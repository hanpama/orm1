package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// Helper function to insert blog post test data
func insertBlogPostTestData(ctx context.Context, session *orm1.Session) error {
	// Insert standard test data (4 posts with various NULL patterns)
	rawQuery1 := orm1.NewRawQuery(session, "INSERT INTO blog_posts (id, title, rating, published_at) VALUES (?, ?, ?, ?)", int64(1), "Post1", 3, int64(1609459200))
	if _, err := rawQuery1.Exec(ctx); err != nil {
		return err
	}
	rawQuery2 := orm1.NewRawQuery(session, "INSERT INTO blog_posts (id, title, rating, published_at) VALUES (?, ?, ?, ?)", int64(2), "Post2", nil, int64(1609545600))
	if _, err := rawQuery2.Exec(ctx); err != nil {
		return err
	}
	rawQuery3 := orm1.NewRawQuery(session, "INSERT INTO blog_posts (id, title, rating, published_at) VALUES (?, ?, ?, ?)", int64(3), "Post3", 4, nil)
	if _, err := rawQuery3.Exec(ctx); err != nil {
		return err
	}
	rawQuery4 := orm1.NewRawQuery(session, "INSERT INTO blog_posts (id, title, rating, published_at) VALUES (?, ?, ?, ?)", int64(4), "Post4", nil, nil)
	if _, err := rawQuery4.Exec(ctx); err != nil {
		return err
	}
	return nil
}

// Helper function to cleanup blog post test data
func cleanupBlogPostTestData(ctx context.Context, session *orm1.Session) {
	rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts")
	rawQuery.Exec(ctx)
}

// Simple Pagination Tests

func TestPaginationSimpleAscForwardNullsLast(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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

			// Third page: get last item
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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

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

// Default NULLS behavior tests (without explicit NULLS FIRST/LAST)

func TestPaginationSimpleAscDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

			// Order by published_at ASC (no explicit NULLS clause)
			// Expected: PostgreSQL default is NULLS LAST for ASC
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.Asc("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post1 (1609459200), blog_post2 (1609545600)
			// NULLs should be last by PostgreSQL default
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor 2, get 2 items
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post3 (NULL), blog_post4 (NULL) - NULLs at the end
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

			// Order by published_at DESC (no explicit NULLS clause)
			// Expected: PostgreSQL default is NULLS FIRST for DESC
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.Desc("bp.published_at"))

			// First page: first 2 items
			first := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post3 (NULL), blog_post4 (NULL) - NULLs first by default
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: after cursor 4, get 2 items
			page2, err := query.Paginate(ctx, page1.Cursors[1], &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post2 (1609545600), blog_post1 (1609459200) - DESC order
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleAscBackwardDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

			// Order by published_at ASC (default NULLS LAST)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.Asc("bp.published_at"))

			// Backward pagination: last 2 items
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post3 (NULL), blog_post4 (NULL) - last items with NULLS LAST
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before cursor 3, get 2 items
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post1 (1609459200), blog_post2 (1609545600)
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(1)), orm1.NewKey(int64(2))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPaginationSimpleDescBackwardDefaultNulls(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

			// Order by published_at DESC (default NULLS FIRST)
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.Desc("bp.published_at"))

			// Backward pagination: last 2 items
			last := 2
			page1, err := query.Paginate(ctx, orm1.NewKey(), nil, orm1.NewKey(), &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post2 (1609545600), blog_post1 (1609459200) - last items in DESC
			want1 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(1))},
				HasPreviousPage: true,
				HasNextPage:     false,
			}
			if diff := cmp.Diff(want1, page1); diff != "" {
				t.Errorf("Page1 mismatch (-want +got):\n%s", diff)
			}

			// Second page: before cursor 2, get 2 items
			page2, err := query.Paginate(ctx, orm1.NewKey(), nil, page1.Cursors[0], &last)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post3 (NULL), blog_post4 (NULL) - NULLs first
			want2 := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(3)), orm1.NewKey(int64(4))},
				HasPreviousPage: false,
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want2, page2); diff != "" {
				t.Errorf("Page2 mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
