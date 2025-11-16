package orm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// TestPaginationWithOffsetNoCursors tests offset pagination via Paginate when no cursors are provided
func TestPaginationWithOffsetNoCursors(t *testing.T) {
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
			registry.Register(&BlogPost{}, orm1.WithTable("blog_posts"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			if err := insertBlogPostTestData(ctx, session); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			defer cleanupBlogPostTestData(ctx, session)

			// Order by id ASC
			query := orm1.NewEntityQuery[BlogPost](session, "bp")
			query.OrderBy(query.Asc("bp.id"))
			query.Offset(1) // Skip first item

			// Use Paginate without cursors (empty keys), with limit
			first := 2
			page, err := query.Paginate(ctx, orm1.NewKey(), &first, orm1.NewKey(), nil)
			if err != nil {
				t.Fatalf("Paginate failed: %v", err)
			}

			// Expect: blog_post2, blog_post3 (skipped blog_post1 due to offset)
			want := &orm1.Page{
				Cursors:         []orm1.Key{orm1.NewKey(int64(2)), orm1.NewKey(int64(3))},
				HasPreviousPage: true, // offset=1 means there's a previous page
				HasNextPage:     true,
			}
			if diff := cmp.Diff(want, page); diff != "" {
				t.Errorf("Page mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
