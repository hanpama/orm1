// Package orm1 provides a lightweight, type-safe Object-Relational Mapping library.
//
// # Core Concepts
//
// orm1 is built around three main ideas:
//
// Entity Mapping: Define how Go structs map to database tables using RegisterEntity.
// Struct tags and naming conventions control the mapping behavior.
//
// Session: All database operations are performed through a session that manages
// entity lifecycle. The session tracks which entities have been persisted to determine
// whether Save operations should INSERT or UPDATE.
//
// Entity State: Entities loaded via Get or saved via Save are marked as persisted.
// This allows the session to perform UPDATEs for persisted entities and INSERTs for new ones.
//
// # Quick Start
//
//	// 1. Define your entity structs
//	type User struct {
//	    ID       int64  `orm1:"primary"`
//	    Name     string
//	    Email    string `orm1:"column:user_email"`
//	    Posts    []*Post
//	}
//
//	type Post struct {
//	    ID       int64 `orm1:"primary"`
//	    UserID   int64 `orm1:"parental"`
//	    Title    string
//	    Content  string
//	}
//
//	// 2. Register entities and create session factory
//	driver := orm1.NewSQLiteDriver(db)
//	factory := orm1.NewSessionFactoryWithDriver(driver)
//	factory.RegisterEntity(&User{})
//	factory.RegisterEntity(&Post{})
//
//	// 3. Create session
//	session := factory.CreateSession()
//
//	// 4. Perform operations
//	var user *User
//	session.Get(ctx, &user, orm1.NewKey(1))
//	user.Name = "Updated Name"
//	session.Save(ctx, user)
//
// # Entity Relationships
//
// Parent-child relationships are automatically loaded and saved:
//
//	var user *User
//	session.Get(ctx, &user, orm1.NewKey(1))
//	// user.Posts is automatically populated
//
//	user.Posts = append(user.Posts, &Post{Title: "New Post"})
//	session.Save(ctx, user)
//	// New post is inserted with UserID set automatically
//
// # Type-Safe Queries
//
// Use EntityQuery for complex queries with compile-time type safety:
//
//	query := orm1.NewEntityQuery[User](session, "u")
//	users, err := query.
//	    Where("u.email LIKE ?", "%@example.com").
//	    OrderBy(query.Desc("u.name")).
//	    FetchAll(ctx)
//
// # Raw SQL
//
// For cases where you need raw SQL:
//
//	var results []*struct {
//	    Name  string
//	    Count int64
//	}
//	raw := orm1.NewRawQuery(session, "SELECT name, COUNT(*) as count FROM users GROUP BY name")
//	raw.ScanAll(ctx, &results)
package orm1

import (
	"fmt"
	"reflect"

	"github.com/hanpama/orm1/mapping"
)

// childrenKey uniquely identifies a child collection within a parent entity.
type childrenKey struct {
	parent any            // parent entity pointer
	child  *mapping.Child // Child metadata pointer
}

// Session manages database operations and tracks entity state.
// It tracks which entities have been persisted to the database, enabling Save to
// determine whether to INSERT (for new entities) or UPDATE (for persisted entities).
// All persistence operations (Get, Save, Delete) are performed through a Session.
type Session struct {
	backend    SessionBackend
	mappings   map[reflect.Type]*mapping.EntityMapping
	scanBuffer []any // Reusable buffer for scanEntity to avoid allocations

	persisted map[any]struct{}      // entity pointer → persisted in DB
	children  map[childrenKey][]any // (parent ptr, child meta) → children
}

// newSession creates a new Session with the given backend and mappings.
func newSession(backend SessionBackend, mappings map[reflect.Type]*mapping.EntityMapping) *Session {
	return &Session{
		backend:   backend,
		mappings:  mappings,
		persisted: make(map[any]struct{}),
		children:  make(map[childrenKey][]any),
	}
}

func (s *Session) getMapping(entityType reflect.Type) (*mapping.EntityMapping, error) {
	mapping, ok := s.mappings[entityType]
	if !ok {
		return nil, fmt.Errorf("no mapping found for type %s", entityType)
	}
	return mapping, nil
}

// markPersisted marks an entity as persisted in the database.
func (s *Session) markPersisted(entity any) {
	s.persisted[entity] = struct{}{}
}

// isPersisted checks if an entity is persisted in the database.
func (s *Session) isPersisted(entity any) bool {
	_, ok := s.persisted[entity]
	return ok
}

// unmarkPersisted removes an entity from the persisted set.
func (s *Session) unmarkPersisted(entity any) {
	delete(s.persisted, entity)
}

// indexChildren stores children entities for a parent entity and child relationship.
func (s *Session) indexChildren(parent any, childMeta *mapping.Child, children []any) {
	s.children[childrenKey{parent: parent, child: childMeta}] = children
}

// getChildren retrieves children entities for a parent entity and child relationship.
func (s *Session) getChildren(parent any, childMeta *mapping.Child) []any {
	return s.children[childrenKey{parent: parent, child: childMeta}]
}

// scanEntity scans a single row into an entity pointer using specified fields.
// fieldNames must be explicitly provided - no nil/default behavior.
// Tracking must be done by the caller if needed.
// Reuses Session.scanBuffer to avoid allocations.
func (s *Session) scanEntity(em *mapping.EntityMapping, entityPtr any, row Rows, fieldNames []string) error {
	// Reuse scanBuffer, resetting to length 0 while keeping capacity
	s.scanBuffer = s.scanBuffer[:0]

	// Grow buffer if needed (only on first use or if entity has more fields)
	if cap(s.scanBuffer) < len(fieldNames) {
		s.scanBuffer = make([]any, 0, len(fieldNames))
	}

	for _, name := range fieldNames {
		field := em.FieldMap[name]
		p := field.GetPtr(entityPtr)
		s.scanBuffer = append(s.scanBuffer, p)
	}

	return row.Scan(s.scanBuffer...)
}

// Clear clears the session state, removing all tracked entities and cached relationships.
// This is useful for long-lived sessions that process multiple logical units of work
// and need to free memory or reset tracking state between operations.
//
// Example usage in a batch processing scenario:
//
//	session := factory.CreateSession()
//	for batch := range batches {
//	    for _, item := range batch {
//	        session.Save(ctx, item)
//	    }
//	    session.Clear() // Clear session state between batches
//	}
//
// Note: After calling Clear(), previously loaded entities are no longer tracked as persisted.
// Calling Save on them again will perform INSERT instead of UPDATE.
func (s *Session) Clear() {
	s.persisted = make(map[any]struct{})
	s.children = make(map[childrenKey][]any)
}
