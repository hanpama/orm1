package orm1

import (
	"reflect"
)

// SessionFactory builds entity mappings and creates sessions.
// It builds EntityMapping instances from Registry metadata at creation time
// and reuses them for all sessions.
type SessionFactory struct {
	mappings map[reflect.Type]*EntityMapping
	driver   Driver
}

// NewSessionFactory creates a new session factory with a registry and driver.
// The factory builds entity mappings immediately from the registry metadata
// and caches them for efficient session creation.
//
// Example:
//
//	registry := orm1.NewRegistry()
//	registry.Register(&User{})
//	registry.Register(&Post{})
//
//	driver := orm1.NewPostgreSQLDriver(db)
//	factory := orm1.NewSessionFactory(registry, driver)
//
//	session := factory.CreateSession()
func NewSessionFactory(registry *Registry, driver Driver) *SessionFactory {
	// Build entity mappings at factory creation time
	mappings := BuildEntityMappings(
		registry.GetMetadata(),
		registry.GetRegistered(),
	)

	return &SessionFactory{
		mappings: mappings,
		driver:   driver,
	}
}

// CreateSession creates a new Session with a fresh backend instance.
// The backend is created using the Driver that was configured.
//
// Each call to CreateSession creates a new backend instance, as backends
// are not safe for concurrent use across multiple sessions.
//
// Entity mappings are already built at factory creation time, making
// session creation very efficient.
func (sf *SessionFactory) CreateSession() *Session {
	backend := sf.driver.CreateBackend()
	return newSession(backend, sf.mappings)
}
