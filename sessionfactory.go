package orm1

// SessionFactory constructs entity mappings and creates sessions.
// It uses Registry internally to build mappings from registered entities.
type SessionFactory struct {
	registry *Registry
	driver   Driver
}

// NewSessionFactory creates a new session factory with a registry and driver.
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
	return &SessionFactory{
		registry: registry,
		driver:   driver,
	}
}

// CreateSession creates a new Session with a fresh backend instance.
// The backend is created using the Driver that was configured.
//
// Each call to CreateSession creates a new backend instance, as backends
// are not safe for concurrent use across multiple sessions.
//
// Entity mappings are built lazily on the first call and cached for
// subsequent calls, making session creation very efficient.
func (sf *SessionFactory) CreateSession() *Session {
	backend := sf.driver.CreateBackend()
	mappings := sf.registry.Build()
	return newSession(backend, mappings)
}
