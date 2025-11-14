package orm1

import (
	"context"
	"fmt"
	"reflect"

	"github.com/hanpama/orm1/driver"
	"github.com/hanpama/orm1/mapping"
)

// Get loads a single entity by primary key.
// dest must be a pointer to an entity pointer (**Entity).
//
// Get always performs a fresh database query. After loading, the entity
// is marked as persisted, so subsequent Save calls will perform UPDATE.
func (s *Session) Get(ctx context.Context, dest any, id Key) error {
	destType := reflect.TypeOf(dest)
	if destType.Kind() != reflect.Ptr {
		return fmt.Errorf("Get: dest must be a pointer to entity pointer, got %s", destType)
	}
	if destType.Elem().Kind() != reflect.Ptr {
		return fmt.Errorf("Get: dest must be a pointer to entity pointer, got %s", destType)
	}
	entityType := destType.Elem().Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("Get: entity type must be a struct, got %s", entityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	entities, err := s.get(ctx, mapping, mapping.PrimaryColumns(), []Key{id})
	if err != nil {
		return err
	}

	destVal := reflect.ValueOf(dest).Elem()
	if len(entities) > 0 {
		destVal.Set(reflect.ValueOf(entities[0]))
	}
	return nil
}

// Save persists an entity to the database.
// Performs UPDATE for entities marked as persisted, INSERT for new entities.
// After saving, the entity is marked as persisted.
// entity must be a pointer to a struct (*Entity).
func (s *Session) Save(ctx context.Context, entity any) error {
	entityType := reflect.TypeOf(entity)
	if entityType.Kind() != reflect.Ptr {
		return fmt.Errorf("Save: entity must be a pointer to struct, got %s", entityType)
	}
	entityType = entityType.Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("Save: entity must be a pointer to struct, got %s", entityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	return s.save(ctx, mapping, []any{entity})
}

// Delete removes an entity from the database.
// Only deletes entities that are marked as persisted.
// After deletion, the entity is unmarked from the persisted state.
// entity must be a pointer to a struct (*Entity).
func (s *Session) Delete(ctx context.Context, entity any) error {
	entityType := reflect.TypeOf(entity)
	if entityType.Kind() != reflect.Ptr {
		return fmt.Errorf("Delete: entity must be a pointer to struct, got %s", entityType)
	}
	entityType = entityType.Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("Delete: entity must be a pointer to struct, got %s", entityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	if !s.isPersisted(entity) {
		return nil
	}
	return s.delete(ctx, mapping, []any{entity})
}

// BatchGet loads multiple entities by their primary keys in a single query.
// dests must be a pointer to a slice of entity pointers (*[]*Entity).
// Results are returned in the same order as ids.
//
// BatchGet always performs a fresh database query. After loading, entities
// are marked as persisted, so subsequent Save calls will perform UPDATE.
//
// For non-existent keys, nil is placed at the corresponding position in the result slice.
// For example, if ids = [key1, key2, key3] and only key1 and key3 exist in the database,
// the result will be [entity1, nil, entity3].
func (s *Session) BatchGet(ctx context.Context, dests any, ids []Key) error {
	destsType := reflect.TypeOf(dests)
	if destsType.Kind() != reflect.Ptr {
		return fmt.Errorf("BatchGet: dests must be a pointer to slice of entity pointers, got %s", destsType)
	}
	sliceType := destsType.Elem()
	if sliceType.Kind() != reflect.Slice {
		return fmt.Errorf("BatchGet: dests must be a pointer to slice of entity pointers, got %s", reflect.TypeOf(dests))
	}
	ptrType := sliceType.Elem()
	if ptrType.Kind() != reflect.Ptr {
		return fmt.Errorf("BatchGet: dests must be a pointer to slice of entity pointers, got %s", reflect.TypeOf(dests))
	}
	entityType := ptrType.Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("BatchGet: entity type must be a struct, got %s", entityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	entities, err := s.get(ctx, mapping, mapping.PrimaryColumns(), ids)
	if err != nil {
		return err
	}

	// Build map from key to entity (Key is now comparable and can be used as map key)
	entityMap := make(map[Key]any, len(entities))
	for _, entity := range entities {
		pk := mapping.ExtractKey(entity, mapping.PrimaryKey)
		entityMap[pk] = entity
	}

	// Create new result slice with ids order and replace dests entirely
	resultSlice := reflect.MakeSlice(sliceType, len(ids), len(ids))
	for i, id := range ids {
		if entity, ok := entityMap[id]; ok {
			resultSlice.Index(i).Set(reflect.ValueOf(entity))
		}
		// Non-existent keys remain as zero value (nil)
	}
	reflect.ValueOf(dests).Elem().Set(resultSlice)

	return nil
}

// BatchSave persists multiple entities in a single batch operation.
// Performs UPDATE for entities marked as persisted, INSERT for new entities.
// After saving, all entities are marked as persisted.
// entities must be entity pointers (*Entity).
func (s *Session) BatchSave(ctx context.Context, entities ...any) error {
	if len(entities) == 0 {
		return nil
	}

	// Unwrap *Entity -> Entity
	firstEntityType := reflect.TypeOf(entities[0])
	if firstEntityType.Kind() != reflect.Ptr {
		return fmt.Errorf("BatchSave: entities must be pointers to struct, got %s", firstEntityType)
	}
	entityType := firstEntityType.Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("BatchSave: entities must be pointers to struct, got %s", firstEntityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	return s.save(ctx, mapping, entities)
}

// BatchDelete removes multiple entities from the database.
// Only deletes entities that are marked as persisted.
// After deletion, entities are unmarked from the persisted state.
// entities must be entity pointers (*Entity).
func (s *Session) BatchDelete(ctx context.Context, entities ...any) error {
	if len(entities) == 0 {
		return nil
	}

	// Unwrap *Entity -> Entity
	firstEntityType := reflect.TypeOf(entities[0])
	if firstEntityType.Kind() != reflect.Ptr {
		return fmt.Errorf("BatchDelete: entities must be pointers to struct, got %s", firstEntityType)
	}
	entityType := firstEntityType.Elem()
	if entityType.Kind() != reflect.Struct {
		return fmt.Errorf("BatchDelete: entities must be pointers to struct, got %s", firstEntityType)
	}
	mapping, err := s.getMapping(entityType)
	if err != nil {
		return err
	}

	var toDeleteEntities []any
	for _, entity := range entities {
		if s.isPersisted(entity) {
			toDeleteEntities = append(toDeleteEntities, entity)
		}
	}
	return s.delete(ctx, mapping, toDeleteEntities)
}

func (s *Session) get(ctx context.Context, em *mapping.EntityMapping, keyColumns []string, ids []Key) ([]any, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	// Validate key lengths
	for _, id := range ids {
		if id.Length() != len(keyColumns) {
			return nil, fmt.Errorf("key has %d values but %d key columns expected (entity %s, key columns: %v)", id.Length(), len(keyColumns), em.EntityType, keyColumns)
		}
	}

	selectStmt := driver.SelectOp{
		Select:     em.Columns(),
		FromSchema: em.Schema,
		FromTable:  em.Table,
		KeyColumns: keyColumns,
		Keys:       ids,
	}

	rows, err := s.backend.Select(ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entities := make([]any, 0)
	for rows.Next() {
		entityPtr := reflect.New(em.EntityType).Interface()
		if err := s.scanEntity(em, entityPtr, rows, em.AllFields); err != nil {
			return nil, err
		}

		s.markPersisted(entityPtr)
		entities = append(entities, entityPtr)
	}

	if len(entities) > 0 && len(em.ChildMap) > 0 {
		if err := s.loadChildren(ctx, em, entities); err != nil {
			return nil, err
		}
	}

	return entities, nil
}

// loadChildren loads child entities for parent entities (cascade loading).
func (s *Session) loadChildren(ctx context.Context, em *mapping.EntityMapping, parents []any) error {
	parentKeys := make([]Key, len(parents))
	for i, parent := range parents {
		parentKeys[i] = em.ExtractKey(parent, em.PrimaryKey)
	}

	for _, child := range em.ChildMap {
		childMapping, err := s.getMapping(child.Target)
		if err != nil {
			return err
		}

		// Recursively load children (and their children) using s.get()
		// Children are already tracked in entities map by s.get()
		childEntities, err := s.get(ctx, childMapping, childMapping.ParentalColumns(), parentKeys)
		if err != nil {
			return err
		}

		// Build map from parent key to parent entity (O(1) lookup)
		parentKeyToParent := make(map[Key]any, len(parents))
		for i, parent := range parents {
			parentKeyToParent[parentKeys[i]] = parent
		}

		// Group children by parent pointer with estimated capacity
		// Estimate: avg children per parent = total children / num parents
		estimatedCapacity := (len(childEntities) + len(parents) - 1) / len(parents)
		if estimatedCapacity < 4 {
			estimatedCapacity = 4 // Minimum reasonable capacity
		}
		childGroups := make(map[any][]any, len(parents))
		for _, childEntity := range childEntities {
			// Extract parental key from child
			parentalKey := childMapping.ExtractKey(childEntity, childMapping.ParentalKey)

			// Find matching parent using map lookup
			if parent, ok := parentKeyToParent[parentalKey]; ok {
				if childGroups[parent] == nil {
					// Pre-allocate slice with estimated capacity
					childGroups[parent] = make([]any, 0, estimatedCapacity)
				}
				childGroups[parent] = append(childGroups[parent], childEntity)
			}
		}

		for _, parent := range parents {
			children := childGroups[parent]

			// Store the grouped children slice indexed by parent pointer and child metadata
			if len(children) > 0 {
				s.indexChildren(parent, child, children)
			}

			child.Set(parent, children)
		}
	}

	return nil
}

func (s *Session) save(ctx context.Context, em *mapping.EntityMapping, entities []any) error {
	toInsert := make([]any, 0, len(entities))
	toUpdate := make([]any, 0, len(entities))

	for _, entity := range entities {
		if s.isPersisted(entity) {
			toUpdate = append(toUpdate, entity)
		} else {
			toInsert = append(toInsert, entity)
		}
	}

	toInserted := []any{}
	if len(toInsert) > 0 {
		inserted, err := s.insert(ctx, em, toInsert)
		if err != nil {
			return err
		}
		toInserted = inserted
	}
	if len(toUpdate) > 0 {
		if err := s.update(ctx, em, toUpdate); err != nil {
			return err
		}
	}

	// All entities in this context (both updated and inserted)
	allEntities := append(toUpdate, toInserted...)

	for _, child := range em.ChildMap {
		childMapping, err := s.getMapping(child.Target)
		if err != nil {
			return err
		}

		var toDeleteEntities []any
		var toSave []any

		for _, entity := range allEntities {
			childEntities := child.Get(entity)

			parentPrimaryKey := em.ExtractKey(entity, em.PrimaryKey)

			for _, childEntity := range childEntities {
				for i, fieldName := range childMapping.ParentalKey {
					field := childMapping.FieldMap[fieldName]
					field.SetValue(childEntity, parentPrimaryKey.At(i))
				}
			}

			// Find children to delete (tracked but not in current list)
			// Use entity pointers directly for comparison
			currentIds := make(map[any]bool)
			for _, childEntity := range childEntities {
				currentIds[childEntity] = true
			}

			previousChildren := s.getChildren(entity, child)
			for _, prevChild := range previousChildren {
				if !currentIds[prevChild] {
					toDeleteEntities = append(toDeleteEntities, prevChild)
				}
			}

			toSave = append(toSave, childEntities...)
		}

		if len(toDeleteEntities) > 0 {
			if err := s.delete(ctx, childMapping, toDeleteEntities); err != nil {
				return err
			}
		}
		if len(toSave) > 0 {
			if err := s.save(ctx, childMapping, toSave); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Session) insert(ctx context.Context, em *mapping.EntityMapping, entities []any) ([]any, error) {
	insertCols := em.InsertableColumns()
	values := make([][]any, len(entities))
	for i, entity := range entities {
		row := make([]any, len(insertCols))
		for j, fieldName := range em.Insertable {
			field := em.FieldMap[fieldName]
			row[j] = field.GetValue(entity)
		}
		values[i] = row
	}

	insertStmt := driver.InsertOp{
		IntoSchema: em.Schema,
		IntoTable:  em.Table,
		Insert:     em.InsertableColumns(),
		Returning:  em.InsertReturningColumns(),
		Values:     values,
	}

	rows, err := s.backend.Insert(ctx, insertStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// INSERT always has RETURNING (primary key + auto fields)
	// Scan all returned values and mark entities as persisted
	result := make([]any, 0, len(entities))
	i := 0
	for rows.Next() {
		if i >= len(entities) {
			return nil, fmt.Errorf("more rows than entities")
		}
		if err := s.scanEntity(em, entities[i], rows, em.InsertReturning()); err != nil {
			return nil, err
		}

		s.markPersisted(entities[i])
		result = append(result, entities[i])
		i++
	}
	return result, nil
}

func (s *Session) update(ctx context.Context, em *mapping.EntityMapping, entities []any) error {
	// Build separate SetValues and WhereValues for batch update
	setValues := make([][]any, len(entities))
	whereValues := make([][]any, len(entities))

	for i, entity := range entities {
		setRow := make([]any, len(em.Updatable))
		for j, fieldName := range em.Updatable {
			field := em.FieldMap[fieldName]
			setRow[j] = field.GetValue(entity)
		}
		setValues[i] = setRow

		whereRow := make([]any, len(em.PrimaryKey))
		for j, fieldName := range em.PrimaryKey {
			field := em.FieldMap[fieldName]
			whereRow[j] = field.GetValue(entity)
		}
		whereValues[i] = whereRow
	}

	updateStmt := driver.UpdateOp{
		Schema:      em.Schema,
		Table:       em.Table,
		Sets:        em.UpdatableColumns(),
		Where:       em.PrimaryColumns(),
		SetValues:   setValues,
		WhereValues: whereValues,
	}

	return s.backend.Update(ctx, updateStmt)
}

func (s *Session) delete(ctx context.Context, em *mapping.EntityMapping, entities []any) error {
	// First, delete children recursively
	for _, child := range em.ChildMap {
		childMapping, err := s.getMapping(child.Target)
		if err != nil {
			return err
		}

		var toDeleteChildEntities []any

		for _, parent := range entities {
			children := s.getChildren(parent, child)
			toDeleteChildEntities = append(toDeleteChildEntities, children...)
		}

		if len(toDeleteChildEntities) > 0 {
			if err := s.delete(ctx, childMapping, toDeleteChildEntities); err != nil {
				return err
			}
		}
	}

	// Extract keys from entities
	keys := make([]Key, len(entities))
	for i, entity := range entities {
		keys[i] = em.ExtractKey(entity, em.PrimaryKey)
	}

	// Then delete the entities themselves
	deleteStmt := driver.DeleteOp{
		FromSchema: em.Schema,
		FromTable:  em.Table,
		KeyColumns: em.PrimaryColumns(),
		Keys:       keys,
	}

	err := s.backend.Delete(ctx, deleteStmt)
	if err != nil {
		return err
	}

	for _, entity := range entities {
		s.unmarkPersisted(entity)
	}
	return nil
}
