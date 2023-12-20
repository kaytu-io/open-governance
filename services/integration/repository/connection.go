package repository

import (
	"context"

	"github.com/kaytu-io/kaytu-engine/services/integration/db"
	"github.com/kaytu-io/kaytu-engine/services/integration/model"
	"github.com/kaytu-io/kaytu-util/pkg/source"
)

type Connection interface {
	List(context.Context) ([]model.Connection, error)
	ListOfType(context.Context, source.Type) ([]model.Connection, error)
	ListOfTypes(context.Context, []source.Type) ([]model.Connection, error)
	ListWithFilters(
		context.Context,
		[]source.Type,
		[]string,
		[]model.ConnectionLifecycleState,
		[]source.HealthStatus,
	) ([]model.Connection, error)

	Get(context.Context, []string) ([]model.Connection, error)

	Count(context.Context) (int64, error)
	CountOfType(context.Context, source.Type) (int64, error)
}

type ConnectionSQL struct {
	db db.Database
}

func NewConnectionSQL(db db.Database) Connection {
	return ConnectionSQL{
		db: db,
	}
}

// List gets list of all source
func (s ConnectionSQL) List(ctx context.Context) ([]model.Connection, error) {
	var connections []model.Connection

	tx := s.db.DB.WithContext(ctx).Joins("Connector").Joins("Credential").Find(&connections)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return connections, nil
}

// ListOfType gets list of sources with matching type
func (s ConnectionSQL) ListOfType(ctx context.Context, t source.Type) ([]model.Connection, error) {
	return s.ListOfTypes(ctx, []source.Type{t})
}

// ListOfTypes gets list of sources with matching types
func (s ConnectionSQL) ListOfTypes(ctx context.Context, types []source.Type) ([]model.Connection, error) {
	var connections []model.Connection

	tx := s.db.DB.WithContext(ctx).Joins("Connector").Joins("Credential").Find(&connections, "type IN ?", types)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return connections, nil
}

// ListWithFilters gets list of all source with specified filters.
func (s ConnectionSQL) ListWithFilters(
	ctx context.Context,
	types []source.Type,
	ids []string,
	lifecycleState []model.ConnectionLifecycleState,
	healthState []source.HealthStatus,
) ([]model.Connection, error) {
	var c []model.Connection

	tx := s.db.DB.WithContext(ctx).Joins("Connector").Joins("Credential")

	if len(types) > 0 {
		tx = tx.Where("type IN ?", types)
	}

	if len(ids) > 0 {
		tx = tx.Where("id IN ?", ids)
	}

	if len(lifecycleState) > 0 {
		tx = tx.Where("lifecycle_state IN ?", lifecycleState)
	}

	if len(healthState) > 0 {
		tx = tx.Where("health_state IN ?", healthState)
	}

	tx.Find(&c)

	if tx.Error != nil {
		return nil, tx.Error
	}

	return c, nil
}

func (s ConnectionSQL) Get(ctx context.Context, ids []string) ([]model.Connection, error) {
	var connections []model.Connection

	tx := s.db.DB.WithContext(ctx).Joins("Connector").Joins("Credential").Find(&connections, "id IN ?", ids)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return connections, nil
}

func (s ConnectionSQL) Count(ctx context.Context) (int64, error) {
	var c int64

	tx := s.db.DB.WithContext(ctx).Model(new(model.Connection)).Count(&c)
	if tx.Error != nil {
		return 0, tx.Error
	}

	return c, nil
}

func (s ConnectionSQL) CountOfType(ctx context.Context, t source.Type) (int64, error) {
	var c int64

	tx := s.db.DB.WithContext(ctx).Model(new(model.Connection)).Where("type = ?", t.String()).Count(&c)
	if tx.Error != nil {
		return 0, tx.Error
	}

	return c, nil
}
