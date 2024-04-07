package db

import (
	"github.com/kaytu-io/kaytu-engine/services/integration/model"
	"github.com/kaytu-io/kaytu-util/pkg/source"
)

// ListConnectors gets list of all connectors
func (db Database) ListConnectors() ([]model.Connector, error) {
	var s []model.Connector
	tx := db.Orm.Find(&s)

	if tx.Error != nil {
		return nil, tx.Error
	}

	return s, nil
}

// GetConnector gets connector by name
func (db Database) GetConnector(name source.Type) (model.Connector, error) {
	var c model.Connector
	tx := db.Orm.First(&c, "name = ?", name)

	if tx.Error != nil {
		return model.Connector{}, tx.Error
	}

	return c, nil
}
