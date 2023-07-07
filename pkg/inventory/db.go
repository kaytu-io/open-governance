package inventory

import (
	"github.com/jackc/pgx/v4"
	"github.com/kaytu-io/kaytu-util/pkg/model"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Database struct {
	orm *gorm.DB
}

func NewDatabase(orm *gorm.DB) Database {
	return Database{orm: orm}
}

func (db Database) Initialize() error {
	err := db.orm.AutoMigrate(
		&Service{},
		&ResourceType{},
		&SmartQuery{},
		&Metric{},
		&ResourceTypeTag{},
		&ServiceTag{},
	)
	if err != nil {
		return err
	}

	return nil
}

// AddQuery adding a query
func (db Database) AddQuery(q *SmartQuery) error {
	tx := db.orm.
		Model(&SmartQuery{}).
		Create(q)

	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

// GetQueries gets list of all queries
func (db Database) GetQueries() ([]SmartQuery, error) {
	var s []SmartQuery
	tx := db.orm.Preload("Tags").Find(&s)

	if tx.Error != nil {
		return nil, tx.Error
	}

	return s, nil
}

// GetQueriesWithFilters gets list of all queries filtered by tags and search
func (db Database) GetQueriesWithFilters(search *string, labels []string, provider *source.Type) ([]SmartQuery, error) {
	var s []SmartQuery

	m := db.orm.Model(&SmartQuery{}).
		Preload("Tags").
		Joins("LEFT JOIN smartquery_tags on smart_queries.id = smart_query_id " +
			"LEFT JOIN tags on smartquery_tags.tag_id = tags.id ")

	if search != nil {
		m = m.Where("title like ?", "%"+*search+"%")
	}
	if provider != nil {
		m = m.Where("provider = ?", string(*provider))
	}
	tx := m.Find(&s)

	if tx.Error != nil {
		return nil, tx.Error
	}

	v := map[uint]SmartQuery{}
	for _, item := range s {
		if _, ok := v[item.ID]; !ok {
			v[item.ID] = item
		}
	}
	var res []SmartQuery
	for _, val := range v {
		res = append(res, val)
	}
	return res, nil
}

// CountQueriesWithFilters count list of all queries filtered by tags and search
func (db Database) CountQueriesWithFilters(search *string, labels []string, provider *source.Type) (*int64, error) {
	var s int64

	m := db.orm.Model(&SmartQuery{}).
		Preload("Tags").
		Joins("LEFT JOIN smartquery_tags on smart_queries.id = smart_query_id " +
			"LEFT JOIN tags on smartquery_tags.tag_id = tags.id ").
		Distinct("smart_queries.id")

	if len(labels) != 0 {
		m = m.Where("tags.value in ?", labels)
	}
	if search != nil {
		m = m.Where("title like ?", "%"+*search+"%")
	}
	if provider != nil {
		m = m.Where("provider = ?", string(*provider))
	}
	tx := m.Count(&s)

	if tx.Error != nil {
		return nil, tx.Error
	}
	return &s, nil
}

// GetQuery gets a query with matching id
func (db Database) GetQuery(id string) (SmartQuery, error) {
	var s SmartQuery
	tx := db.orm.First(&s, "id = ?", id)

	if tx.Error != nil {
		return SmartQuery{}, tx.Error
	} else if tx.RowsAffected != 1 {
		return SmartQuery{}, pgx.ErrNoRows
	}

	return s, nil
}

func (db Database) CreateOrUpdateMetric(metric Metric) error {
	return db.orm.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_id"}, {Name: "resource_type"}},
		DoUpdates: clause.AssignmentColumns([]string{"schedule_job_id", "count", "last_day_count", "last_week_count", "last_quarter_count", "last_year_count"}),
	}).Create(metric).Error
}

func (db Database) FetchConnectionMetrics(sourceID string, resourceTypes []string) ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).
		Where("source_id = ?", sourceID).
		Where("resource_type in ?", resourceTypes).
		Find(&metrics)
	return metrics, tx.Error
}

func (db Database) FetchConnectionAllMetrics(sourceID string) ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).
		Where("source_id = ?", sourceID).
		Find(&metrics)
	return metrics, tx.Error
}

func (db Database) FetchProviderAllMetrics(provider source.Type) ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).
		Where("provider = ?", string(provider)).
		Find(&metrics)
	return metrics, tx.Error
}

func (db Database) FetchProviderMetrics(provider source.Type, resourceTypes []string) ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).
		Where("provider = ?", string(provider)).
		Where("resource_type in ?", resourceTypes).
		Find(&metrics)
	return metrics, tx.Error
}

func (db Database) FetchMetrics(resourceTypes []string) ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).
		Where("resource_type in ?", resourceTypes).
		Find(&metrics)
	return metrics, tx.Error
}

func (db Database) ListMetrics() ([]Metric, error) {
	var metrics []Metric
	tx := db.orm.Model(Metric{}).Find(&metrics)
	return metrics, tx.Error
}

func (db Database) ListResourceTypeTagsKeysWithPossibleValues(connectorTypes []source.Type, doSummarize *bool) (map[string][]string, error) {
	var tags []ResourceTypeTag
	tx := db.orm.Model(ResourceTypeTag{}).Joins("JOIN resource_types ON resource_type_tags.resource_type = resource_types.resource_type")
	if doSummarize != nil {
		tx = tx.Where("resource_types.do_summarize = ?", true)
	}
	if len(connectorTypes) > 0 {
		tx = tx.Where("resource_types.connector in ?", connectorTypes)
	}
	tx.Find(&tags)
	if tx.Error != nil {
		return nil, tx.Error
	}
	tagLikes := make([]model.TagLike, 0, len(tags))
	for _, tag := range tags {
		tagLikes = append(tagLikes, tag)
	}
	result := model.GetTagsMap(tagLikes)
	return result, nil
}

func (db Database) GetResourceTypeTagPossibleValues(key string, connectorTypes []source.Type, doSummarize *bool) ([]string, error) {
	var tags []ResourceTypeTag
	tx := db.orm.Model(ResourceTypeTag{}).Joins("JOIN resource_types ON resource_type_tags.resource_type = resource_types.resource_type")
	if doSummarize != nil {
		tx = tx.Where("resource_types.do_summarize = ?", true)
	}
	if len(connectorTypes) > 0 {
		tx = tx.Where("resource_types.connector in ?", connectorTypes)
	}
	tx.Where("key = ?", key).Find(&tags)
	if tx.Error != nil {
		return nil, tx.Error
	}
	tagLikes := make([]model.TagLike, 0, len(tags))
	for _, tag := range tags {
		tagLikes = append(tagLikes, tag)
	}
	result := model.GetTagsMap(tagLikes)
	return result[key], nil
}

func (db Database) ListFilteredResourceTypes(tags map[string][]string, resourceTypeNames []string, serviceNames []string, connectorTypes []source.Type, doSummarize bool) ([]ResourceType, error) {
	var resourceTypes []ResourceType
	query := db.orm.Model(ResourceType{}).Preload(clause.Associations)
	query = query.Where("resource_types.do_summarize = ?", doSummarize)
	if len(tags) != 0 {
		query = query.Joins("JOIN resource_type_tags AS tags ON tags.resource_type = resource_types.resource_type")
		for key, values := range tags {
			if len(values) != 0 {
				query = query.Where("tags.key = ? AND tags.value @> ?", key, pq.StringArray(values))
			} else {
				query = query.Where("tags.key = ?", key)
			}
		}
	}
	if len(serviceNames) != 0 {
		query = query.Where("service_name IN ?", serviceNames)
	}
	if len(connectorTypes) != 0 {
		query = query.Where("connector IN ?", connectorTypes)
	}
	if len(resourceTypeNames) != 0 {
		query = query.Where("resource_type IN ?", resourceTypeNames)
	}
	tx := query.Find(&resourceTypes)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return resourceTypes, nil
}

func (db Database) GetResourceType(resourceType string) (*ResourceType, error) {
	var rtObj ResourceType
	tx := db.orm.Model(ResourceType{}).Preload(clause.Associations).Where("resource_type = ?", resourceType).First(&rtObj)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &rtObj, nil
}

func (db Database) ListServiceTagsKeysWithPossibleValues() (map[string][]string, error) {
	var tags []ServiceTag
	tx := db.orm.Model(ServiceTag{}).Find(&tags)
	if tx.Error != nil {
		return nil, tx.Error
	}
	tagLikes := make([]model.TagLike, 0, len(tags))
	for _, tag := range tags {
		tagLikes = append(tagLikes, tag)
	}
	result := model.GetTagsMap(tagLikes)
	return result, nil
}

func (db Database) GetServiceTagPossibleValues(key string) (map[string][]string, error) {
	var tags []ServiceTag
	tx := db.orm.Model(ServiceTag{}).Where("key = ?", key).Find(&tags)
	if tx.Error != nil {
		return nil, tx.Error
	}
	tagLikes := make([]model.TagLike, 0, len(tags))
	for _, tag := range tags {
		tagLikes = append(tagLikes, tag)
	}
	result := model.GetTagsMap(tagLikes)
	return result, nil
}

func (db Database) ListFilteredServices(tags map[string][]string, connectorTypes []source.Type) ([]Service, error) {
	var services []Service
	query := db.orm.Model(Service{}).Preload(clause.Associations)
	if len(tags) != 0 {
		query = query.Joins("JOIN service_tags AS tags ON tags.service_name = services.service_name")
		for key, values := range tags {
			if len(values) != 0 {
				query = query.Where("tags.key = ? AND tags.value @> ?", key, pq.StringArray(values))
			} else {
				query = query.Where("tags.key = ?", key)
			}
		}
	}
	if len(connectorTypes) != 0 {
		query = query.Where("connector IN ?", connectorTypes)
	}
	tx := query.Find(&services)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return services, nil
}

func (db Database) GetService(serviceName string) (*Service, error) {
	var service Service
	tx := db.orm.Model(Service{}).Preload(clause.Associations).Where("service_name = ?", serviceName).First(&service)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &service, nil
}
