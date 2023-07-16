package compliance

import (
	"fmt"

	"github.com/kaytu-io/kaytu-engine/pkg/compliance/db"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func PopulateDatabase(dbc *gorm.DB, compliancePath, queryPath string) error {
	p := GitParser{}
	if err := p.ExtractQueries(queryPath); err != nil {
		return err
	}

	if err := p.ExtractCompliance(compliancePath); err != nil {
		return err
	}

	err := dbc.Transaction(func(tx *gorm.DB) error {
		tx.Model(&db.Benchmark{}).Where("1=1").Unscoped().Delete(&db.Benchmark{})
		tx.Model(&db.Policy{}).Where("1=1").Unscoped().Delete(&db.Policy{})
		tx.Model(&db.Query{}).Where("1=1").Unscoped().Delete(&db.Query{})

		for _, obj := range p.queries {
			obj.Policies = nil
			err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},                                                                                 // key column
				DoUpdates: clause.AssignmentColumns([]string{"query_to_execute", "connector", "list_of_tables", "engine", "updated_at"}), // column needed to be updated
			}).Create(&obj).Error
			if err != nil {
				return err
			}
		}

		for _, obj := range p.policies {
			obj.Benchmarks = nil
			err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},                                                                                                          // key column
				DoUpdates: clause.AssignmentColumns([]string{"title", "description", "document_uri", "severity", "manual_verification", "managed", "updated_at"}), // column needed to be updated
			}).Create(&obj).Error
			if err != nil {
				return err
			}
			for _, tag := range obj.Tags {
				err = tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "key"}, {Name: "policy_id"}}, // key columns
					DoUpdates: clause.AssignmentColumns([]string{"key", "value"}),  // column needed to be updated
				}).Create(&tag).Error
				if err != nil {
					return fmt.Errorf("failure in policy tag insert: %v", err)
				}
			}
		}

		for _, obj := range p.benchmarks {
			obj.Children = nil
			obj.Policies = nil
			err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},                                                                                                                                     // key column
				DoUpdates: clause.AssignmentColumns([]string{"title", "description", "logo_uri", "category", "document_uri", "enabled", "managed", "auto_assign", "baseline", "updated_at"}), // column needed to be updated
			}).Create(&obj).Error
			if err != nil {
				return err
			}
			for _, tag := range obj.Tags {
				err = tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "key"}, {Name: "benchmark_id"}}, // key columns
					DoUpdates: clause.AssignmentColumns([]string{"key", "value"}),     // column needed to be updated
				}).Create(&tag).Error
				if err != nil {
					return fmt.Errorf("failure in benchmark tag insert: %v", err)
				}
			}
		}

		for _, obj := range p.benchmarks {
			for _, child := range obj.Children {
				err := tx.Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(&db.BenchmarkChild{
					BenchmarkID: obj.ID,
					ChildID:     child.ID,
				}).Error
				if err != nil {
					return err
				}
			}

			for _, policy := range obj.Policies {
				err := tx.Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(&db.BenchmarkPolicies{
					BenchmarkID: obj.ID,
					PolicyID:    policy.ID,
				}).Error
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
