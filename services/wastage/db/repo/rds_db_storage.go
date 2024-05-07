package repo

import (
	"errors"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/connector"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/model"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"math"
	"strings"
)

type RDSDBStorageRepo interface {
	Create(tx *gorm.DB, m *model.RDSDBStorage) error
	Get(id uint) (*model.RDSDBStorage, error)
	Update(id uint, m model.RDSDBStorage) error
	Delete(id uint) error
	List() ([]model.RDSDBStorage, error)
	Truncate(tx *gorm.DB) error
	GetCheapestBySpecs(region string, engine, edition, clusterType string, volumeSize int32, iops int32, throughput float64, validTypes []model.RDSDBStorageVolumeType) (*model.RDSDBStorage, int32, int32, float64, error)
}

type RDSDBStorageRepoImpl struct {
	logger *zap.Logger
	db     *connector.Database
}

func NewRDSDBStorageRepo(logger *zap.Logger, db *connector.Database) RDSDBStorageRepo {
	return &RDSDBStorageRepoImpl{
		logger: logger,
		db:     db,
	}
}

func (r *RDSDBStorageRepoImpl) Create(tx *gorm.DB, m *model.RDSDBStorage) error {
	if tx == nil {
		tx = r.db.Conn()
	}
	return tx.Create(&m).Error
}

func (r *RDSDBStorageRepoImpl) Get(id uint) (*model.RDSDBStorage, error) {
	var m model.RDSDBStorage
	tx := r.db.Conn().Model(&model.RDSDBStorage{}).Where("id=?", id).First(&m)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, tx.Error
	}
	return &m, nil
}

func (r *RDSDBStorageRepoImpl) Update(id uint, m model.RDSDBStorage) error {
	return r.db.Conn().Model(&model.RDSDBStorage{}).Where("id=?", id).Updates(&m).Error
}

func (r *RDSDBStorageRepoImpl) Delete(id uint) error {
	return r.db.Conn().Unscoped().Delete(&model.RDSDBStorage{}, id).Error
}

func (r *RDSDBStorageRepoImpl) List() ([]model.RDSDBStorage, error) {
	var ms []model.RDSDBStorage
	tx := r.db.Conn().Model(&model.RDSDBStorage{}).Find(&ms)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return ms, nil
}

func (r *RDSDBStorageRepoImpl) Truncate(tx *gorm.DB) error {
	if tx == nil {
		tx = r.db.Conn()
	}
	tx = tx.Unscoped().Where("1 = 1").Delete(&model.RDSDBStorage{})
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func (r *RDSDBStorageRepoImpl) getMagneticTotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32, iops *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeMagnetic) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)

	millionIoPerMonth := math.Ceil(float64(*iops) * 30 * 24 * 60 * 60 / 1e6) // 30 days, 24 hours, 60 minutes, 60 seconds
	iopsCost := 0.0

	tx := r.db.Conn().Model(&model.RDSDBStorage{}).
		Where("product_family = ?", "System Operation").
		Where("region_code = ?", dbStorage.RegionCode).
		Where("volume_type = ?", "Magnetic").
		Where("group = ?", "RDS I/O Operation").
		Where("database_engine IN ?", []string{dbStorage.DatabaseEngine, "Any"})
	tx = tx.Order("price_per_unit asc")
	var iopsStorage model.RDSDBStorage
	err := tx.First(&iopsStorage).Error
	if err != nil {
		return 0, tx.Error
	}
	iopsCost = iopsStorage.PricePerUnit * millionIoPerMonth

	return sizeCost + iopsCost, nil
}

func (r *RDSDBStorageRepoImpl) getGp2TotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeGP2) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	return dbStorage.PricePerUnit * float64(*volumeSize), nil
}

func (r *RDSDBStorageRepoImpl) getGp3TotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32, iops *int32, throughput *float64) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeGP3) {
		return 0, errors.New("invalid volume type")
	}

	if *iops > model.RDSDBStorageTier1Gp3BaseIops ||
		*throughput > model.RDSDBStorageTier1Gp3BaseThroughput {
		*volumeSize = max(*volumeSize, model.RDSDBStorageTier1Gp3SizeThreshold)
	} else {
		*iops = model.RDSDBStorageTier1Gp3BaseIops
		*throughput = model.RDSDBStorageTier1Gp3BaseThroughput
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)
	iopsCost := 0.0
	throughputCost := 0.0

	if *volumeSize > model.RDSDBStorageTier1Gp3SizeThreshold {
		provisionedIops := int(*iops) - model.RDSDBStorageTier2Gp3BaseIops
		provisionedIops = max(provisionedIops, 0)
		if provisionedIops > 0 {
			tx := r.db.Conn().Model(&model.RDSDBStorage{}).
				Where("product_family = ?", "Provisioned IOPS").
				Where("region_code = ?", dbStorage.RegionCode).
				Where("deployment_option = ?", dbStorage.DeploymentOption).
				Where("group_description = ?", "RDS Provisioned GP3 IOPS").
				Where("database_engine = ?", dbStorage.DatabaseEngine)
			if len(dbStorage.DatabaseEdition) > 0 {
				tx = tx.Where("database_edition = ?", dbStorage.DatabaseEdition)
			}
			tx = tx.Order("price_per_unit asc")
			var iopsStorage model.RDSDBStorage
			err := tx.First(&iopsStorage).Error
			if err != nil {
				return 0, tx.Error
			}
			iopsCost = iopsStorage.PricePerUnit * float64(provisionedIops)
		} else {
			*iops = model.RDSDBStorageTier2Gp3BaseIops
		}

		provisionedThroughput := *throughput - model.RDSDBStorageTier2Gp3BaseThroughput
		provisionedThroughput = max(provisionedThroughput, 0)
		if provisionedThroughput > 0 {
			tx := r.db.Conn().Model(&model.RDSDBStorage{}).
				Where("product_family = ?", "Provisioned Throughput").
				Where("region_code = ?", dbStorage.RegionCode).
				Where("deployment_option = ?", dbStorage.DeploymentOption).
				Where("database_engine = ?", dbStorage.DatabaseEngine)
			if len(dbStorage.DatabaseEdition) > 0 {
				tx = tx.Where("database_edition = ?", dbStorage.DatabaseEdition)
			}
			tx = tx.Order("price_per_unit asc")
			var throughputStorage model.RDSDBStorage
			err := tx.First(&throughputStorage).Error
			if err != nil {
				return 0, tx.Error
			}
			throughputCost = throughputStorage.PricePerUnit * provisionedThroughput
		} else {
			*throughput = model.RDSDBStorageTier2Gp3BaseThroughput
		}
	} // Else is not needed since tier 1 iops/throughput is not configurable and is not charged

	return sizeCost + iopsCost + throughputCost, nil
}

func (r *RDSDBStorageRepoImpl) getIo1TotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32, iops *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeIO1) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)
	iopsCost := 0.0
	tx := r.db.Conn().Model(&model.RDSDBStorage{}).
		Where("product_family = ?", "Provisioned IOPS").
		Where("region_code = ?", dbStorage.RegionCode).
		Where("deployment_option = ?", dbStorage.DeploymentOption).
		Where("group_description = ?", "RDS Provisioned IOPS").
		Where("database_engine = ?", dbStorage.DatabaseEngine)
	if len(dbStorage.DatabaseEdition) > 0 {
		tx = tx.Where("database_edition = ?", dbStorage.DatabaseEdition)
	}
	tx = tx.Order("price_per_unit asc")
	var iopsStorage model.RDSDBStorage
	err := tx.First(&iopsStorage).Error
	if err != nil {
		return 0, tx.Error
	}
	iopsCost = iopsStorage.PricePerUnit * float64(*iops)

	return sizeCost + iopsCost, nil
}

func (r *RDSDBStorageRepoImpl) getIo2TotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32, iops *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeIO2) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)
	iopsCost := 0.0
	tx := r.db.Conn().Model(&model.RDSDBStorage{}).
		Where("product_family = ?", "Provisioned IOPS").
		Where("region_code = ?", dbStorage.RegionCode).
		Where("deployment_option = ?", dbStorage.DeploymentOption).
		Where("group_description = ?", "RDS Provisioned IO2 IOPS").
		Where("database_engine = ?", dbStorage.DatabaseEngine)
	if len(dbStorage.DatabaseEdition) > 0 {
		tx = tx.Where("database_edition = ?", dbStorage.DatabaseEdition)
	}
	tx = tx.Order("price_per_unit asc")
	var iopsStorage model.RDSDBStorage
	err := tx.First(&iopsStorage).Error
	if err != nil {
		return 0, tx.Error
	}
	iopsCost = iopsStorage.PricePerUnit * float64(*iops)

	return sizeCost + iopsCost, nil
}

func (r *RDSDBStorageRepoImpl) getAuroraGeneralPurposeTotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32, iops *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeGeneralPurposeAurora) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)

	millionIoPerMonth := math.Ceil(float64(*iops) * 30 * 24 * 60 * 60 / 1e6) // 30 days, 24 hours, 60 minutes, 60 seconds
	iopsCost := 0.0

	tx := r.db.Conn().Model(&model.RDSDBStorage{}).
		Where("product_family = ?", "System Operation").
		Where("region_code = ?", dbStorage.RegionCode).
		Where("group = ?", "Aurora I/O Operation").
		Where("database_engine IN ?", []string{dbStorage.DatabaseEngine, "Any"})
	tx = tx.Order("price_per_unit asc")
	var iopsStorage model.RDSDBStorage
	err := tx.First(&iopsStorage).Error
	if err != nil {
		return 0, tx.Error
	}
	iopsCost = iopsStorage.PricePerUnit * millionIoPerMonth

	return sizeCost + iopsCost, nil

}

func (r *RDSDBStorageRepoImpl) getAuroraIOOptimizedTotalPrice(dbStorage model.RDSDBStorage, volumeSize *int32) (float64, error) {
	if dbStorage.VolumeType != string(model.RDSDBStorageVolumeTypeIOOptimizedAurora) {
		return 0, errors.New("invalid volume type")
	}
	if dbStorage.MinVolumeSizeGb != 0 && *volumeSize < dbStorage.MinVolumeSizeGb {
		*volumeSize = dbStorage.MinVolumeSizeGb
	}
	sizeCost := dbStorage.PricePerUnit * float64(*volumeSize)

	return sizeCost, nil
}

func (r *RDSDBStorageRepoImpl) getFeasibleVolumeTypes(region string, engine, edition, clusterType string, volumeSize int32, iops int32, throughput float64, validTypes []model.RDSDBStorageVolumeType) ([]model.RDSDBStorage, error) {
	var res []model.RDSDBStorage
	tx := r.db.Conn().Model(&model.RDSDBStorage{}).
		Where("product_family = ?", "Database Storage").
		Where("region_code = ?", region).
		Where("deployment_option = ?", clusterType).
		Where("max_volume_size_gb >= ?", volumeSize).
		Where("max_iops >= ?", iops).
		Where("max_throughput_mb >= ?", throughput)

	if strings.Contains(strings.ToLower(engine), "aurora") {
		var filteredValidTypes []model.RDSDBStorageVolumeType
		for _, t := range validTypes {
			if t == model.RDSDBStorageVolumeTypeIOOptimizedAurora ||
				t == model.RDSDBStorageVolumeTypeGeneralPurposeAurora {
				filteredValidTypes = append(filteredValidTypes, t)
			}
		}
		if len(filteredValidTypes) == 0 {
			filteredValidTypes = []model.RDSDBStorageVolumeType{
				model.RDSDBStorageVolumeTypeIOOptimizedAurora,
				model.RDSDBStorageVolumeTypeGeneralPurposeAurora,
			}
		}
		validTypes = filteredValidTypes
		tx = tx.Where("database_engine IN ?", []string{engine, "Any"})
	} else {
		var filteredValidTypes []model.RDSDBStorageVolumeType
		for _, t := range validTypes {
			if t != model.RDSDBStorageVolumeTypeIOOptimizedAurora &&
				t != model.RDSDBStorageVolumeTypeGeneralPurposeAurora {
				filteredValidTypes = append(filteredValidTypes, t)
			}
		}
		validTypes = filteredValidTypes
		tx = tx.Where("database_engine = ?", engine)
		if len(edition) > 0 {
			tx = tx.Where("edition = ?", edition)
		}
	}

	if len(validTypes) > 0 {
		tx = tx.Where("volume_type IN ?", validTypes)
	}

	tx = tx.Find(&res)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return res, nil
}

func (r *RDSDBStorageRepoImpl) GetCheapestBySpecs(region string, engine, edition, clusterType string, volumeSize int32, iops int32, throughput float64, validTypes []model.RDSDBStorageVolumeType) (
	res *model.RDSDBStorage,
	cheapestVSize int32,
	cheapestIops int32,
	cheapestThroughput float64,
	err error) {

	res = nil
	err = nil
	cheapestVSize = volumeSize
	cheapestIops = iops
	cheapestThroughput = throughput

	volumes, err := r.getFeasibleVolumeTypes(region, engine, edition, clusterType, volumeSize, iops, throughput, validTypes)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	if len(volumes) == 0 {
		return nil, 0, 0, 0, nil
	}

	var cheapestPrice float64

	for _, v := range volumes {
		v := v
		vSize := volumeSize
		vIops := iops
		vThroughput := throughput
		var totalCost float64
		switch model.RDSDBStorageVolumeType(v.VolumeType) {
		case model.RDSDBStorageVolumeTypeMagnetic:
			totalCost, err = r.getMagneticTotalPrice(v, &vSize, &vIops)
		case model.RDSDBStorageVolumeTypeGP2:
			totalCost, err = r.getGp2TotalPrice(v, &vSize)
		case model.RDSDBStorageVolumeTypeGP3:
			totalCost, err = r.getGp3TotalPrice(v, &vSize, &vIops, &vThroughput)
		case model.RDSDBStorageVolumeTypeIO1:
			totalCost, err = r.getIo1TotalPrice(v, &vSize, &vIops)
		case model.RDSDBStorageVolumeTypeIO2:
			totalCost, err = r.getIo2TotalPrice(v, &vSize, &vIops)
		case model.RDSDBStorageVolumeTypeGeneralPurposeAurora:
			totalCost, err = r.getAuroraGeneralPurposeTotalPrice(v, &vSize, &vIops)
		case model.RDSDBStorageVolumeTypeIOOptimizedAurora:
			totalCost, err = r.getAuroraIOOptimizedTotalPrice(v, &vSize)
		}

		if err != nil {
			r.logger.Error("failed to calculate total cost", zap.Error(err), zap.Any("volume", v))
			return nil, 0, 0, 0, err
		}

		if res == nil || totalCost < cheapestPrice {
			res = &v
			cheapestVSize = vSize
			cheapestIops = vIops
			cheapestThroughput = vThroughput
			cheapestPrice = totalCost
		}
	}

	return res, cheapestVSize, cheapestIops, cheapestThroughput, nil
}
