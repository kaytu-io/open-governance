package repo

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/kaytu-io/open-governance/services/wastage/db/connector"
	"github.com/kaytu-io/open-governance/services/wastage/db/model"
	"github.com/sony/sonyflake"
	"gorm.io/gorm"
	"math"
	"time"
)

type EBSVolumeTypeRepo interface {
	Create(tableName string, tx *gorm.DB, m *model.EBSVolumeType) error
	Get(id uint) (*model.EBSVolumeType, error)
	Update(tableName string, id uint, m model.EBSVolumeType) error
	Delete(tableName string, id uint) error
	List() ([]model.EBSVolumeType, error)
	Truncate(tx *gorm.DB) error
	GetCheapestTypeWithSpecs(ctx context.Context, region string, volumeSize int32, iops int32, throughput float64, validTypes []types.VolumeType) (types.VolumeType, int32, int32, float64, string, error)
	MoveViewTransaction(tableName string) error
	RemoveOldTables(tableName string) error
	CreateNewTable() (string, error)
}

type EBSVolumeTypeRepoImpl struct {
	db *connector.Database

	viewName string
}

func NewEBSVolumeTypeRepo(db *connector.Database) EBSVolumeTypeRepo {
	stmt := &gorm.Statement{DB: db.Conn()}
	stmt.Parse(&model.EBSVolumeType{})

	return &EBSVolumeTypeRepoImpl{
		db: db,

		viewName: stmt.Schema.Table,
	}
}

func (r *EBSVolumeTypeRepoImpl) Create(tableName string, tx *gorm.DB, m *model.EBSVolumeType) error {
	if tx == nil {
		tx = r.db.Conn()
	}
	tx = tx.Table(tableName)
	return tx.Create(&m).Error
}

func (r *EBSVolumeTypeRepoImpl) Get(id uint) (*model.EBSVolumeType, error) {
	var m model.EBSVolumeType
	tx := r.db.Conn().Table(r.viewName).Where("id=?", id).First(&m)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, tx.Error
	}
	return &m, nil
}

func (r *EBSVolumeTypeRepoImpl) Update(tableName string, id uint, m model.EBSVolumeType) error {
	return r.db.Conn().Table(tableName).Where("id=?", id).Updates(&m).Error
}

func (r *EBSVolumeTypeRepoImpl) Delete(tableName string, id uint) error {
	return r.db.Conn().Unscoped().Table(tableName).Delete(&model.EBSVolumeType{}, id).Error
}

func (r *EBSVolumeTypeRepoImpl) List() ([]model.EBSVolumeType, error) {
	var ms []model.EBSVolumeType
	tx := r.db.Conn().Table(r.viewName).Find(&ms)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return ms, nil
}

func (r *EBSVolumeTypeRepoImpl) Truncate(tx *gorm.DB) error {
	if tx == nil {
		tx = r.db.Conn()
	}
	tx = tx.Unscoped().Where("1 = 1").Delete(&model.EBSVolumeType{})
	if tx.Error != nil {
		return tx.Error
	}
	return nil
}

func (r *EBSVolumeTypeRepoImpl) getDimensionCostsByRegionVolumeTypeAndChargeType(ctx context.Context, regionCode string, volumeType types.VolumeType, chargeType model.EBSVolumeChargeType) ([]model.EBSVolumeType, error) {
	var m []model.EBSVolumeType
	tx := r.db.Conn().Table(r.viewName).
		Where("region_code = ?", regionCode).
		Where("volume_type = ?", volumeType).
		Where("charge_type = ?", chargeType).
		Find(&m)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, tx.Error
	}
	return m, nil
}

func (r *EBSVolumeTypeRepoImpl) getIo1TotalPrice(ctx context.Context, region string, volumeSize int32, iops int32) (float64, string, error) {
	io1IopsPrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeIo1, model.ChargeTypeIOPS)
	if err != nil {
		return 0, "", err
	}
	io1Iops := 0.0
	for _, iops := range io1IopsPrices {
		io1Iops = iops.PricePerUnit
		break
	}
	io1SizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeIo1, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	io1Size := 0.0
	for _, sizes := range io1SizePrices {
		io1Size = sizes.PricePerUnit
		break
	}
	io1Price := io1Iops*float64(iops) + io1Size*float64(volumeSize)
	costBreakdown := fmt.Sprintf("Provisioned IOPS: $%.2f * %d + Size: $%.2f * %d", io1Iops, iops, io1Size, volumeSize)

	return io1Price, costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getIo2TotalPrice(ctx context.Context, region string, volumeSize int32, iops int32) (float64, string, error) {
	io2IopsPrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeIo2, model.ChargeTypeIOPS)
	if err != nil {
		return 0, "", err
	}
	io2IopsTier1 := 0.0
	io2IopsTier2 := 0.0
	io2IopsTier3 := 0.0
	for _, iops := range io2IopsPrices {
		switch iops.PriceGroup {
		case "EBS IOPS":
			io2IopsTier1 = iops.PricePerUnit
		case "EBS IOPS Tier 2":
			io2IopsTier2 = iops.PricePerUnit
		case "EBS IOPS Tier 3":
			io2IopsTier3 = iops.PricePerUnit
		}
	}
	io2SizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeIo2, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	io2Size := 0.0
	for _, sizes := range io2SizePrices {
		io2Size = sizes.PricePerUnit
		break
	}
	io2Price := io2Size * float64(volumeSize)
	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", io2Size, volumeSize)
	if iops > model.Io2ProvisionedIopsTier2UpperBound {
		io2Price += io2IopsTier3 * float64(iops-model.Io2ProvisionedIopsTier2UpperBound)
		iops = model.Io2ProvisionedIopsTier2UpperBound
		costBreakdown += fmt.Sprintf(" + IOPS Tier 3 (over %d): $%.2f * %d", model.Io2ProvisionedIopsTier2UpperBound, io2IopsTier3, iops-model.Io2ProvisionedIopsTier2UpperBound)
	}
	if iops > model.Io2ProvisionedIopsTier1UpperBound {
		io2Price += io2IopsTier2 * float64(iops-model.Io2ProvisionedIopsTier1UpperBound)
		iops = model.Io2ProvisionedIopsTier1UpperBound
		costBreakdown += fmt.Sprintf(" + IOPS Tier 2 (over %d under %d): $%.2f * %d", model.Io2ProvisionedIopsTier1UpperBound, model.Io2ProvisionedIopsTier2UpperBound, io2IopsTier2, iops-model.Io2ProvisionedIopsTier1UpperBound)
	}
	io2Price += io2IopsTier1 * float64(iops)
	costBreakdown += fmt.Sprintf(" + IOPS Tier 1 (under %d): $%.2f * %d", model.Io2ProvisionedIopsTier1UpperBound, io2IopsTier1, iops)

	return io2Price, "", nil
}

func (r *EBSVolumeTypeRepoImpl) getGp2TotalPrice(ctx context.Context, region string, volumeSize *int32, iops int32) (float64, string, error) {
	gp2Prices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeGp2, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	gp2Price := 0.0
	for _, gp2 := range gp2Prices {
		gp2Price = gp2.PricePerUnit
		break
	}

	if iops > 100 {
		minSizeReq := int32(math.Ceil(float64(iops) / model.Gp2IopsPerGiB))
		if minSizeReq > *volumeSize {
			*volumeSize = minSizeReq
		}
	}

	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", gp2Price, *volumeSize)

	return gp2Price * float64(*volumeSize), costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getGp3TotalPrice(ctx context.Context, region string, volumeSize int32, iops int32, throughput float64) (float64, string, error) {
	iops = max(iops-model.Gp3BaseIops, 0)
	throughput = max(throughput-model.Gp3BaseThroughput, 0.0)

	gp3SizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeGp3, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	gp3SizePrice := 0.0
	for _, gp3 := range gp3SizePrices {
		gp3SizePrice = gp3.PricePerUnit
		break
	}
	gp3IopsPrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeGp3, model.ChargeTypeIOPS)
	if err != nil {
		return 0, "", err
	}
	gp3IopsPrice := 0.0
	for _, gp3 := range gp3IopsPrices {
		gp3IopsPrice = gp3.PricePerUnit
		break
	}

	gp3ThroughputPrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeGp3, model.ChargeTypeThroughput)
	if err != nil {
		return 0, "", err
	}
	gp3ThroughputPrice := 0.0
	for _, gp3 := range gp3ThroughputPrices {
		gp3ThroughputPrice = gp3.PricePerUnit
		break
	}

	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", gp3SizePrice, volumeSize)
	if iops > 0 {
		costBreakdown += fmt.Sprintf(" + Provisioned IOPS (over %d): $%.2f * %d", model.Gp3BaseIops, gp3IopsPrice, iops)
	}
	if throughput > 0 {
		costBreakdown += fmt.Sprintf(" + Provisioned Throughput (over %d): $%.2f * %.2f", model.Gp3BaseThroughput, gp3ThroughputPrice, throughput)
	}

	return gp3SizePrice*float64(volumeSize) + gp3IopsPrice*float64(iops) + gp3ThroughputPrice*throughput, costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getSc1TotalPrice(ctx context.Context, region string, volumeSize int32) (float64, string, error) {
	sc1SizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeSc1, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	sc1SizePrice := 0.0
	for _, sc1 := range sc1SizePrices {
		sc1SizePrice = sc1.PricePerUnit
		break
	}

	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", sc1SizePrice, volumeSize)

	return sc1SizePrice * float64(volumeSize), costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getSt1TotalPrice(ctx context.Context, region string, volumeSize int32) (float64, string, error) {
	st1SizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeSt1, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	st1SizePrice := 0.0
	for _, st1 := range st1SizePrices {
		st1SizePrice = st1.PricePerUnit
		break
	}

	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", st1SizePrice, volumeSize)

	return st1SizePrice * float64(volumeSize), costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getStandardTotalPrice(ctx context.Context, region string, volumeSize int32) (float64, string, error) {
	standardSizePrices, err := r.getDimensionCostsByRegionVolumeTypeAndChargeType(ctx, region, types.VolumeTypeStandard, model.ChargeTypeSize)
	if err != nil {
		return 0, "", err
	}
	standardSizePrice := 0.0
	for _, standard := range standardSizePrices {
		standardSizePrice = standard.PricePerUnit
		break
	}

	costBreakdown := fmt.Sprintf("Size: $%.2f * %d", standardSizePrice, volumeSize)

	return standardSizePrice * float64(volumeSize), costBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) getFeasibleVolumeTypes(ctx context.Context, region string, volumeSize int32, iops int32, throughput float64, validTypes []types.VolumeType) ([]model.EBSVolumeType, error) {
	var res []model.EBSVolumeType
	tx := r.db.Conn().Table(r.viewName).WithContext(ctx).
		Where("region_code = ?", region).
		Where("max_iops >= ?", iops).
		Where("max_throughput >= ?", throughput).
		Where("max_size >= ?", volumeSize)
	if len(validTypes) > 0 {
		tx = tx.Where("volume_type IN ?", validTypes)
	}
	tx = tx.Find(&res)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return res, nil
}

func (r *EBSVolumeTypeRepoImpl) GetCheapestTypeWithSpecs(ctx context.Context, region string, volumeSize int32, iops int32, throughput float64, validTypes []types.VolumeType) (types.VolumeType, int32, int32, float64, string, error) {
	volumeTypes, err := r.getFeasibleVolumeTypes(ctx, region, volumeSize, iops, throughput, validTypes)
	if err != nil {
		return "", 0, 0, 0, "", err
	}

	if len(volumeTypes) == 0 {
		return "", 0, 0, 0, "", errors.New("no feasible volume types found")
	}

	minPrice := 0.0
	resVolumeType := ""
	rescostBreakdown := ""
	resBaselineIOPS := int32(0)
	resBaselineThroughput := 0.0
	resVolumeSize := volumeSize
	for _, vt := range volumeTypes {
		var price float64
		var costBreakdown string
		var volIops int32
		var volThroughput float64
		var volSize int32 = volumeSize
		switch vt.VolumeType {
		case types.VolumeTypeIo1:
			price, costBreakdown, err = r.getIo1TotalPrice(ctx, region, volSize, iops)
			volIops = 0
			volThroughput = float64(vt.MaxThroughput)
		case types.VolumeTypeIo2:
			price, costBreakdown, err = r.getIo2TotalPrice(ctx, region, volSize, iops)
			volIops = 0
			volThroughput = float64(vt.MaxThroughput)
		case types.VolumeTypeGp2:
			price, costBreakdown, err = r.getGp2TotalPrice(ctx, region, &volSize, iops)
			volIops = vt.MaxIops
			volThroughput = float64(vt.MaxThroughput)
		case types.VolumeTypeGp3:
			price, costBreakdown, err = r.getGp3TotalPrice(ctx, region, volSize, iops, throughput)
			volIops = model.Gp3BaseIops
			volThroughput = model.Gp3BaseThroughput
		case types.VolumeTypeSc1:
			price, costBreakdown, err = r.getSc1TotalPrice(ctx, region, volSize)
			volIops = vt.MaxIops
			volThroughput = float64(vt.MaxThroughput)
		case types.VolumeTypeSt1:
			price, costBreakdown, err = r.getSt1TotalPrice(ctx, region, volSize)
			volIops = vt.MaxIops
			volThroughput = float64(vt.MaxThroughput)
		case types.VolumeTypeStandard:
			price, costBreakdown, err = r.getStandardTotalPrice(ctx, region, volSize)
			volIops = vt.MaxIops
			volThroughput = float64(vt.MaxThroughput)
		}
		if err != nil {
			return "", 0, 0, 0, "", err
		}
		if resVolumeType == "" || price < minPrice {
			minPrice = price
			resVolumeType = string(vt.VolumeType)
			resBaselineIOPS = volIops
			resBaselineThroughput = volThroughput
			resVolumeSize = volSize
			rescostBreakdown = costBreakdown
		}
	}

	return types.VolumeType(resVolumeType), resVolumeSize, resBaselineIOPS, resBaselineThroughput, rescostBreakdown, nil
}

func (r *EBSVolumeTypeRepoImpl) CreateNewTable() (string, error) {
	sf := sonyflake.NewSonyflake(sonyflake.Settings{})
	var ec2InstanceTypeTable string
	for {
		id, err := sf.NextID()
		if err != nil {
			return "", err
		}

		ec2InstanceTypeTable = fmt.Sprintf("%s_%s_%d",
			r.viewName,
			time.Now().Format("2006_01_02"),
			id,
		)
		var c int32
		tx := r.db.Conn().Raw(fmt.Sprintf(`
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema
		AND table_name = '%s';
	`, ec2InstanceTypeTable)).First(&c)
		if tx.Error != nil {
			return "", err
		}
		if c == 0 {
			break
		}
	}

	err := r.db.Conn().Table(ec2InstanceTypeTable).AutoMigrate(&model.EBSVolumeType{})
	if err != nil {
		return "", err
	}
	return ec2InstanceTypeTable, nil
}

func (r *EBSVolumeTypeRepoImpl) MoveViewTransaction(tableName string) error {
	tx := r.db.Conn().Begin()
	var err error
	defer func() {
		_ = tx.Rollback()
	}()

	dropViewQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", r.viewName)
	tx = tx.Exec(dropViewQuery)
	err = tx.Error
	if err != nil {
		return err
	}

	createViewQuery := fmt.Sprintf(`
  CREATE OR REPLACE VIEW %s AS
  SELECT *
  FROM %s;
`, r.viewName, tableName)

	tx = tx.Exec(createViewQuery)
	err = tx.Error
	if err != nil {
		return err
	}

	tx = tx.Commit()
	err = tx.Error
	if err != nil {
		return err
	}
	return nil
}

func (r *EBSVolumeTypeRepoImpl) getOldTables(currentTableName string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = current_schema
		AND table_name LIKE '%s_%%' AND table_name <> '%s';
	`, r.viewName, currentTableName)

	var tableNames []string
	tx := r.db.Conn().Raw(query).Find(&tableNames)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return tableNames, nil
}

func (r *EBSVolumeTypeRepoImpl) RemoveOldTables(currentTableName string) error {
	tableNames, err := r.getOldTables(currentTableName)
	if err != nil {
		return err
	}
	for _, tn := range tableNames {
		err = r.db.Conn().Migrator().DropTable(tn)
		if err != nil {
			return err
		}
	}
	return nil
}
