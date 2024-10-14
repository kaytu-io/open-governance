package aws

import (
	"github.com/opengovern/opengovernance/pkg/workspace/api"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/aws/region"
	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/price"
	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/product"
	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/query"
	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/util"
)

// DBInstance represents an RDS database instance definition that can be cost-estimated.
type DBInstance struct {
	providerKey string

	region       region.Code
	instanceType string

	// databaseEngine can be one of "Aurora MySQL", "MariaDB", "MySQL", "PostgreSQL", "Oracle", "SQL Server".
	databaseEngine string

	// databaseEdition is only valid for Oracle and SQL Server and denotes an edition of the database.
	databaseEdition string

	// licenseModel is only valid for Oracle and SQL Server and can be either "License included" or "Bring your own license".
	licenseModel string

	// deploymentOption can be either "Single-AZ" or "Multi-AZ".
	deploymentOption string

	// storageType can be either "standard" (magnetic), "io1" (provisioned IOPS) or "gp2" (general purpose).
	storageType string

	// allocatedStorage is how much storage should be allocated for the database, in GB.
	allocatedStorage decimal.Decimal

	// storageIOPS is only valid for Provisioned IOPS types of storage and denotes the amount of IOPS allocated.
	storageIOPS decimal.Decimal
}

type dbInstanceValues struct {
	RegionCode       string  `mapstructure:"region_code"`
	InstanceClass    string  `mapstructure:"instance_class"`
	AvailabilityZone string  `mapstructure:"availability_zone"`
	Engine           string  `mapstructure:"engine"`
	LicenseModel     string  `mapstructure:"license_model"`
	MultiAZ          bool    `mapstructure:"multi_az"`
	AllocatedStorage float64 `mapstructure:"allocated_storage"`
	StorageType      string  `mapstructure:"storage_type"`
	IOPS             float64 `mapstructure:"iops"`
}

type dbType struct {
	engine, edition string
}

var dbTypeMap = map[string]dbType{
	"aurora":            {"Aurora MySQL", ""},
	"aurora-postgresql": {"Aurora MySQL", ""},
	"mariadb":           {"MariaDB", ""},
	"postgresql":        {"MySQL", ""},
	"postgres":          {"PostgreSQL", ""},
	"oracle-se":         {"Oracle", "Standard"},
	"oracle-se1":        {"Oracle", "Standard One"},
	"oracle-se2":        {"Oracle", "Standard Two"},
	"oracle-ee":         {"Oracle", "Enterprise"},
	"sqlserver-se":      {"SQL Server", "Standard"},
	"sqlserver-ee":      {"SQL Server", "Enterprise"},
	"sqlserver-ex":      {"SQL Server", "Express"},
	"sqlserver-web":     {"SQL Server", "Web"},
}
var licenseModelMap = map[string]string{
	"license-included":       "License included",
	"bring-your-own-license": "Bring your own license",
}

func decodeDBInstanceValues(request api.GetRDSInstanceRequest) dbInstanceValues {
	return dbInstanceValues{
		RegionCode:       request.RegionCode,
		InstanceClass:    "dbinstance",
		AvailabilityZone: request.RegionCode,
		Engine:           request.InstanceEngine,
		LicenseModel:     request.InstanceLicenseModel,
		MultiAZ:          request.InstanceMultiAZ,
		AllocatedStorage: request.AllocatedStorage,
		StorageType:      request.StorageType,
		IOPS:             request.IOPs,
	}
}

// NewInstance creates a new Instance from Terraform values.
func (p *Provider) newDBInstance(vals dbInstanceValues) *DBInstance {
	dbType := dbTypeMap[vals.Engine]
	licenseModel := licenseModelMap[vals.LicenseModel]

	deploymentOption := "Single-AZ"
	if vals.MultiAZ {
		deploymentOption = "Multi-AZ"
	}

	inst := &DBInstance{
		providerKey:      p.key,
		region:           region.Code(vals.RegionCode),
		instanceType:     vals.InstanceClass,
		databaseEngine:   dbType.engine,
		databaseEdition:  dbType.edition,
		licenseModel:     licenseModel,
		deploymentOption: deploymentOption,
		allocatedStorage: decimal.NewFromFloat(vals.AllocatedStorage),
		storageType:      vals.StorageType,
		storageIOPS:      decimal.NewFromFloat(vals.IOPS),
	}

	if reg := region.NewFromZone(vals.AvailabilityZone); reg.Valid() {
		inst.region = reg
	}

	return inst
}

// Components returns the price component queries that make up this Instance.
func (inst *DBInstance) Components() []query.Component {
	components := []query.Component{inst.databaseInstanceComponent(), inst.storageComponent()}

	if strings.HasPrefix(inst.storageType, "io") {
		components = append(components, inst.iopsComponent())
	}

	return components
}

func (inst *DBInstance) databaseInstanceComponent() query.Component {
	instClass := inst.instanceType
	attrFilters := []*product.AttributeFilter{
		{Key: "InstanceType", Value: util.StringPtr(inst.instanceType)},
		{Key: "DeploymentOption", Value: util.StringPtr(inst.deploymentOption)},
		{Key: "DatabaseEngine", Value: util.StringPtr(inst.databaseEngine)},
	}

	if inst.databaseEdition != "" {
		f := &product.AttributeFilter{Key: "DatabaseEdition", Value: util.StringPtr(inst.databaseEdition)}
		attrFilters = append(attrFilters, f)
	}

	if inst.licenseModel != "" {
		f := &product.AttributeFilter{Key: "LicenseModel", Value: util.StringPtr(inst.licenseModel)}
		attrFilters = append(attrFilters, f)
	}

	return query.Component{
		Name:           "Database instance",
		Details:        []string{inst.deploymentOption, instClass},
		HourlyQuantity: decimal.NewFromInt(1),
		ProductFilter: &product.Filter{
			Provider:         util.StringPtr(inst.providerKey),
			Service:          util.StringPtr("AmazonRDS"),
			Family:           util.StringPtr("Database Instance"),
			Location:         util.StringPtr(inst.region.String()),
			AttributeFilters: attrFilters,
		},
		PriceFilter: &price.Filter{
			Unit: util.StringPtr("Hrs"),
			AttributeFilters: []*price.AttributeFilter{
				{Key: "TermType", Value: util.StringPtr("OnDemand")},
			},
		},
	}
}

func (inst *DBInstance) storageComponent() query.Component {
	var volumeType string
	switch inst.storageType {
	case "standard":
		volumeType = "Magnetic"
	case "io1", "io2":
		volumeType = "Provisioned IOPS"
	default:
		volumeType = "General Purpose"
	}

	return query.Component{
		Name:            "Database storage",
		Details:         []string{volumeType},
		MonthlyQuantity: inst.allocatedStorage,
		Unit:            "GB",
		ProductFilter: &product.Filter{
			Provider: util.StringPtr(inst.providerKey),
			Service:  util.StringPtr("AmazonRDS"),
			Family:   util.StringPtr("Database Storage"),
			Location: util.StringPtr(inst.region.String()),
			AttributeFilters: []*product.AttributeFilter{
				{Key: "DeploymentOption", Value: util.StringPtr(inst.deploymentOption)},
				{Key: "VolumeType", Value: util.StringPtr(volumeType)},
			},
		},
	}
}

func (inst *DBInstance) iopsComponent() query.Component {
	return query.Component{
		Name:            "Database IOPS",
		MonthlyQuantity: inst.storageIOPS,
		Unit:            "IOPS",
		ProductFilter: &product.Filter{
			Provider: util.StringPtr(inst.providerKey),
			Service:  util.StringPtr("AmazonRDS"),
			Family:   util.StringPtr("Provisioned IOPS"),
			Location: util.StringPtr(inst.region.String()),
			AttributeFilters: []*product.AttributeFilter{
				{Key: "DeploymentOption", Value: util.StringPtr(inst.deploymentOption)},
			},
		},
	}
}
