package aws

import (
	"github.com/kaytu-io/open-governance/pkg/workspace/api"
	"github.com/shopspring/decimal"

	"github.com/kaytu-io/open-governance/pkg/workspace/costestimator/aws/region"
	"github.com/kaytu-io/open-governance/pkg/workspace/costestimator/product"
	"github.com/kaytu-io/open-governance/pkg/workspace/costestimator/query"
	"github.com/kaytu-io/open-governance/pkg/workspace/costestimator/util"
)

// Volume represents an EBS volume that can be cost-estimated.
type Volume struct {
	provider *Provider
	region   region.Code

	volumeType string
	size       decimal.Decimal
	iops       decimal.Decimal
}

// volumeValues represents the structure of Terraform values for aws_ebs_volume resource.
type volumeValues struct {
	AvailabilityZone string  `mapstructure:"availability_zone"`
	Type             string  `mapstructure:"type"`
	Size             float64 `mapstructure:"size"`
	IOPS             float64 `mapstructure:"iops"`
}

// decodeVolumeValues decodes and returns volumeValues from a Terraform values map.
func decodeVolumeValues(request api.GetEC2VolumeCostRequest) volumeValues {
	return volumeValues{
		AvailabilityZone: request.RegionCode,
		Type:             request.Type,
		Size:             request.Size,
		IOPS:             request.IOPs,
	}
}

// newVolume creates a new Volume from volumeValues.
func (p *Provider) newVolume(vals volumeValues) *Volume {
	v := &Volume{
		provider:   p,
		region:     region.Code(vals.AvailabilityZone),
		volumeType: "gp3",
		size:       decimal.NewFromInt(8),
		iops:       decimal.NewFromInt(16000),
	}

	if reg := region.NewFromZone(vals.AvailabilityZone); reg.Valid() {
		v.region = reg
	}

	if vals.Type != "" {
		v.volumeType = vals.Type
	}

	if vals.Size > 0 {
		v.size = decimal.NewFromFloat(vals.Size)
	}

	if vals.IOPS > 0 {
		v.iops = decimal.NewFromFloat(vals.IOPS)
	}

	return v
}

// Components returns the price component queries that make up the Volume.
func (v *Volume) Components() []query.Component {
	comps := []query.Component{v.storageComponent()}

	if v.volumeType == "io1" || v.volumeType == "io2" {
		comps = append(comps, v.iopsComponent())
	}

	return comps
}

func (v *Volume) storageComponent() query.Component {
	return query.Component{
		Name:            "Storage",
		MonthlyQuantity: v.size,
		Unit:            "GB",
		Details:         []string{v.volumeType},
		ProductFilter: &product.Filter{
			Provider: util.StringPtr(v.provider.key),
			Service:  util.StringPtr("AmazonEC2"),
			Family:   util.StringPtr("Storage"),
			Location: util.StringPtr(v.region.String()),
			AttributeFilters: []*product.AttributeFilter{
				{Key: "VolumeAPIName", Value: util.StringPtr(v.volumeType)},
			},
		},
	}
}

func (v *Volume) iopsComponent() query.Component {
	return query.Component{
		Name:            "Provisioned IOPS",
		MonthlyQuantity: v.iops,
		Unit:            "IOPS",
		ProductFilter: &product.Filter{
			Provider: util.StringPtr(v.provider.key),
			Service:  util.StringPtr("AmazonEC2"),
			Family:   util.StringPtr("System Operation"),
			Location: util.StringPtr(v.region.String()),
			AttributeFilters: []*product.AttributeFilter{
				{Key: "VolumeAPIName", Value: util.StringPtr(v.volumeType)},
				{Key: "UsageType", ValueRegex: util.StringPtr("^EBS:VolumeP-IOPS")},
			},
		},
	}
}
