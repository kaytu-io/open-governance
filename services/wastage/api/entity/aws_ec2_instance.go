package entity

import (
	types2 "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type EC2Placement struct {
	Tenancy          types.Tenancy `json:"tenancy"`
	AvailabilityZone string        `json:"availabilityZone"`
	HashedHostId     string        `json:"hashedHostId"`
}

type EC2Instance struct {
	HashedInstanceId  string                      `json:"hashedInstanceId"`
	State             types.InstanceStateName     `json:"state"`
	InstanceType      types.InstanceType          `json:"instanceType"`
	Platform          string                      `json:"platform"`
	ThreadsPerCore    int32                       `json:"threadsPerCore"`
	CoreCount         int32                       `json:"coreCount"`
	EbsOptimized      bool                        `json:"ebsOptimized"`
	InstanceLifecycle types.InstanceLifecycleType `json:"instanceLifecycle"`
	Monitoring        *types.MonitoringState      `json:"monitoring"`
	Placement         *EC2Placement               `json:"placement"`
	UsageOperation    string                      `json:"usageOperation"`
	Tenancy           types.Tenancy               `json:"tenancy"`
}

type EC2Volume struct {
	HashedVolumeId   string           `json:"hashedVolumeId"`
	VolumeType       types.VolumeType `json:"volumeType"`
	Size             *int32           `json:"size"`
	Iops             *int32           `json:"iops"`
	AvailabilityZone *string          `json:"availabilityZone"`
	Throughput       *float64         `json:"throughput"`
}

type EC2InstanceWastageRequest struct {
	RequestId      *string                                  `json:"requestId"`
	CliVersion     *string                                  `json:"cliVersion"`
	Identification map[string]string                        `json:"identification"`
	Instance       EC2Instance                              `json:"instance"`
	Volumes        []EC2Volume                              `json:"volumes"`
	VolumeCount    int                                      `json:"volumeCount"`
	Metrics        map[string][]types2.Datapoint            `json:"metrics"`
	VolumeMetrics  map[string]map[string][]types2.Datapoint `json:"volumeMetrics"`
	Region         string                                   `json:"region"`
	Preferences    map[string]*string                       `json:"preferences"`
	Loading        bool                                     `json:"loading"`
}

type RightsizingEC2Instance struct {
	InstanceType      string             `json:"instanceType"`
	Region            string             `json:"region"`
	Cost              float64            `json:"cost"`
	CostComponents    map[string]float64 `json:"costComponents"`
	Processor         string             `json:"processor"`
	Architecture      string             `json:"architecture"`
	VCPU              int64              `json:"vCPU"`
	Memory            float64            `json:"memory"`
	EBSBandwidth      string             `json:"ebsBandwidth"`
	EBSIops           string             `json:"ebsIops"`
	NetworkThroughput string             `json:"networkThroughput"`
	ENASupported      string             `json:"enaSupported"`
	LicensePrice      float64            `json:"licensePrice"`
	License           string             `json:"license"`
}

type Usage struct {
	Avg  *float64
	Min  *float64
	Max  *float64
	Last *types2.Datapoint
}

type RightSizingRecommendation struct {
	Current     RightsizingEC2Instance  `json:"current"`
	Recommended *RightsizingEC2Instance `json:"recommended"`

	VCPU              Usage `json:"vCPU"`
	Memory            Usage `json:"memory"`
	EBSBandwidth      Usage `json:"ebsBandwidth"`
	EBSIops           Usage `json:"ebsIops"`
	NetworkThroughput Usage `json:"networkThroughput"`

	Description string `json:"description"`
}

type RightsizingEBSVolume struct {
	Tier                  types.VolumeType   `json:"tier"`
	VolumeSize            *int32             `json:"volumeSize"`
	BaselineIOPS          int32              `json:"baselineIOPS"`
	ProvisionedIOPS       *int32             `json:"provisionedIOPS"`
	BaselineThroughput    float64            `json:"baselineThroughput"`
	ProvisionedThroughput *float64           `json:"provisionedThroughput"`
	Cost                  float64            `json:"cost"`
	CostComponents        map[string]float64 `json:"costComponents"`
}

type EBSVolumeRecommendation struct {
	Current     RightsizingEBSVolume
	Recommended *RightsizingEBSVolume

	IOPS       Usage `json:"iops"`
	Throughput Usage `json:"throughput"`

	Description string `json:"description"`
}

type EC2InstanceWastageResponse struct {
	RightSizing       RightSizingRecommendation          `json:"rightSizing"`
	VolumeRightSizing map[string]EBSVolumeRecommendation `json:"volumes"`
}
