package recommendation

import (
	"context"
	"errors"
	"fmt"
	types2 "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/kaytu-io/kaytu-engine/pkg/utils"
	"github.com/kaytu-io/kaytu-engine/services/wastage/api/entity"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/model"
	"github.com/kaytu-io/kaytu-engine/services/wastage/recommendation/preferences/ec2instance"
	aws "github.com/kaytu-io/plugin-aws/plugin/proto/src/golang"
	"github.com/labstack/echo/v4"
	"github.com/sashabaranov/go-openai"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Service) EC2InstanceRecommendationGrpc(
	ctx context.Context,
	region string,
	instance *aws.EC2Instance,
	volumes []*aws.EC2Volume,
	metrics map[string]*aws.Metric,
	volumeMetrics map[string]*aws.VolumeMetrics,
	preferences map[string]*wrapperspb.StringValue,
	usageAverageType UsageAverageType,
) (*aws.EC2InstanceRightSizingRecommendation, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(fmt.Sprintf("recovered from panic: %v", r))
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()
	if instance == nil {
		s.logger.Error("Instance is nil")
	}
	var monitoring *types.MonitoringState
	if instance.Monitoring != nil {
		monitoringTmp := types.MonitoringState(instance.Monitoring.GetValue())
		monitoring = &monitoringTmp
	}
	var placement *entity.EC2Placement
	if instance.Placement != nil {
		placement = &entity.EC2Placement{
			AvailabilityZone: instance.Placement.AvailabilityZone,
			Tenancy:          types.Tenancy(instance.Placement.Tenancy),
			HashedHostId:     instance.Placement.HashedHostId,
		}
	}
	newInstance := entity.EC2Instance{
		HashedInstanceId:  instance.HashedInstanceId,
		State:             types.InstanceStateName(instance.State),
		InstanceType:      types.InstanceType(instance.InstanceType),
		Platform:          instance.Platform,
		ThreadsPerCore:    instance.ThreadsPerCore,
		CoreCount:         instance.CoreCount,
		EbsOptimized:      instance.EbsOptimized,
		InstanceLifecycle: types.InstanceLifecycleType(instance.InstanceLifecycle),
		Monitoring:        monitoring,
		Placement:         placement,
		UsageOperation:    instance.UsageOperation,
		Tenancy:           types.Tenancy(instance.Tenancy),
	}

	var newVolumes []entity.EC2Volume
	for _, v := range volumes {
		newVolumes = append(newVolumes, entity.EC2Volume{
			HashedVolumeId:   v.HashedVolumeId,
			VolumeType:       types.VolumeType(v.VolumeType),
			Iops:             WrappedToInt32(v.Iops),
			Size:             WrappedToInt32(v.Size),
			Throughput:       WrappedToFloat64(v.Throughput),
			AvailabilityZone: WrappedToString(v.AvailabilityZone),
		})
	}
	for k, m := range metrics {
		if m.Metric != nil {
			s.logger.Info("Metric Info", zap.String("key", k), zap.Any("length", len(m.Metric)))
		}
	}
	newMetrics := convertMetrics(metrics)
	newVolumeMetrics := make(map[string]map[string][]types2.Datapoint)
	for k, v := range volumeMetrics {
		newVolumeMetrics[k] = convertMetrics(v.Metrics)
	}
	newPreferences := make(map[string]*string)
	for k, v := range preferences {
		newPreferences[k] = WrappedToString(v)
	}

	s.logger.Info("EC2InstanceRecommendation parameters", zap.String("region", region), zap.Any("instance", newInstance),
		zap.Any("volumes", newVolumes), zap.Any("metrics len", len(metrics)), zap.Any("volumeMetrics len", len(newVolumeMetrics)),
		zap.Any("preferences", newPreferences), zap.Any("usageAverageType", usageAverageType))

	result, err := s.EC2InstanceRecommendation(ctx, region, newInstance, newVolumes, newMetrics, newVolumeMetrics, newPreferences, usageAverageType)
	if err != nil {
		return nil, err
	}
	return &aws.EC2InstanceRightSizingRecommendation{
		Current:           convertRightsizingEC2Instance(&result.Current),
		Recommended:       convertRightsizingEC2Instance(result.Recommended),
		Vcpu:              convertUsage(&result.VCPU),
		Memory:            convertUsage(&result.Memory),
		EbsBandwidth:      convertUsage(&result.EBSBandwidth),
		EbsIops:           convertUsage(&result.EBSIops),
		NetworkThroughput: convertUsage(&result.NetworkThroughput),
		Description:       result.Description,
	}, nil
}

func convertMetrics(metrics map[string]*aws.Metric) map[string][]types2.Datapoint {
	newMetrics := make(map[string][]types2.Datapoint)
	for k, v := range metrics {
		var datapoints []types2.Datapoint
		for _, d := range v.Metric {
			var timestamp *time.Time
			if d.Timestamp != nil {
				timestampTmp := d.Timestamp.AsTime()
				timestamp = &timestampTmp
			}
			datapoints = append(datapoints, types2.Datapoint{
				Average:     WrappedToFloat64(d.Average),
				Maximum:     WrappedToFloat64(d.Maximum),
				Minimum:     WrappedToFloat64(d.Minimum),
				SampleCount: WrappedToFloat64(d.SampleCount),
				Sum:         WrappedToFloat64(d.Sum),
				Timestamp:   timestamp,
			})
		}
		newMetrics[k] = datapoints
	}
	return newMetrics
}

func convertRightsizingEC2Instance(rightSizing *entity.RightsizingEC2Instance) *aws.RightsizingEC2Instance {
	if rightSizing == nil {
		return nil
	}
	return &aws.RightsizingEC2Instance{
		InstanceType:      rightSizing.InstanceType,
		Region:            rightSizing.Region,
		Cost:              rightSizing.Cost,
		CostComponents:    rightSizing.CostComponents,
		Processor:         rightSizing.Processor,
		Architecture:      rightSizing.Architecture,
		Vcpu:              rightSizing.VCPU,
		Memory:            rightSizing.Memory,
		EbsBandwidth:      rightSizing.EBSBandwidth,
		EbsIops:           rightSizing.EBSIops,
		NetworkThroughput: rightSizing.NetworkThroughput,
		EnaSupported:      rightSizing.ENASupported,
		LicensePrice:      rightSizing.LicensePrice,
		License:           rightSizing.License,
	}
}

func convertUsage(usage *entity.Usage) *aws.Usage {
	if usage == nil {
		return nil
	}
	newUsage := aws.Usage{
		Avg: Float64ToWrapper(usage.Avg),
		Max: Float64ToWrapper(usage.Max),
		Min: Float64ToWrapper(usage.Min),
	}
	if usage.Last != nil {
		last := aws.Datapoint{
			Maximum:     Float64ToWrapper(usage.Last.Maximum),
			Minimum:     Float64ToWrapper(usage.Last.Minimum),
			Average:     Float64ToWrapper(usage.Last.Average),
			SampleCount: Float64ToWrapper(usage.Last.SampleCount),
			Sum:         Float64ToWrapper(usage.Last.Sum),
		}
		if usage.Last.Timestamp != nil {
			last.Timestamp = timestamppb.New(*usage.Last.Timestamp)
		}
		newUsage.Last = &last
	}
	return &newUsage
}

func (s *Service) EC2InstanceRecommendation(
	ctx context.Context,
	region string,
	instance entity.EC2Instance,
	volumes []entity.EC2Volume,
	metrics map[string][]types2.Datapoint,
	volumeMetrics map[string]map[string][]types2.Datapoint,
	preferences map[string]*string,
	usageAverageType UsageAverageType,
) (*entity.RightSizingRecommendation, error) {
	cpuUsage := extractUsage(metrics["CPUUtilization"], usageAverageType)
	memoryUsage := extractUsage(metrics["mem_used_percent"], usageAverageType)
	networkUsage := extractUsage(sumMergeDatapoints(metrics["NetworkIn"], metrics["NetworkOut"]), usageAverageType)

	var ebsThroughputDatapoints []types2.Datapoint
	var ebsIopsDatapoints []types2.Datapoint
	for _, v := range volumeMetrics {
		ebsThroughputDatapoints = sumMergeDatapoints(sumMergeDatapoints(v["VolumeReadBytes"], v["VolumeWriteBytes"]), ebsThroughputDatapoints)
		ebsIopsDatapoints = sumMergeDatapoints(sumMergeDatapoints(v["VolumeReadOps"], v["VolumeWriteOps"]), ebsIopsDatapoints)
	}
	ebsThroughputUsage := extractUsage(ebsThroughputDatapoints, usageAverageType)
	ebsIopsUsage := extractUsage(ebsIopsDatapoints, usageAverageType)

	currentInstanceTypeList, err := s.ec2InstanceRepo.ListByInstanceType(ctx, string(instance.InstanceType), instance.UsageOperation, region)
	if err != nil {
		err = fmt.Errorf("failed to list instances by types: %s", err.Error())
		return nil, err
	}
	if len(currentInstanceTypeList) == 0 {
		return nil, echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("instance type not found: %s", string(instance.InstanceType)))
	}
	currentInstanceType := currentInstanceTypeList[0]
	newCtx := context.Background()
	currentCost, currentComponentCost, err := s.costSvc.GetEC2InstanceCost(newCtx, region, instance, volumes, metrics)
	if err != nil {
		err = fmt.Errorf("failed to get current ec2 instance cost: %s", err.Error())
		return nil, err
	}
	currLicensePrice, err := s.costSvc.EstimateLicensePrice(newCtx, instance)
	if err != nil {
		err = fmt.Errorf("failed to get current ec2 instance license price: %s", err.Error())
		return nil, err
	}

	s.logger.Info("Show preferences", zap.Any("preferences", preferences))
	for k, v := range preferences {
		s.logger.Info("Show preference", zap.String("key", k), zap.Any("value", v))
	}

	current := entity.RightsizingEC2Instance{
		Region:            currentInstanceType.RegionCode,
		InstanceType:      currentInstanceType.InstanceType,
		Processor:         currentInstanceType.PhysicalProcessor,
		Architecture:      currentInstanceType.PhysicalProcessorArch,
		VCPU:              int64(currentInstanceType.VCpu),
		Memory:            currentInstanceType.MemoryGB,
		NetworkThroughput: currentInstanceType.NetworkPerformance,
		ENASupported:      currentInstanceType.EnhancedNetworkingSupported,
		Cost:              currentCost,
		CostComponents:    currentComponentCost,
		LicensePrice:      currLicensePrice,
		License:           instance.UsageOperation,
	}
	if currentInstanceType.EbsBaselineThroughput != nil {
		current.EBSBandwidth = fmt.Sprintf("%.2f MB/s", *currentInstanceType.EbsBaselineThroughput)
	}
	if currentInstanceType.EbsBaselineIops != nil {
		current.EBSIops = fmt.Sprintf("%d io/s", *currentInstanceType.EbsBaselineIops)
	}

	//TODO Burst in CPU & Network
	//TODO Network: UpTo

	vCPU := instance.ThreadsPerCore * instance.CoreCount
	cpuBreathingRoom := int64(0)
	if preferences["CPUBreathingRoom"] != nil {
		cpuBreathingRoom, _ = strconv.ParseInt(*preferences["CPUBreathingRoom"], 10, 64)
	}
	memoryBreathingRoom := int64(0)
	if preferences["MemoryBreathingRoom"] != nil {
		memoryBreathingRoom, _ = strconv.ParseInt(*preferences["MemoryBreathingRoom"], 10, 64)
	}
	neededCPU := float64(vCPU) * (getValueOrZero(cpuUsage.Avg) + float64(cpuBreathingRoom)) / 100.0
	neededMemory := 0.0
	if memoryUsage.Max != nil {
		neededMemory = calculateHeadroom(currentInstanceType.MemoryGB*(*memoryUsage.Max), memoryBreathingRoom)
	}
	neededNetworkThroughput := getValueOrZero(networkUsage.Avg)
	if preferences["NetworkBreathingRoom"] != nil {
		room, _ := strconv.ParseInt(*preferences["NetworkBreathingRoom"], 10, 64)
		neededNetworkThroughput += neededNetworkThroughput * float64(room) / 100.0
	}

	pref := map[string]any{}
	for k, v := range preferences {
		var vl any
		if v == nil || *v == "" {
			vl = extractFromInstance(instance, currentInstanceType, region, k)
		} else {
			vl = *v
		}
		if ec2instance.PreferenceDBKey[k] == "" || vl == "" {
			continue
		}

		cond := "="
		if sc, ok := ec2instance.PreferenceSpecialCond[k]; ok {
			cond = sc
		}
		pref[fmt.Sprintf("%s %s ?", ec2instance.PreferenceDBKey[k], cond)] = vl
	}
	if _, ok := preferences["vCPU"]; !ok {
		pref["v_cpu >= ?"] = neededCPU
	}
	if _, ok := metrics["mem_used_percent"]; ok {
		if _, ok := preferences["MemoryGB"]; !ok {
			pref["memory_gb >= ?"] = neededMemory
		}
	}

	excludeBurstable := false
	if value, ok := preferences["ExcludeBurstableInstances"]; ok && value != nil {
		if *value == "Yes" {
			excludeBurstable = true
			pref["NOT(instance_type like ?)"] = "t%"
		} else if *value == "if current resource is burstable" {
			if !strings.HasPrefix(string(instance.InstanceType), "t") {
				excludeBurstable = true
				pref["NOT(instance_type like ?)"] = "t%"
			}
		}
	}
	if value, ok := preferences["UsageOperation"]; ok && value != nil {
		if v, ok := ec2instance.UsageOperationHumanToMachine[*value]; ok {
			pref["operation = ?"] = v
		} else {
			delete(pref, "operation = ?")
		}
	}
	// if operation is not provided, limit the results to one with no pre-installed software
	if _, ok := pref["operation = ?"]; !ok {
		pref["pre_installed_sw = ?"] = "NA"
	}
	if ebsIopsUsage.Avg != nil && *ebsIopsUsage.Avg > 0 {
		pref["ebs_maximum_iops IS NULL OR ebs_maximum_iops >= ?"] = *ebsIopsUsage.Avg
	}
	// Metric is in bytes so we convert to Mbytes
	if ebsThroughputUsage.Avg != nil && *ebsThroughputUsage.Avg > 0 {
		pref["ebs_maximum_throughput IS NULL OR ebs_maximum_throughput >= ?"] = *ebsThroughputUsage.Avg / (1024 * 1024)
	}

	var recommended *entity.RightsizingEC2Instance
	rightSizedInstanceType, err := s.ec2InstanceRepo.GetCheapestByCoreAndNetwork(ctx, neededNetworkThroughput, pref)
	if err != nil {
		err = fmt.Errorf("failed to find cheapest ec2 instance: %s", err.Error())
		return nil, err
	}
	if rightSizedInstanceType != nil {
		newInstance := instance
		newInstance.InstanceType = types.InstanceType(rightSizedInstanceType.InstanceType)
		newInstance.UsageOperation = rightSizedInstanceType.Operation
		if newInstance.Placement == nil {
			newInstance.Placement = &entity.EC2Placement{}
		}
		if rightSizedInstanceType.Tenancy == "Dedicated" {
			newInstance.Placement.Tenancy = types.TenancyDedicated
		} else if rightSizedInstanceType.Tenancy == "Host" {
			newInstance.Placement.Tenancy = types.TenancyHost
		} else {
			newInstance.Placement.Tenancy = types.TenancyDefault
		}
		recommendedCost, recommendedComponentCost, err := s.costSvc.GetEC2InstanceCost(newCtx, rightSizedInstanceType.RegionCode, newInstance, volumes, metrics)
		if err != nil {
			err = fmt.Errorf("failed to get recommended ec2 instance cost: %s", err.Error())
			return nil, err
		}
		recomLicensePrice, err := s.costSvc.EstimateLicensePrice(newCtx, newInstance)
		if err != nil {
			err = fmt.Errorf("failed to get recommended ec2 instance license price: %s", err.Error())
			return nil, err
		}
		recommended = &entity.RightsizingEC2Instance{
			Region:            rightSizedInstanceType.RegionCode,
			InstanceType:      rightSizedInstanceType.InstanceType,
			Processor:         rightSizedInstanceType.PhysicalProcessor,
			Architecture:      rightSizedInstanceType.PhysicalProcessorArch,
			VCPU:              int64(rightSizedInstanceType.VCpu),
			Memory:            rightSizedInstanceType.MemoryGB,
			NetworkThroughput: rightSizedInstanceType.NetworkPerformance,
			ENASupported:      rightSizedInstanceType.EnhancedNetworkingSupported,
			Cost:              recommendedCost,
			CostComponents:    recommendedComponentCost,
			LicensePrice:      recomLicensePrice,
			License:           newInstance.UsageOperation,
		}
		if rightSizedInstanceType.EbsBaselineThroughput != nil {
			recommended.EBSBandwidth = fmt.Sprintf("%.2f MB/s", *rightSizedInstanceType.EbsBaselineThroughput)
		}
		if rightSizedInstanceType.EbsBaselineIops != nil {
			recommended.EBSIops = fmt.Sprintf("%d io/s", *rightSizedInstanceType.EbsBaselineIops)
		}
	}

	recommendation := entity.RightSizingRecommendation{
		Current:           current,
		Recommended:       recommended,
		VCPU:              cpuUsage,
		EBSBandwidth:      ebsThroughputUsage,
		EBSIops:           ebsIopsUsage,
		NetworkThroughput: networkUsage,
		Description:       "",
	}
	if len(metrics["mem_used_percent"]) > 0 {
		recommendation.Memory = memoryUsage
	}

	if preferences["ExcludeUpsizingFeature"] != nil {
		if *preferences["ExcludeUpsizingFeature"] == "Yes" {
			if recommendation.Recommended != nil && recommendation.Recommended.Cost > recommendation.Current.Cost {
				recommendation.Recommended = &recommendation.Current
				recommendation.Description = "No recommendation available as upsizing feature is disabled"
				return &recommendation, nil
			}
		}
	}

	if rightSizedInstanceType != nil {
		recommendation.Description, _ = s.generateEc2InstanceDescription(ctx, instance, region, &currentInstanceType, rightSizedInstanceType, metrics, excludeBurstable, preferences, neededCPU, neededMemory, neededNetworkThroughput)
	}

	return &recommendation, nil
}
func bpsToMBps(bps *float64) float64 {
	if bps == nil {
		return 0
	}
	return *bps / (1024.0 * 1024.0)
}
func PFloat(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func (s *Service) generateEc2InstanceDescription(ctx context.Context, instance entity.EC2Instance, region string, currentInstanceType, rightSizedInstanceType *model.EC2InstanceType, metrics map[string][]types2.Datapoint, excludeBurstable bool, preferences map[string]*string, neededCPU, neededMemory, neededNetworkThroughput float64) (string, error) {
	minCPU, avgCPU, maxCPU := minOfDatapoints(metrics["CPUUtilization"]), averageOfDatapoints(metrics["CPUUtilization"]), maxOfDatapoints(metrics["CPUUtilization"])
	minMemory, avgMemory, maxMemory := minOfDatapoints(metrics["mem_used_percent"]), averageOfDatapoints(metrics["mem_used_percent"]), maxOfDatapoints(metrics["mem_used_percent"])
	networkDatapoints := sumMergeDatapoints(metrics["NetworkIn"], metrics["NetworkOut"])
	_, avgNetwork, _ := minOfDatapoints(networkDatapoints), averageOfDatapoints(networkDatapoints), maxOfDatapoints(networkDatapoints)

	usage := fmt.Sprintf("- %s has %.0f vCPUs. Usage over the course of last week is min=%.2f%%, avg=%.2f%%, max=%.2f%%, so you only need %.2f vCPUs. %s has %.0f vCPUs.\n", currentInstanceType.InstanceType, currentInstanceType.VCpu, PFloat(minCPU), PFloat(avgCPU), PFloat(maxCPU), neededCPU, rightSizedInstanceType.InstanceType, rightSizedInstanceType.VCpu)
	if len(metrics["mem_used_percent"]) > 0 {
		usage += fmt.Sprintf("- %s has %.1fGB Memory. Usage over the course of last week is min=%.2f%%, avg=%.2f%%, max=%.2f%%, so you only need %.2fGB Memory. %s has %.1fGB Memory.\n", currentInstanceType.InstanceType, currentInstanceType.MemoryGB, PFloat(minMemory), PFloat(avgMemory), PFloat(maxMemory), neededMemory, rightSizedInstanceType.InstanceType, rightSizedInstanceType.MemoryGB)
	} else {
		usage += fmt.Sprintf("- %s has %.1fGB Memory. Usage is not available. You need to install CloudWatch Agent on your instance to get this data. %s has %.1fGB Memory.\n", currentInstanceType.InstanceType, currentInstanceType.MemoryGB, rightSizedInstanceType.InstanceType, rightSizedInstanceType.MemoryGB)
	}
	usage += fmt.Sprintf("- %s's network performance is %s. Throughput over the course of last week is avg=%.2f MB/s, so you only need %.2f MB/s. %s has %s.\n", currentInstanceType.InstanceType, currentInstanceType.NetworkPerformance, bpsToMBps(avgNetwork), neededNetworkThroughput/(1024.0*1024.0), rightSizedInstanceType.InstanceType, rightSizedInstanceType.NetworkPerformance)

	needs := ""
	for k, v := range preferences {
		if ec2instance.PreferenceDBKey[k] == "" {
			continue
		}
		if v == nil {
			vl := extractFromInstance(instance, *currentInstanceType, region, k)
			needs += fmt.Sprintf("- You asked %s to be same as the current instance value which is %v\n", k, vl)
		} else {
			needs += fmt.Sprintf("- You asked %s to be %s\n", k, *v)
		}
	}

	prompt := fmt.Sprintf(`
I'm giving recommendation on ec2 instance right sizing. Based on user's usage and needs I have concluded that the best option for him is to use %s instead of %s. I need help summarizing the explanation into 280 characters (it's not a tweet! dont use hashtag!) while keeping these rules:
- mention the requirements from user side.
- for those fields which are changing make sure you mention the change.

Here's usage data:
%s

User's needs:
%s
`, rightSizedInstanceType.InstanceType, currentInstanceType.InstanceType, usage, needs)
	if excludeBurstable {
		prompt += "\nBurstable instances are excluded."
	}
	resp, err := s.openaiSvc.CreateChatCompletion(
		ctx,
		openai.ChatCompletionRequest{
			Model: openai.GPT4TurboPreview,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: prompt,
				},
			},
		},
	)

	if err != nil {
		return "", err
	}

	if len(resp.Choices) == 0 {
		return "", errors.New("empty choices")
	}
	return strings.TrimSpace(resp.Choices[0].Message.Content), nil
}

func extractFromInstance(instance entity.EC2Instance, i model.EC2InstanceType, region string, k string) any {
	switch k {
	case "InstanceFamily":
		return i.InstanceFamily
	case "Tenancy":
		switch instance.Tenancy {
		case types.TenancyDefault:
			return "Shared"
		case types.TenancyDedicated:
			return "Dedicated"
		case types.TenancyHost:
			return "Host"
		default:
			return ""
		}
	case "UsageOperation":
		return instance.UsageOperation
	case "EBSOptimized":
		if instance.EbsOptimized {
			return "Yes"
		} else {
			return "No"
		}
	case "LicenseModel":
		return i.LicenseModel
	case "Region":
		return region
	case "Hypervisor":
		return "" //TODO
	case "CurrentGeneration":
		return i.CurrentGeneration
	case "PhysicalProcessor":
		return i.PhysicalProcessor
	case "ClockSpeed":
		return i.ClockSpeed
	case "ProcessorArchitecture":
		return i.PhysicalProcessorArch
	case "SupportedArchitectures":
		return "" //TODO
	case "ENASupport":
		return i.EnhancedNetworkingSupported
	case "EncryptionInTransitSupported":
		return "" //TODO
	case "SupportedRootDeviceTypes":
		return "" //TODO
	case "Cores":
		return "" //TODO
	case "Threads":
		return "" //TODO
	case "vCPU":
		return i.VCpu
	case "MemoryGB":
		return i.MemoryGB
	}
	return ""
}

func (s *Service) EBSVolumeRecommendationGrpc(
	ctx context.Context,
	region string,
	volume *aws.EC2Volume,
	metrics *aws.VolumeMetrics,
	preferences map[string]*wrapperspb.StringValue,
	usageAverageType UsageAverageType,
) (*aws.EBSVolumeRecommendation, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(fmt.Sprintf("recovered from panic: %v", r))
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()
	newVolume := entity.EC2Volume{
		HashedVolumeId:   volume.HashedVolumeId,
		VolumeType:       types.VolumeType(volume.VolumeType),
		Size:             WrappedToInt32(volume.Size),
		Iops:             WrappedToInt32(volume.Iops),
		Throughput:       WrappedToFloat64(volume.Throughput),
		AvailabilityZone: WrappedToString(volume.AvailabilityZone),
	}
	newMetrics := convertMetrics(metrics.Metrics)
	newPreferences := make(map[string]*string)
	for k, v := range preferences {
		newPreferences[k] = WrappedToString(v)
	}

	s.logger.Info("EBSVolumeRecommendation parameters", zap.String("region", region), zap.Any("volume", newVolume),
		zap.Any("len metrics", len(newMetrics)), zap.Any("preferences", newPreferences), zap.Any("usageAverageType", usageAverageType))

	result, err := s.EBSVolumeRecommendation(ctx, region, newVolume, newMetrics, newPreferences, usageAverageType)
	if err != nil {
		return nil, err
	}
	newResult := &aws.EBSVolumeRecommendation{
		Current:     convertRightsizingEBSVolume(&result.Current),
		Recommended: convertRightsizingEBSVolume(result.Recommended),
		Iops:        convertUsage(&result.IOPS),
		Throughput:  convertUsage(&result.Throughput),
		Description: result.Description,
	}
	return newResult, nil
}

func convertRightsizingEBSVolume(rightSizing *entity.RightsizingEBSVolume) *aws.RightsizingEBSVolume {
	if rightSizing == nil {
		return nil
	}
	return &aws.RightsizingEBSVolume{
		Tier:                  string(rightSizing.Tier),
		VolumeSize:            Int32ToWrapper(rightSizing.VolumeSize),
		BaselineIops:          rightSizing.BaselineIOPS,
		ProvisionedIops:       Int32ToWrapper(rightSizing.ProvisionedIOPS),
		BaselineThroughput:    rightSizing.BaselineThroughput,
		ProvisionedThroughput: Float64ToWrapper(rightSizing.ProvisionedThroughput),
		Cost:                  rightSizing.Cost,
		CostComponents:        rightSizing.CostComponents,
	}
}

func (s *Service) EBSVolumeRecommendation(ctx context.Context, region string, volume entity.EC2Volume, metrics map[string][]types2.Datapoint, preferences map[string]*string, usageAverageType UsageAverageType) (*entity.EBSVolumeRecommendation, error) {
	iopsUsage := extractUsage(sumMergeDatapoints(metrics["VolumeReadOps"], metrics["VolumeWriteOps"]), usageAverageType)
	throughputUsageBytes := extractUsage(sumMergeDatapoints(metrics["VolumeReadBytes"], metrics["VolumeWriteBytes"]), usageAverageType)
	usageStorageThroughputMB := entity.Usage{
		Avg: funcP(throughputUsageBytes.Avg, throughputUsageBytes.Avg, func(a, _ float64) float64 { return a / (1024 * 1024) }),
		Min: funcP(throughputUsageBytes.Min, throughputUsageBytes.Min, func(a, _ float64) float64 { return a / (1024 * 1024) }),
		Max: funcP(throughputUsageBytes.Max, throughputUsageBytes.Max, func(a, _ float64) float64 { return a / (1024 * 1024) }),
	}
	sizeUsage := extractUsage(metrics["disk_used_percent"], usageAverageType)

	s.logger.Info("Show preferences", zap.Any("preferences", preferences))
	for k, v := range preferences {
		s.logger.Info("Show preference", zap.String("key", k), zap.Any("value", v))
	}

	size := float64(0)
	if size == 0 && volume.Size != nil {
		size = float64(*volume.Size)
	}

	iopsBreathingRoom := int64(0)
	if preferences["IOPSBreathingRoom"] != nil {
		iopsBreathingRoom, _ = strconv.ParseInt(*preferences["IOPSBreathingRoom"], 10, 32)
	}
	throughputBreathingRoom := int64(0)
	if preferences["ThroughputBreathingRoom"] != nil {
		throughputBreathingRoom, _ = strconv.ParseInt(*preferences["ThroughputBreathingRoom"], 10, 32)
	}
	sizeBreathingRoom := int64(0)
	if preferences["SizeBreathingRoom"] != nil {
		sizeBreathingRoom, _ = strconv.ParseInt(*preferences["SizeBreathingRoom"], 10, 32)
	}
	neededIops := pCalculateHeadroom(iopsUsage.Avg, iopsBreathingRoom)
	neededThroughput := pCalculateHeadroom(usageStorageThroughputMB.Avg, throughputBreathingRoom)
	neededSize := size
	if _, ok := metrics["disk_used_percent"]; ok && sizeUsage.Avg != nil {
		neededSize = max(1, neededSize*(*sizeUsage.Avg/100.0))
		neededSize = calculateHeadroom(neededSize, sizeBreathingRoom)
	}

	var validTypes []types.VolumeType
	if v, ok := preferences["IOPS"]; ok {
		if v == nil && volume.Iops != nil {
			neededIops = float64(*volume.Iops)
		} else {
			neededIops, _ = strconv.ParseFloat(*v, 64)
		}
	}
	if v, ok := preferences["Throughput"]; ok {
		if v == nil && volume.Throughput != nil {
			neededThroughput = *volume.Throughput
		} else {
			neededThroughput, _ = strconv.ParseFloat(*v, 64)
		}
	}
	if v, ok := preferences["Size"]; ok {
		if v == nil && volume.Size != nil {
			neededSize = float64(*volume.Size)
		} else if v != nil {
			neededSize, _ = strconv.ParseFloat(*v, 64)
		}
	}

	if v, ok := preferences["VolumeFamily"]; ok {
		if preferences["VolumeFamily"] == nil {
			validTypes = []types.VolumeType{volume.VolumeType}
		} else {
			switch strings.ToLower(*v) {
			case "general purpose", "ssd", "solid state drive", "gp":
				validTypes = []types.VolumeType{types.VolumeTypeGp2, types.VolumeTypeGp3}
			case "io", "io optimized":
				validTypes = []types.VolumeType{types.VolumeTypeIo1, types.VolumeTypeIo2}
			case "hdd", "sc", "cold", "hard disk drive", "st":
				validTypes = []types.VolumeType{types.VolumeTypeSc1, types.VolumeTypeSt1}
			}
		}
	}

	if v, ok := preferences["VolumeType"]; ok {
		if preferences["VolumeType"] == nil {
			validTypes = []types.VolumeType{volume.VolumeType}
		} else {
			validTypes = []types.VolumeType{types.VolumeType(*v)}
		}
	}

	if v, ok := preferences["ExcludeVolumeTypes"]; ok {
		if v != nil && len(*v) > 0 {
			if len(validTypes) == 0 {
				var t types.VolumeType
				validTypes = t.Values()
			}

			excludeList := strings.Split(*v, ",")

			var newValidTypes []types.VolumeType
			for _, o := range validTypes {
				ignore := false
				for _, e := range excludeList {
					if string(o) == e {
						ignore = true
					}
				}

				if ignore {
					continue
				}
				newValidTypes = append(newValidTypes, o)
			}
			validTypes = newValidTypes
		}
	}

	newCtx := context.Background()
	volumeCost, currentVolCostComponents, err := s.costSvc.GetEBSVolumeCost(newCtx, region, volume, metrics)
	if err != nil {
		err = fmt.Errorf("failed to get current ebs volume %s cost: %s", volume.HashedVolumeId, err.Error())
		return nil, err
	}

	var result = &entity.EBSVolumeRecommendation{
		Current: entity.RightsizingEBSVolume{
			Tier:                  volume.VolumeType,
			VolumeSize:            volume.Size,
			BaselineIOPS:          0, //TODO-Saleh
			ProvisionedIOPS:       volume.Iops,
			BaselineThroughput:    0, //TODO-Saleh
			ProvisionedThroughput: volume.Throughput,
			Cost:                  volumeCost,
			CostComponents:        currentVolCostComponents,
		},
		Recommended: nil,
		IOPS:        iopsUsage,
		Throughput:  throughputUsageBytes,
		Description: "",
	}
	if result.Current.ProvisionedIOPS != nil {
		result.Current.BaselineIOPS = *result.Current.ProvisionedIOPS
		result.Current.ProvisionedIOPS = nil
	}
	if result.Current.ProvisionedThroughput != nil {
		result.Current.BaselineThroughput = *result.Current.ProvisionedThroughput
		result.Current.ProvisionedThroughput = nil
	}
	if volume.VolumeType == types.VolumeTypeGp3 {
		provIops := max(int32(result.Current.BaselineIOPS)-model.Gp3BaseIops, 0)
		provThroughput := max(result.Current.BaselineThroughput-model.Gp3BaseThroughput, 0)
		result.Current.ProvisionedIOPS = &provIops
		result.Current.ProvisionedThroughput = &provThroughput
	}
	if volume.VolumeType == types.VolumeTypeIo1 || volume.VolumeType == types.VolumeTypeIo2 {
		provIops := result.Current.BaselineIOPS
		result.Current.ProvisionedIOPS = &provIops
		result.Current.BaselineIOPS = 0
	}

	newType, newSize, newBaselineIops, newBaselineThroughput, costBreakdown, err := s.ebsVolumeRepo.GetCheapestTypeWithSpecs(ctx, region, int32(neededSize), int32(neededIops), neededThroughput, validTypes)
	if err != nil {
		if strings.Contains(err.Error(), "no feasible volume types found") {
			return result, nil
		}
		err = fmt.Errorf("failed to find cheapest ebs volume: %s", err.Error())
		return nil, err
	}

	result.Recommended = &entity.RightsizingEBSVolume{
		Tier:                  "",
		VolumeSize:            utils.GetPointer(newSize),
		BaselineIOPS:          newBaselineIops,
		ProvisionedIOPS:       nil,
		BaselineThroughput:    newBaselineThroughput,
		ProvisionedThroughput: nil,
		Cost:                  0,
	}
	result.Description = fmt.Sprintf("- cost breakdown: %s", costBreakdown)
	newVolume := volume
	result.Recommended.Tier = newType
	newVolume.VolumeType = newType
	if newType != volume.VolumeType {
		result.Description += fmt.Sprintf("- change your volume from %s to %s\n", volume.VolumeType, newType)
	}

	if int32(neededSize) != getValueOrZero(volume.Size) {
		result.Recommended.VolumeSize = utils.GetPointer(int32(neededSize))
		newVolume.Size = utils.GetPointer(int32(neededSize))
		result.Description += fmt.Sprintf("- change volume size from %d to %d\n", getValueOrZero(volume.Size), int32(neededSize))
	}

	if newType == types.VolumeTypeIo1 || newType == types.VolumeTypeIo2 {
		avgIOps := int32(neededIops)
		result.Recommended.ProvisionedIOPS = &avgIOps
		newVolume.Iops = &avgIOps

		if volume.Iops == nil {
			result.Description += fmt.Sprintf("- add provisioned iops: %d\n", avgIOps)
		} else if avgIOps > *volume.Iops {
			result.Description += fmt.Sprintf("- increase provisioned iops from %d to %d\n", *volume.Iops, avgIOps)
		} else if avgIOps < *volume.Iops {
			result.Description += fmt.Sprintf("- decrease provisioned iops from %d to %d\n", *volume.Iops, avgIOps)
		} else {
			result.Recommended.ProvisionedIOPS = nil
			newVolume.Iops = volume.Iops
		}
	}

	if newType == types.VolumeTypeGp3 {
		provIops := max(int32(neededIops)-model.Gp3BaseIops, 0)
		result.Recommended.ProvisionedIOPS = &provIops
		newVolume.Iops = &provIops

		oldProvIops := int32(0)
		if volume.Iops != nil {
			oldProvIops = *volume.Iops
			if volume.VolumeType != types.VolumeTypeGp3 {
				oldProvIops -= model.Gp3BaseIops
				oldProvIops = max(oldProvIops, 0)
			}
		}

		if volume.Iops == nil {
			result.Description += fmt.Sprintf("- add provisioned iops: %d\n", provIops)
		} else if provIops > oldProvIops {
			result.Description += fmt.Sprintf("- increase provisioned iops from %d to %d\n", oldProvIops, provIops)
		} else if provIops < oldProvIops {
			result.Description += fmt.Sprintf("- decrease provisioned iops from %d to %d\n", oldProvIops, provIops)
		} else {
			result.Recommended.ProvisionedIOPS = nil
			newVolume.Iops = volume.Iops
		}
	}

	if newType == types.VolumeTypeGp3 {
		provThroughput := max(neededThroughput-model.Gp3BaseThroughput, 0)
		result.Recommended.ProvisionedThroughput = &provThroughput
		newVolume.Throughput = &provThroughput

		oldProvThroughput := float64(0)
		if volume.Throughput != nil {
			oldProvThroughput = *volume.Throughput
			if volume.VolumeType != types.VolumeTypeGp3 {
				oldProvThroughput -= model.Gp3BaseThroughput
				oldProvThroughput = max(oldProvThroughput, 0)
			}
		}

		if volume.Throughput == nil {
			result.Description += fmt.Sprintf("- add provisioned throughput: %.2f\n", provThroughput)
		} else if provThroughput > oldProvThroughput {
			result.Description += fmt.Sprintf("- increase provisioned throughput from %.2f to %.2f\n", oldProvThroughput, provThroughput)
		} else if provThroughput < oldProvThroughput {
			result.Description += fmt.Sprintf("- decrease provisioned throughput from %.2f to %.2f\n", oldProvThroughput, provThroughput)
		} else {
			result.Recommended.ProvisionedThroughput = nil
			newVolume.Throughput = volume.Throughput
		}
	}

	newVolumeCost, newVolCostComponents, err := s.costSvc.GetEBSVolumeCost(newCtx, region, newVolume, metrics)
	if err != nil {
		err = fmt.Errorf("failed to get recommended ebs volume %s cost: %s", newVolume.HashedVolumeId, err.Error())
		return nil, err
	}
	result.Recommended.Cost = newVolumeCost
	result.Recommended.CostComponents = newVolCostComponents

	if preferences["ExcludeUpsizingFeature"] != nil {
		if *preferences["ExcludeUpsizingFeature"] == "Yes" {
			if result.Recommended != nil && result.Recommended.Cost > result.Current.Cost {
				result.Recommended = &result.Current
				result.Description = "No recommendation available as upsizing feature is disabled"
			}
		}
	}

	return result, nil
}

func WrappedToInt32(v *wrapperspb.Int32Value) *int32 {
	if v == nil {
		return nil
	}
	tmp := v.GetValue()
	return &tmp
}

func WrappedToFloat64(v *wrapperspb.DoubleValue) *float64 {
	if v == nil {
		return nil
	}
	tmp := v.GetValue()
	return &tmp
}

func WrappedToString(v *wrapperspb.StringValue) *string {
	if v == nil {
		return nil
	}
	tmp := v.GetValue()
	if tmp == "" {
		return nil
	}
	return &tmp
}

func Int32ToWrapper(v *int32) *wrapperspb.Int32Value {
	if v == nil {
		return nil
	}
	return wrapperspb.Int32(*v)
}

func Float64ToWrapper(v *float64) *wrapperspb.DoubleValue {
	if v == nil {
		return nil
	}
	return wrapperspb.Double(*v)
}

func StringToWrapper(v *string) *wrapperspb.StringValue {
	if v == nil {
		return nil
	}
	return wrapperspb.String(*v)
}
