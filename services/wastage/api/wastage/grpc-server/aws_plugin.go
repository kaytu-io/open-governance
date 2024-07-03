package grpc_server

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/alitto/pond"
	"github.com/kaytu-io/kaytu-engine/services/wastage/config"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/repo"
	"github.com/kaytu-io/kaytu-engine/services/wastage/recommendation"
	aws "github.com/kaytu-io/plugin-aws/plugin/proto/src/golang"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type awsPluginServer struct {
	aws.OptimizationServer

	cfg config.WastageConfig

	tracer trace.Tracer
	logger *zap.Logger

	blobClient     *azblob.Client
	blobWorkerPool *pond.WorkerPool

	usageRepo repo.UsageV2Repo
	recomSvc  *recommendation.Service
}

func newAwsPluginServer(logger *zap.Logger, cfg config.WastageConfig, blobClient *azblob.Client, blobWorkerPool *pond.WorkerPool, usageRepo repo.UsageV2Repo, recomSvc *recommendation.Service) *awsPluginServer {
	return &awsPluginServer{
		cfg:            cfg,
		tracer:         otel.GetTracerProvider().Tracer("wastage.http.sources"),
		logger:         logger.Named("grpc"),
		blobClient:     blobClient,
		blobWorkerPool: blobWorkerPool,
		usageRepo:      usageRepo,
		recomSvc:       recomSvc,
	}
}
//
//func (s *gcpPluginServer) EC2InstanceOptimization(ctx context.Context, req *aws.EC2InstanceOptimizationRequest) (*aws.EC2InstanceOptimizationResponse, error) {
//	start := time.Now()
//	ctx, span := s.tracer.Start(ctx, "get")
//	defer span.End()
//
//	var resp aws.EC2InstanceOptimizationResponse
//	var err error
//
//	md, ok := metadata.FromIncomingContext(ctx)
//	if !ok {
//		return nil, fmt.Errorf("failed to get incoming context")
//	}
//
//	userIds := md.Get(httpserver.XKaytuUserIDHeader)
//	userId := ""
//	if len(userIds) == 0 {
//		return nil, fmt.Errorf("user not found")
//	}
//	userId = userIds[0]
//
//	stats := model.Statistics{
//		AccountID:   req.Identification["account"],
//		OrgEmail:    req.Identification["org_m_email"],
//		ResourceID:  req.Instance.HashedInstanceId,
//		Auth0UserId: userId,
//	}
//	statsOut, _ := json.Marshal(stats)
//
//	fullReqJson, _ := json.Marshal(req)
//	metrics := req.Metrics
//	volMetrics := req.VolumeMetrics
//	req.Metrics = nil
//	req.VolumeMetrics = nil
//	trimmedReqJson, _ := json.Marshal(req)
//	req.Metrics = metrics
//	req.VolumeMetrics = volMetrics
//
//	var requestId *string
//	var cliVersion *string
//	if req.RequestId != nil {
//		requestId = &req.RequestId.Value
//	}
//	if req.CliVersion != nil {
//		cliVersion = &req.CliVersion.Value
//	}
//
//	if requestId == nil {
//		id := uuid.New().String()
//		requestId = &id
//	}
//
//	s.blobWorkerPool.Submit(func() {
//		_, err = s.blobClient.UploadBuffer(context.Background(), s.cfg.AzBlob.Container, fmt.Sprintf("ec2-instance/%s.json", *req.RequestId), fullReqJson, &azblob.UploadBufferOptions{AccessTier: utils.GetPointer(blob.AccessTierCold)})
//		if err != nil {
//			s.logger.Error("failed to upload usage to blob storage", zap.Error(err))
//		}
//	})
//
//	usage := model.UsageV2{
//		ApiEndpoint:    "ec2-instance",
//		Request:        trimmedReqJson,
//		RequestId:      requestId,
//		CliVersion:     cliVersion,
//		Response:       nil,
//		FailureMessage: nil,
//		Statistics:     statsOut,
//	}
//	err = s.usageRepo.Create(&usage)
//	if err != nil {
//		return nil, err
//	}
//
//	defer func() {
//		if err != nil {
//			fmsg := err.Error()
//			usage.FailureMessage = &fmsg
//		} else {
//			usage.Response, _ = json.Marshal(resp)
//			id := uuid.New()
//			responseId := id.String()
//			usage.ResponseId = &responseId
//
//			recom := aws.RightsizingEC2Instance{}
//			if resp.RightSizing.Recommended != nil {
//				recom = *resp.RightSizing.Recommended
//			}
//
//			instanceCost := resp.RightSizing.Current.Cost
//			recomInstanceCost := recom.Cost
//
//			volumeCurrentCost := 0.0
//			volumeRecomCost := 0.0
//			for _, v := range resp.VolumeRightSizing {
//				volumeCurrentCost += v.Current.Cost
//				if v.Recommended != nil {
//					volumeRecomCost += v.Recommended.Cost
//				}
//			}
//
//			stats.CurrentCost = instanceCost + volumeCurrentCost
//			stats.RecommendedCost = recomInstanceCost + volumeRecomCost
//			stats.Savings = (instanceCost + volumeCurrentCost) - (recomInstanceCost + volumeRecomCost)
//			stats.EC2InstanceCurrentCost = instanceCost
//			stats.EC2InstanceRecommendedCost = recomInstanceCost
//			stats.EC2InstanceSavings = instanceCost - recomInstanceCost
//			stats.EBSCurrentCost = volumeCurrentCost
//			stats.EBSRecommendedCost = volumeRecomCost
//			stats.EBSSavings = volumeCurrentCost - volumeRecomCost
//			stats.EBSVolumeCount = len(resp.VolumeRightSizing)
//
//			statsOut, _ := json.Marshal(stats)
//			usage.Statistics = statsOut
//		}
//		err = s.usageRepo.Update(usage.ID, usage)
//		if err != nil {
//			s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
//		}
//	}()
//
//	if req.Instance.State != string(types2.InstanceStateNameRunning) {
//		err = echo.NewHTTPError(http.StatusBadRequest, "instance is not running")
//		return nil, err
//	}
//
//	if req.Loading {
//		return echoCtx.JSON(http.StatusOK, entity.EC2InstanceWastageResponse{})
//	}
//
//	usageAverageType := recommendation.UsageAverageTypeMax
//	if req.CliVersion == nil || semver.Compare("v"+*req.CliVersion, "v0.5.2") < 0 {
//		return echo.NewHTTPError(http.StatusBadRequest, "plugin version is no longer supported - please update to the latest version")
//	}
//
//	ok, err = s.checkAccountsLimit(httpserver.GetUserID(echoCtx), req.Identification["org_m_email"], req.Identification["account"])
//	if err != nil {
//		s.logger.Error("failed to check profile limit", zap.Error(err))
//		return nil, err
//	}
//	if !ok {
//		err = s.checkPremiumAndSendErr(echoCtx, req.Identification["org_m_email"], "profile")
//		if err != nil {
//			return err
//		}
//	}
//
//	ok, err = s.checkEC2InstanceLimit(httpserver.GetUserID(echoCtx), req.Identification["org_m_email"])
//	if err != nil {
//		s.logger.Error("failed to check aws ec2 instance limit", zap.Error(err))
//		return nil, err
//	}
//	if !ok {
//		err = s.checkPremiumAndSendErr(echoCtx, req.Identification["org_m_email"], "ec2 instance")
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	ec2RightSizingRecom, err := s.recomSvc.EC2InstanceRecommendation(ctx, req.Region, req.Instance, req.Volumes, req.Metrics, req.VolumeMetrics, req.Preferences, usageAverageType)
//	if err != nil {
//		err = fmt.Errorf("failed to get ec2 instance recommendation: %s", err.Error())
//		return nil, err
//	}
//
//	ebsRightSizingRecoms := make(map[string]entity.EBSVolumeRecommendation)
//	for _, vol := range req.Volumes {
//		//ok, err := checkEBSVolumeLimit(s.usageRepo, httpserver.GetUserID(c), req.Identification["org_m_email"])
//		//if err != nil {
//		//	s.logger.Error("failed to check aws ebs volume limit", zap.Error(err))
//		//	return err
//		//}
//		//if !ok {
//		//	err = s.checkPremiumAndSendErr(c, req.Identification["org_m_email"], "ebs volume")
//		//	if err != nil {
//		//		return err
//		//	}
//		//}
//		var ebsRightSizingRecom *entity.EBSVolumeRecommendation
//		ebsRightSizingRecom, err = s.recomSvc.EBSVolumeRecommendation(ctx, req.Region, vol, req.VolumeMetrics[vol.HashedVolumeId], req.Preferences, usageAverageType)
//		if err != nil {
//			err = fmt.Errorf("failed to get ebs volume %s recommendation: %s", vol.HashedVolumeId, err.Error())
//			return nil, err
//		}
//		ebsRightSizingRecoms[vol.HashedVolumeId] = *ebsRightSizingRecom
//	}
//	elapsed := time.Since(start).Seconds()
//	usage.Latency = &elapsed
//	err = s.usageRepo.Update(usage.ID, usage)
//	if err != nil {
//		s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
//	}
//
//	// DO NOT change this, resp is used in updating usage
//	resp = aws.EC2InstanceOptimizationResponse{
//		RightSizing:       *ec2RightSizingRecom,
//		VolumeRightSizing: ebsRightSizingRecoms,
//	}
//	// DO NOT change this, resp is used in updating usage
//	return &resp, nil
//}
