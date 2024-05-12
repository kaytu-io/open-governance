package wastage

import (
	"encoding/json"
	"fmt"
	types2 "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/google/uuid"
	"github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	"github.com/kaytu-io/kaytu-engine/pkg/httpclient"
	"github.com/kaytu-io/kaytu-engine/pkg/httpserver"
	"github.com/kaytu-io/kaytu-engine/services/wastage/api/entity"
	"github.com/kaytu-io/kaytu-engine/services/wastage/cost"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/model"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/repo"
	"github.com/kaytu-io/kaytu-engine/services/wastage/ingestion"
	"github.com/kaytu-io/kaytu-engine/services/wastage/recommendation"
	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
	"net/http"
	"strconv"
	"time"
)

type API struct {
	tracer       trace.Tracer
	logger       *zap.Logger
	costSvc      *cost.Service
	usageRepo    repo.UsageV2Repo
	usageV1Repo  repo.UsageRepo
	recomSvc     *recommendation.Service
	ingestionSvc *ingestion.Service
}

func New(costSvc *cost.Service, recomSvc *recommendation.Service, ingestionService *ingestion.Service, usageV1Repo repo.UsageRepo, usageRepo repo.UsageV2Repo, logger *zap.Logger) API {
	return API{
		costSvc:      costSvc,
		recomSvc:     recomSvc,
		usageRepo:    usageRepo,
		usageV1Repo:  usageV1Repo,
		ingestionSvc: ingestionService,
		tracer:       otel.GetTracerProvider().Tracer("wastage.http.sources"),
		logger:       logger.Named("wastage-api"),
	}
}

func (s API) Register(e *echo.Echo) {
	g := e.Group("/api/v1/wastage")
	g.POST("/ec2-instance", s.EC2Instance)
	g.POST("/aws-rds", s.AwsRDS)
	i := e.Group("/api/v1/wastage-ingestion")
	i.PUT("/ingest/:service", httpserver.AuthorizeHandler(s.TriggerIngest, api.InternalRole))
	i.GET("/usages/:id", httpserver.AuthorizeHandler(s.GetUsage, api.InternalRole))
	i.PUT("/usages/migrate", s.MigrateUsages)
	i.PUT("/usages/migrate/v2", s.MigrateUsagesV2)
}

// EC2Instance godoc
//
//	@Summary		List wastage in EC2 Instances
//	@Description	List wastage in EC2 Instances
//	@Security		BearerToken
//	@Tags			wastage
//	@Produce		json
//	@Param			request	body		entity.EC2InstanceWastageRequest	true	"Request"
//	@Success		200		{object}	entity.EC2InstanceWastageResponse
//	@Router			/wastage/api/v1/wastage/ec2-instance [post]
func (s API) EC2Instance(c echo.Context) error {
	start := time.Now()
	ctx := otel.GetTextMapPropagator().Extract(c.Request().Context(), propagation.HeaderCarrier(c.Request().Header))
	ctx, span := s.tracer.Start(ctx, "get")
	defer span.End()

	var req entity.EC2InstanceWastageRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := c.Validate(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	var resp entity.EC2InstanceWastageResponse
	var err error

	stats := model.Statistics{
		AccountID:  req.Identification["account"],
		OrgEmail:   req.Identification["org_m_email"],
		ResourceID: req.Instance.HashedInstanceId,
	}
	statsOut, _ := json.Marshal(stats)

	reqJson, _ := json.Marshal(req)
	usage := model.UsageV2{
		ApiEndpoint:    "ec2-instance",
		Request:        reqJson,
		RequestId:      req.RequestId,
		CliVersion:     req.CliVersion,
		Response:       nil,
		FailureMessage: nil,
		Statistics:     statsOut,
	}
	err = s.usageRepo.Create(&usage)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			fmsg := err.Error()
			usage.FailureMessage = &fmsg
		} else {
			usage.Response, _ = json.Marshal(resp)
			id := uuid.New()
			responseId := id.String()
			usage.ResponseId = &responseId

			recom := entity.RightsizingEC2Instance{}
			if resp.RightSizing.Recommended != nil {
				recom = *resp.RightSizing.Recommended
			}

			instanceCost := resp.RightSizing.Current.Cost
			recomInstanceCost := recom.Cost

			volumeCurrentCost := 0.0
			volumeRecomCost := 0.0
			for _, v := range resp.VolumeRightSizing {
				volumeCurrentCost += v.Current.Cost
				if v.Recommended != nil {
					volumeRecomCost += v.Recommended.Cost
				}
			}

			stats.CurrentCost = instanceCost + volumeCurrentCost
			stats.RecommendedCost = recomInstanceCost + volumeRecomCost
			stats.Savings = (instanceCost + volumeCurrentCost) - (recomInstanceCost + volumeRecomCost)
			stats.EC2InstanceCurrentCost = instanceCost
			stats.EC2InstanceRecommendedCost = recomInstanceCost
			stats.EC2InstanceSavings = instanceCost - recomInstanceCost
			stats.EBSCurrentCost = volumeCurrentCost
			stats.EBSRecommendedCost = volumeRecomCost
			stats.EBSSavings = volumeCurrentCost - volumeRecomCost
			stats.EBSVolumeCount = len(resp.VolumeRightSizing)

			statsOut, _ := json.Marshal(stats)
			usage.Statistics = statsOut
		}
		err = s.usageRepo.Update(usage.ID, usage)
		if err != nil {
			s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
		}
	}()

	if req.Instance.State != types2.InstanceStateNameRunning {
		err = echo.NewHTTPError(http.StatusBadRequest, "instance is not running")
		return err
	}

	usageAverageType := recommendation.UsageAverageTypeMax
	if req.CliVersion == nil || semver.Compare("v"+*req.CliVersion, "v0.1.22") < 0 {
		usageAverageType = recommendation.UsageAverageTypeAverage
	}

	ec2RightSizingRecom, err := s.recomSvc.EC2InstanceRecommendation(req.Region, req.Instance, req.Volumes, req.Metrics, req.VolumeMetrics, req.Preferences, usageAverageType)
	if err != nil {
		err = fmt.Errorf("failed to get ec2 instance recommendation: %s", err.Error())
		return err
	}

	ebsRightSizingRecoms := make(map[string]entity.EBSVolumeRecommendation)
	for _, vol := range req.Volumes {
		var ebsRightSizingRecom *entity.EBSVolumeRecommendation
		ebsRightSizingRecom, err = s.recomSvc.EBSVolumeRecommendation(req.Region, vol, req.VolumeMetrics[vol.HashedVolumeId], req.Preferences, usageAverageType)
		if err != nil {
			err = fmt.Errorf("failed to get ebs volume %s recommendation: %s", vol.HashedVolumeId, err.Error())
			return err
		}
		ebsRightSizingRecoms[vol.HashedVolumeId] = *ebsRightSizingRecom
	}
	elapsed := time.Since(start).Seconds()
	usage.Latency = &elapsed
	err = s.usageRepo.Update(usage.ID, usage)
	if err != nil {
		s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
	}

	// DO NOT change this, resp is used in updating usage
	resp = entity.EC2InstanceWastageResponse{
		RightSizing:       *ec2RightSizingRecom,
		VolumeRightSizing: ebsRightSizingRecoms,
	}
	// DO NOT change this, resp is used in updating usage
	return c.JSON(http.StatusOK, resp)
}

// AwsRDS godoc
//
//	@Summary		List wastage in AWS RDS
//	@Description	List wastage in AWS RDS
//	@Security		BearerToken
//	@Tags			wastage
//	@Produce		json
//	@Param			request	body		entity.AwsRdsWastageRequest	true	"Request"
//	@Success		200		{object}	entity.AwsRdsWastageResponse
//	@Router			/wastage/api/v1/wastage/aws-rds [post]
func (s API) AwsRDS(c echo.Context) error {
	start := time.Now()
	ctx := otel.GetTextMapPropagator().Extract(c.Request().Context(), propagation.HeaderCarrier(c.Request().Header))
	ctx, span := s.tracer.Start(ctx, "get")
	defer span.End()

	var req entity.AwsRdsWastageRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := c.Validate(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	var resp entity.AwsRdsWastageResponse
	var err error

	stats := model.Statistics{
		AccountID:  req.Identification["account"],
		OrgEmail:   req.Identification["org_m_email"],
		ResourceID: req.Instance.HashedInstanceId,
	}
	statsOut, _ := json.Marshal(stats)

	reqJson, _ := json.Marshal(req)
	usage := model.UsageV2{
		ApiEndpoint:    "aws-rds",
		Request:        reqJson,
		RequestId:      req.RequestId,
		CliVersion:     req.CliVersion,
		Response:       nil,
		FailureMessage: nil,
		Statistics:     statsOut,
	}
	err = s.usageRepo.Create(&usage)
	if err != nil {
		s.logger.Error("failed to create usage", zap.Error(err))
		return err
	}

	defer func() {
		if err != nil {
			fmsg := err.Error()
			usage.FailureMessage = &fmsg
		} else {
			usage.Response, _ = json.Marshal(resp)
			id := uuid.New()
			responseId := id.String()
			usage.ResponseId = &responseId

			recom := entity.RightsizingAwsRds{}
			if resp.RightSizing.Recommended != nil {
				recom = *resp.RightSizing.Recommended
			}
			stats.CurrentCost = resp.RightSizing.Current.Cost
			stats.RecommendedCost = recom.Cost
			stats.Savings = resp.RightSizing.Current.Cost - recom.Cost
			stats.RDSInstanceCurrentCost = resp.RightSizing.Current.Cost
			stats.RDSInstanceRecommendedCost = recom.Cost
			stats.RDSInstanceSavings = resp.RightSizing.Current.Cost - recom.Cost

			statsOut, _ := json.Marshal(stats)
			usage.Statistics = statsOut
		}
		err = s.usageRepo.Update(usage.ID, usage)
		if err != nil {
			s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
		}
	}()

	usageAverageType := recommendation.UsageAverageTypeMax
	if req.CliVersion == nil || semver.Compare("v"+*req.CliVersion, "v0.1.22") < 0 {
		usageAverageType = recommendation.UsageAverageTypeAverage
	}

	ec2RightSizingRecom, err := s.recomSvc.AwsRdsRecommendation(req.Region, req.Instance, req.Metrics, req.Preferences, usageAverageType)
	if err != nil {
		s.logger.Error("failed to get aws rds recommendation", zap.Error(err))
		return err
	}

	elapsed := time.Since(start).Seconds()
	usage.Latency = &elapsed
	err = s.usageRepo.Update(usage.ID, usage)
	if err != nil {
		s.logger.Error("failed to update usage", zap.Error(err), zap.Any("usage", usage))
	}

	// DO NOT change this, resp is used in updating usage
	resp = entity.AwsRdsWastageResponse{
		RightSizing: *ec2RightSizingRecom,
	}
	// DO NOT change this, resp is used in updating usage
	return c.JSON(http.StatusOK, resp)
}

func (s API) TriggerIngest(c echo.Context) error {
	ctx := otel.GetTextMapPropagator().Extract(c.Request().Context(), propagation.HeaderCarrier(c.Request().Header))
	ctx, span := s.tracer.Start(ctx, "get")
	defer span.End()

	service := c.Param("service")

	switch service {
	case "aws-ec2-instance":
		err := s.ingestionSvc.DataAgeRepo.Delete("AWS::EC2::Instance")
		if err != nil {
			s.logger.Error("failed to delete data age", zap.Error(err), zap.String("service", service))
			return err
		}
		s.logger.Info("deleted data age for AWS::EC2::Instance ingestion will be triggered soon")
	case "aws-rds":
		err := s.ingestionSvc.DataAgeRepo.Delete("AWS::RDS::Instance")
		if err != nil {
			s.logger.Error("failed to delete data age", zap.Error(err), zap.String("service", service))
			return err
		}
		s.logger.Info("deleted data age for AWS::RDS::Instance ingestion will be triggered soon")
	}

	return c.NoContent(http.StatusOK)
}

func (s API) MigrateUsages(c echo.Context) error {
	go func() {
		s.logger.Info("Usage table migration started")

		for true {
			usage, err := s.usageV1Repo.GetRandomNotMoved()
			if err != nil {
				s.logger.Error("error while getting usage_v1 usages list", zap.Error(err))
				break
			}
			if usage == nil {
				break
			}
			if usage.Endpoint == "aws-rds" {
				var requestBody entity.AwsRdsWastageRequest
				err = json.Unmarshal(usage.Request, &requestBody)
				if err != nil {
					s.logger.Error("failed to unmarshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				requestId := fmt.Sprintf("usage_v1_%v", usage.ID)
				cliVersion := "unknown"
				requestBody.RequestId = &requestId
				requestBody.CliVersion = &cliVersion

				url := "https://api.kaytu.io/kaytu/wastage/api/v1/wastage/aws-rds"

				payload, err := json.Marshal(requestBody)
				if err != nil {
					s.logger.Error("failed to marshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}

				if _, err := httpclient.DoRequest(http.MethodPost, url, httpclient.FromEchoContext(c).ToHeaders(), payload, nil); err != nil {
					s.logger.Error("failed to rerun request", zap.Any("usage_id", usage.ID), zap.Error(err))
				}

				usage.Moved = true
				err = s.usageV1Repo.Update(usage.ID, *usage)
				if err != nil {
					s.logger.Error("failed to update usage moved flag", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
			} else {
				var requestBody entity.EC2InstanceWastageRequest
				err = json.Unmarshal(usage.Request, &requestBody)
				if err != nil {
					s.logger.Error("failed to unmarshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				requestId := fmt.Sprintf("usage_v1_%v", usage.ID)
				cliVersion := "unknown"
				requestBody.RequestId = &requestId
				requestBody.CliVersion = &cliVersion

				url := "https://api.kaytu.io/kaytu/wastage/api/v1/wastage/ec2-instance"

				payload, err := json.Marshal(requestBody)
				if err != nil {
					s.logger.Error("failed to marshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}

				if _, err := httpclient.DoRequest(http.MethodPost, url, httpclient.FromEchoContext(c).ToHeaders(), payload, nil); err != nil {
					s.logger.Error("failed to rerun request", zap.Any("usage_id", usage.ID), zap.Error(err))
				}

				usage.Moved = true
				err = s.usageV1Repo.Update(usage.ID, *usage)
				if err != nil {
					s.logger.Error("failed to update usage moved flag", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
			}
		}

	}()

	return c.NoContent(http.StatusOK)
}

func (s API) MigrateUsagesV2(c echo.Context) error {
	go func() {
		s.logger.Info("Usage table migration started")

		for {
			usage, err := s.usageRepo.GetRandomNullStatistics()
			if err != nil {
				s.logger.Error("error while getting null statistic usages list", zap.Error(err))
				break
			}
			if usage == nil {
				break
			}
			if usage.ApiEndpoint == "aws-rds" {
				var requestBody entity.AwsRdsWastageRequest
				var responseBody entity.AwsRdsWastageResponse
				err = json.Unmarshal(usage.Request, &requestBody)
				if err != nil {
					s.logger.Error("failed to unmarshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				stats := model.Statistics{
					AccountID:  requestBody.Identification["account"],
					OrgEmail:   requestBody.Identification["org_m_email"],
					ResourceID: requestBody.Instance.HashedInstanceId,
				}

				err = json.Unmarshal(usage.Response, &responseBody)
				if err == nil {
					recom := entity.RightsizingAwsRds{}
					if responseBody.RightSizing.Recommended != nil {
						recom = *responseBody.RightSizing.Recommended
					}
					stats.CurrentCost = responseBody.RightSizing.Current.Cost
					stats.RecommendedCost = recom.Cost
					stats.Savings = responseBody.RightSizing.Current.Cost - recom.Cost
					stats.RDSInstanceCurrentCost = responseBody.RightSizing.Current.Cost
					stats.RDSInstanceRecommendedCost = recom.Cost
					stats.RDSInstanceSavings = responseBody.RightSizing.Current.Cost - recom.Cost
				}

				out, err := json.Marshal(stats)
				if err != nil {
					s.logger.Error("failed to marshal stats", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				usage.Statistics = out

				err = s.usageRepo.Update(usage.ID, *usage)
				if err != nil {
					s.logger.Error("failed to update usage moved flag", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
			} else {
				var requestBody entity.EC2InstanceWastageRequest
				var responseBody entity.EC2InstanceWastageResponse
				err = json.Unmarshal(usage.Request, &requestBody)
				if err != nil {
					s.logger.Error("failed to unmarshal request body", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				stats := model.Statistics{
					AccountID:  requestBody.Identification["account"],
					OrgEmail:   requestBody.Identification["org_m_email"],
					ResourceID: requestBody.Instance.HashedInstanceId,
				}

				err = json.Unmarshal(usage.Response, &responseBody)
				if err == nil {
					recom := entity.RightsizingEC2Instance{}
					if responseBody.RightSizing.Recommended != nil {
						recom = *responseBody.RightSizing.Recommended
					}

					instanceCost := responseBody.RightSizing.Current.Cost
					recomInstanceCost := recom.Cost

					volumeCurrentCost := 0.0
					volumeRecomCost := 0.0
					for _, v := range responseBody.VolumeRightSizing {
						volumeCurrentCost += v.Current.Cost
						if v.Recommended != nil {
							volumeRecomCost += v.Recommended.Cost
						}
					}

					stats.CurrentCost = instanceCost + volumeCurrentCost
					stats.RecommendedCost = recomInstanceCost + volumeRecomCost
					stats.Savings = (instanceCost + volumeCurrentCost) - (recomInstanceCost + volumeRecomCost)
					stats.EC2InstanceCurrentCost = instanceCost
					stats.EC2InstanceRecommendedCost = recomInstanceCost
					stats.EC2InstanceSavings = instanceCost - recomInstanceCost
					stats.EBSCurrentCost = volumeCurrentCost
					stats.EBSRecommendedCost = volumeRecomCost
					stats.EBSSavings = volumeCurrentCost - volumeRecomCost
					stats.EBSVolumeCount = len(responseBody.VolumeRightSizing)
				}

				out, err := json.Marshal(stats)
				if err != nil {
					s.logger.Error("failed to marshal stats", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
				usage.Statistics = out

				err = s.usageRepo.Update(usage.ID, *usage)
				if err != nil {
					s.logger.Error("failed to update usage moved flag", zap.Any("usage_id", usage.ID), zap.Error(err))
					continue
				}
			}
		}

	}()

	return c.NoContent(http.StatusOK)
}

func (s API) GetUsage(c echo.Context) error {
	idStr := c.QueryParam("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return err
	}

	usage, err := s.usageRepo.Get(uint(id))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, usage)
}
