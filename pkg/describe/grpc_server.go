package describe

import (
	"context"
	"encoding/json"
	"fmt"
	config2 "github.com/kaytu-io/kaytu-engine/pkg/describe/config"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/db"
	es2 "github.com/kaytu-io/kaytu-util/pkg/es"
	kaytuTrace "github.com/kaytu-io/kaytu-util/pkg/trace"
	"go.opentelemetry.io/otel"
	"net/http"
	"strings"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	envoyauth "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"github.com/go-redis/redis/v8"
	"github.com/gogo/googleapis/google/rpc"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/api"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/enums"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/es"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/proto/src/golang"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GRPCDescribeServer struct {
	db                        db.Database
	rdb                       *redis.Client
	producer                  *confluent_kafka.Producer
	conf                      config2.SchedulerConfig
	topic                     string
	logger                    *zap.Logger
	DoProcessReceivedMessages bool
	authGrpcClient            envoyauth.AuthorizationClient

	golang.DescribeServiceServer
}

func NewDescribeServer(db db.Database, rdb *redis.Client, producer *confluent_kafka.Producer, topic string, authGrpcClient envoyauth.AuthorizationClient, logger *zap.Logger, conf config2.SchedulerConfig) *GRPCDescribeServer {
	return &GRPCDescribeServer{
		db:                        db,
		rdb:                       rdb,
		producer:                  producer,
		topic:                     topic,
		logger:                    logger,
		DoProcessReceivedMessages: true,
		authGrpcClient:            authGrpcClient,
		conf:                      conf,
	}
}

func (s *GRPCDescribeServer) checkGRPCAuth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	mdHeaders := make(map[string]string)
	for k, v := range md {
		if len(v) > 0 {
			mdHeaders[k] = v[0]
		}
	}

	result, err := s.authGrpcClient.Check(ctx, &envoyauth.CheckRequest{
		Attributes: &envoyauth.AttributeContext{
			Request: &envoyauth.AttributeContext_Request{
				Http: &envoyauth.AttributeContext_HttpRequest{
					Headers: mdHeaders,
				},
			},
		},
	})

	if err != nil {
		return status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
	}

	if result.GetStatus() == nil || result.GetStatus().GetCode() != int32(rpc.OK) {
		return status.Errorf(codes.Unauthenticated, http.StatusText(http.StatusUnauthorized))
	}

	return nil
}

func (s *GRPCDescribeServer) grpcUnaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := s.checkGRPCAuth(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *GRPCDescribeServer) grpcStreamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.checkGRPCAuth(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *GRPCDescribeServer) SetInProgress(ctx context.Context, req *golang.SetInProgressRequest) (*golang.ResponseOK, error) {
	s.logger.Info("changing job to in progress", zap.Uint("jobId", uint(req.JobId)))
	err := s.db.UpdateDescribeConnectionJobToInProgress(uint(req.JobId)) //TODO this is called too much
	if err != nil {
		return nil, err
	}
	return &golang.ResponseOK{}, nil
}

func (s *GRPCDescribeServer) DeliverAWSResources(ctx context.Context, resources *golang.AWSResources) (*golang.ResponseOK, error) {
	startTime := time.Now().UnixMilli()
	defer func() {
		ResourceBatchProcessLatency.WithLabelValues("aws").Observe(float64(time.Now().UnixMilli() - startTime))
	}()

	var msgs []kafka.Doc
	for _, resource := range resources.GetResources() {
		var description any
		err := json.Unmarshal([]byte(resource.DescriptionJson), &description)
		if err != nil {
			//ResourcesDescribedCount.WithLabelValues("aws", "failure").Inc()
			s.logger.Error("failed to parse resource description json", zap.Error(err), zap.Uint32("jobID", resource.Job.JobId), zap.String("resourceID", resource.Id))
			return nil, err
		}

		var tags []es2.Tag
		for k, v := range resource.Tags {
			tags = append(tags, es2.Tag{
				// tags should be case-insensitive
				Key:   strings.ToLower(k),
				Value: strings.ToLower(v),
			})
		}

		kafkaResource := es2.Resource{
			ID:            resource.UniqueId,
			ARN:           resource.Arn,
			Name:          resource.Name,
			SourceType:    source.CloudAWS,
			ResourceType:  strings.ToLower(resource.Job.ResourceType),
			ResourceGroup: "",
			Location:      resource.Region,
			SourceID:      resource.Job.SourceId,
			ResourceJobID: uint(resource.Job.JobId),
			SourceJobID:   uint(resource.Job.ParentJobId),
			ScheduleJobID: uint(resource.Job.ScheduleJobId),
			CreatedAt:     resource.Job.DescribedAt,
			Description:   description,
			Metadata:      resource.Metadata,
			CanonicalTags: tags,
		}
		kmsg, _ := json.Marshal(kafkaResource)
		if len(kmsg) >= 32766 {
			// it's gonna hit error in kafka connect
			if !es.IsHandledAWSResourceType(resource.Job.ResourceType) {
				LargeDescribeResourceMessage.WithLabelValues(resource.Job.ResourceType).Inc()
			}
			s.logger.Warn("too large message",
				zap.String("resource_type", resource.Job.ResourceType),
				zap.String("json", string(kmsg)),
			)
		}
		//kmsg, _ := json.Marshal(kafkaResource)
		//keys, _ := kafkaResource.KeysAndIndex()
		//id := kafka.HashOf(keys...)
		//s.logger.Warn(fmt.Sprintf("sending resource id=%s : %s", id, string(kmsg)))

		lookupResource := es2.LookupResource{
			ResourceID:    resource.UniqueId,
			Name:          resource.Name,
			SourceType:    source.CloudAWS,
			ResourceType:  strings.ToLower(resource.Job.ResourceType),
			Location:      resource.Region,
			SourceID:      resource.Job.SourceId,
			ResourceJobID: uint(resource.Job.JobId),
			SourceJobID:   uint(resource.Job.ParentJobId),
			ScheduleJobID: uint(resource.Job.ScheduleJobId),
			CreatedAt:     resource.Job.DescribedAt,
			Tags:          tags,
		}
		kmsg, _ = json.Marshal(lookupResource)
		if len(kmsg) >= 32766 {
			// it's gonna hit error in kafka connect
			if !es.IsHandledAWSResourceType(resource.Job.ResourceType) {
				LargeDescribeResourceMessage.WithLabelValues(resource.Job.ResourceType).Inc()
			}
			s.logger.Warn("too large message",
				zap.String("resource_type", resource.Job.ResourceType),
				zap.String("json", string(kmsg)),
			)
		}
		//kmsg, _ = json.Marshal(lookupResource)
		//keys, _ = lookupResource.KeysAndIndex()
		//id = kafka.HashOf(keys...)
		//s.logger.Warn(fmt.Sprintf("sending lookup id=%s : %s", id, string(kmsg)))

		msgs = append(msgs, kafkaResource)
		msgs = append(msgs, lookupResource)
		//ResourcesDescribedCount.WithLabelValues("aws", "successful").Inc()
	}

	if !s.DoProcessReceivedMessages {
		return &golang.ResponseOK{}, nil
	}

	i := 0
	for {
		if s.conf.ElasticSearch.IsOpenSearch {
			s.logger.Error("workspace on opensearch and getting described resources on grpc???")
		}
		if err := kafka.DoSend(s.producer, resources.KafkaTopic, -1, msgs, s.logger, nil); err != nil {
			if i > 10 {
				StreamFailureCount.WithLabelValues("aws").Inc()
				s.logger.Warn("send to kafka",
					zap.String("connector:", "aws"),
					zap.String("error message", err.Error()))
				return nil, fmt.Errorf("send to kafka: %w", err)
			} else {
				i++
				continue
			}
		}
		break
	}
	return &golang.ResponseOK{}, nil
}

func (s *GRPCDescribeServer) DeliverAzureResources(ctx context.Context, resources *golang.AzureResources) (*golang.ResponseOK, error) {
	startTime := time.Now().UnixMilli()
	defer func() {
		ResourceBatchProcessLatency.WithLabelValues("azure").Observe(float64(time.Now().UnixMilli() - startTime))
	}()

	var msgs []kafka.Doc
	for _, resource := range resources.GetResources() {
		var description any
		err := json.Unmarshal([]byte(resource.DescriptionJson), &description)
		if err != nil {
			//ResourcesDescribedCount.WithLabelValues("azure", "failure").Inc()
			s.logger.Error("failed to parse resource description json", zap.Error(err), zap.Uint32("jobID", resource.Job.JobId), zap.String("resourceID", resource.Id))
			return nil, err
		}

		var tags []es2.Tag
		for k, v := range resource.Tags {
			tags = append(tags, es2.Tag{
				// tags should be case-insensitive
				Key:   strings.ToLower(k),
				Value: strings.ToLower(v),
			})
		}

		kafkaResource := es2.Resource{
			ID:            resource.UniqueId,
			ARN:           "",
			Description:   description,
			SourceType:    source.CloudAzure,
			ResourceType:  strings.ToLower(resource.Job.ResourceType),
			ResourceJobID: uint(resource.Job.JobId),
			SourceID:      resource.Job.SourceId,
			SourceJobID:   uint(resource.Job.ParentJobId),
			Metadata:      resource.Metadata,
			Name:          resource.Name,
			ResourceGroup: resource.ResourceGroup,
			Location:      resource.Location,
			ScheduleJobID: uint(resource.Job.ScheduleJobId),
			CreatedAt:     resource.Job.DescribedAt,
			CanonicalTags: tags,
		}
		kmsg, _ := json.Marshal(kafkaResource)
		if len(kmsg) >= 32766 {
			// it's gonna hit error in kafka connect
			if !es.IsHandledAzureResourceType(resource.Job.ResourceType) {
				LargeDescribeResourceMessage.WithLabelValues(resource.Job.ResourceType).Inc()
			}
			s.logger.Warn("too large message",
				zap.String("resource_type", resource.Job.ResourceType),
				zap.String("json", string(kmsg)),
			)
		}
		//keys, _ := kafkaResource.KeysAndIndex()
		//id := kafka.HashOf(keys...)
		//s.logger.Warn(fmt.Sprintf("sending resource id=%s : %s", id, string(kmsg)))

		lookupResource := es2.LookupResource{
			ResourceID:    resource.UniqueId,
			Name:          resource.Name,
			SourceType:    source.CloudAzure,
			ResourceType:  strings.ToLower(resource.Job.ResourceType),
			ResourceGroup: resource.ResourceGroup,
			Location:      resource.Location,
			SourceID:      resource.Job.SourceId,
			ResourceJobID: uint(resource.Job.JobId),
			SourceJobID:   uint(resource.Job.ParentJobId),
			ScheduleJobID: uint(resource.Job.ScheduleJobId),
			CreatedAt:     resource.Job.DescribedAt,
			Tags:          tags,
		}
		kmsg, _ = json.Marshal(lookupResource)
		if len(kmsg) >= 32766 {
			// it's gonna hit error in kafka connect
			if !es.IsHandledAzureResourceType(resource.Job.ResourceType) {
				LargeDescribeResourceMessage.WithLabelValues(resource.Job.ResourceType).Inc()
			}
			s.logger.Warn("too large message",
				zap.String("resource_type", resource.Job.ResourceType),
				zap.String("json", string(kmsg)),
			)
		}
		//keys, _ = lookupResource.KeysAndIndex()
		//id = kafka.HashOf(keys...)
		//s.logger.Warn(fmt.Sprintf("sending lookup id=%s : %s", id, string(kmsg)))

		msgs = append(msgs, kafkaResource)
		msgs = append(msgs, lookupResource)
		//ResourcesDescribedCount.WithLabelValues("azure", "successful").Inc()
	}

	i := 0
	for {
		if s.conf.ElasticSearch.IsOpenSearch {
			s.logger.Error("workspace on opensearch and getting described resources on grpc???")
		}
		if err := kafka.DoSend(s.producer, resources.KafkaTopic, -1, msgs, s.logger, nil); err != nil {
			if i > 10 {
				s.logger.Warn("send to kafka",
					zap.String("connector:", "azure"),
					zap.String("error message", err.Error()))
				StreamFailureCount.WithLabelValues("azure").Inc()
				return nil, fmt.Errorf("send to kafka: %w", err)
			} else {
				i++
				continue
			}
		}
		break
	}
	return &golang.ResponseOK{}, nil
}

func (s *GRPCDescribeServer) DeliverResult(ctx context.Context, req *golang.DeliverResultRequest) (*golang.ResponseOK, error) {
	ResultsDeliveredCount.WithLabelValues(req.DescribeJob.SourceType).Inc()
	result := DescribeJobResult{
		JobID:       uint(req.JobId),
		ParentJobID: uint(req.ParentJobId),
		Status:      api.DescribeResourceJobStatus(req.Status),
		Error:       req.Error,
		ErrorCode:   req.ErrorCode,
		DescribeJob: DescribeJob{
			JobID:         uint(req.DescribeJob.JobId),
			ScheduleJobID: uint(req.DescribeJob.ScheduleJobId),
			ParentJobID:   uint(req.DescribeJob.ParentJobId),
			ResourceType:  req.DescribeJob.ResourceType,
			SourceID:      req.DescribeJob.SourceId,
			AccountID:     req.DescribeJob.AccountId,
			DescribedAt:   req.DescribeJob.DescribedAt,
			SourceType:    source.Type(req.DescribeJob.SourceType),
			CipherText:    req.DescribeJob.ConfigReg,
			TriggerType:   enums.DescribeTriggerType(req.DescribeJob.TriggerType),
			RetryCounter:  uint(req.DescribeJob.RetryCounter),
		},
		DescribedResourceIDs: req.DescribedResourceIds,
	}

	s.logger.Info("Result delivered",
		zap.Uint("jobID", result.JobID),
		zap.String("status", string(result.Status)),
	)

	ctx, span := otel.Tracer(kaytuTrace.JaegerTracerName).Start(ctx, kaytuTrace.GetCurrentFuncName())
	defer span.End()

	var docs []kafka.Doc
	docs = append(docs, result)

	err := kafka.DoSend(s.producer, "kaytu-describe-results-queue", -1, docs, s.logger, nil)
	//err := s.describeJobResultQueue.Publish(result)
	if err != nil {
		s.logger.Error("Failed to publish into rabbitMQ",
			zap.Uint("jobID", result.JobID),
			zap.Error(err),
		)
		return nil, err
	}

	s.logger.Info("Publish finished",
		zap.Uint("jobID", result.JobID),
		zap.String("status", string(result.Status)),
	)
	return &golang.ResponseOK{}, nil
}
