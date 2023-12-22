package describe

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kaytu-io/kaytu-aws-describer/aws"
	"github.com/kaytu-io/kaytu-azure-describer/azure"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/api"
	es2 "github.com/kaytu-io/kaytu-util/pkg/es"
	"github.com/kaytu-io/kaytu-util/pkg/pipeline"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/pkg/ticker"
	"strings"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"

	"github.com/kaytu-io/kaytu-engine/pkg/describe/es"
	"go.uber.org/zap"
)

func (s *Scheduler) UpdateDescribedResourceCountScheduler() error {
	s.logger.Info("DescribedResourceCount update scheduler started")

	t := ticker.NewTicker(1*time.Minute, time.Second*10)
	defer t.Stop()

	for ; ; <-t.C {
		s.UpdateDescribedResourceCount()
	}
}

func (s *Scheduler) UpdateDescribedResourceCount() {
	s.logger.Info("Updating DescribedResourceCount")
	AwsFailedCount, err := s.db.CountJobsWithStatus(8, source.CloudAWS, api.DescribeResourceJobFailed)
	if err != nil {
		s.logger.Error("Failed to count described resources",
			zap.String("connector", "AWS"),
			zap.String("status", "failed"),
			zap.Error(err))
		return
	}
	ResourcesDescribedCount.WithLabelValues("aws", "failure").Set(float64(*AwsFailedCount))
	AzureFailedCount, err := s.db.CountJobsWithStatus(8, source.CloudAzure, api.DescribeResourceJobFailed)
	if err != nil {
		s.logger.Error("Failed to count described resources",
			zap.String("connector", "Azure"),
			zap.String("status", "failed"),
			zap.Error(err))
		return
	}
	ResourcesDescribedCount.WithLabelValues("azure", "failure").Set(float64(*AzureFailedCount))
	AwsSucceededCount, err := s.db.CountJobsWithStatus(8, source.CloudAWS, api.DescribeResourceJobSucceeded)
	if err != nil {
		s.logger.Error("Failed to count described resources",
			zap.String("connector", "AWS"),
			zap.String("status", "successful"),
			zap.Error(err))
		return
	}
	ResourcesDescribedCount.WithLabelValues("aws", "successful").Set(float64(*AwsSucceededCount))
	AzureSucceededCount, err := s.db.CountJobsWithStatus(8, source.CloudAzure, api.DescribeResourceJobSucceeded)
	if err != nil {
		s.logger.Error("Failed to count described resources",
			zap.String("connector", "Azure"),
			zap.String("status", "successful"),
			zap.Error(err))
		return
	}
	ResourcesDescribedCount.WithLabelValues("azure", "successful").Set(float64(*AzureSucceededCount))
}

func (s *Scheduler) RunDescribeJobResultsConsumer() error {
	s.logger.Info("Consuming messages from the JobResults queue")

	ctx := context.Background()
	consumer, err := kafka.NewTopicConsumer(ctx, s.kafkaServers, "kaytu-describe-results-queue", "describe-receiver", false)
	if err != nil {
		return err
	}

	msgs := consumer.Consume(ctx, s.logger, 100)

	//msgs, err := s.describeJobResultQueue.Consume()
	//if err != nil {
	//	return err
	//}

	t := ticker.NewTicker(JobTimeoutCheckInterval, time.Second*10)
	defer t.Stop()

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("tasks channel is closed")
			}
			var result DescribeJobResult
			if err := json.Unmarshal(msg.Value, &result); err != nil {
				ResultsProcessedCount.WithLabelValues("", "failure").Inc()

				s.logger.Error("failed to consume message from describeJobResult", zap.Error(err))
				//err = msg.Nack(false, false)
				err := consumer.Commit(msg)
				if err != nil {
					s.logger.Error("failure while sending nack for message", zap.Error(err))
				}
				continue
			}

			s.logger.Info("Processing JobResult for Job",
				zap.Uint("jobId", result.JobID),
				zap.String("status", string(result.Status)),
			)

			var deletedCount int64
			if s.DoDeleteOldResources && result.Status == api.DescribeResourceJobSucceeded {
				if s.conf.ElasticSearch.IsOpenSearch {
					result.Status = api.DescribeResourceJobOldResourceDeletion
				}

				deletedCount, err = s.cleanupOldResources(result)
				if err != nil {
					ResultsProcessedCount.WithLabelValues(string(result.DescribeJob.SourceType), "failure").Inc()
					s.logger.Error("failed to cleanupOldResources", zap.Error(err))
					kmsg := kafka.Msg(string(msg.Key), msg.Value, "", "kaytu-describe-results-queue", confluent_kafka.PartitionAny)
					_, err := kafka.SyncSend(s.logger, s.kafkaProducer, []*confluent_kafka.Message{kmsg}, nil)
					if err != nil {
						s.logger.Error("failure while sending requeue", zap.Error(err))
						continue
					}

					err = consumer.Commit(msg)
					if err != nil {
						s.logger.Error("failure while committing requeue", zap.Error(err))
						continue
					}
					continue
				}
			}

			errStr := strings.ReplaceAll(result.Error, "\x00", "")
			errCodeStr := strings.ReplaceAll(result.ErrorCode, "\x00", "")
			if errCodeStr == "" {
				if strings.Contains(errStr, "exceeded maximum number of attempts") {
					errCodeStr = "TooManyRequestsException"
				} else if strings.Contains(errStr, "context deadline exceeded") {
					errCodeStr = "ContextDeadlineExceeded"
				}

			}
			s.logger.Info("updating job status", zap.Uint("jobID", result.JobID), zap.String("status", string(result.Status)))
			err = s.db.UpdateDescribeConnectionJobStatus(result.JobID, result.Status, errStr, errCodeStr, int64(len(result.DescribedResourceIDs)), deletedCount)
			if err != nil {
				ResultsProcessedCount.WithLabelValues(string(result.DescribeJob.SourceType), "failure").Inc()
				s.logger.Error("failed to UpdateDescribeResourceJobStatus", zap.Error(err))
				kmsg := kafka.Msg(string(msg.Key), msg.Value, "", "kaytu-describe-results-queue", confluent_kafka.PartitionAny)
				_, err := kafka.SyncSend(s.logger, s.kafkaProducer, []*confluent_kafka.Message{kmsg}, nil)
				if err != nil {
					s.logger.Error("failure while sending requeue", zap.Error(err))
					continue
				}

				err = consumer.Commit(msg)
				if err != nil {
					s.logger.Error("failure while committing requeue", zap.Error(err))
					continue
				}
				//err = msg.Nack(false, true)
				//if err != nil {
				//	s.logger.Error("failure while sending nack for message", zap.Error(err))
				//}
				continue
			}
			ResultsProcessedCount.WithLabelValues(string(result.DescribeJob.SourceType), "successful").Inc()
			if err := consumer.Commit(msg); err != nil {
				s.logger.Error("failure while sending ack for message", zap.Error(err))
			}
		case <-t.C:
			s.handleTimedoutDiscoveryJobs()
		}
	}
}
func (s *Scheduler) handleTimedoutDiscoveryJobs() {
	awsResources := aws.ListResourceTypes()
	for _, r := range awsResources {
		var interval time.Duration
		resourceType, err := aws.GetResourceType(r)
		if err != nil {
			s.logger.Error(fmt.Sprintf("failed to get resource type %s", r), zap.Error(err))
		}
		if resourceType.FastDiscovery {
			interval = s.describeIntervalHours
		} else if resourceType.CostDiscovery {
			interval = s.costDiscoveryIntervalHours
		} else {
			interval = s.fullDiscoveryIntervalHours
		}
		_, err = s.db.UpdateResourceTypeDescribeConnectionJobsTimedOut(r, interval)
		//s.logger.Warn(fmt.Sprintf("describe resource job timed out on %s:", r), zap.Error(err))
		//DescribeResourceJobsCount.WithLabelValues("failure", "timedout_aws").Inc()
		if err != nil {
			s.logger.Error(fmt.Sprintf("failed to update timed out DescribeResourceJobs on %s:", r), zap.Error(err))
		}
	}
	azureResources := azure.ListResourceTypes()
	for _, r := range azureResources {
		var interval time.Duration
		resourceType, err := azure.GetResourceType(r)
		if err != nil {
			s.logger.Error(fmt.Sprintf("failed to get resource type %s", r), zap.Error(err))
		}
		if resourceType.FastDiscovery {
			interval = s.describeIntervalHours
		} else if resourceType.CostDiscovery {
			interval = s.costDiscoveryIntervalHours
		} else {
			interval = s.fullDiscoveryIntervalHours
		}
		_, err = s.db.UpdateResourceTypeDescribeConnectionJobsTimedOut(r, interval)
		//s.logger.Warn(fmt.Sprintf("describe resource job timed out on %s:", r), zap.Error(err))
		//DescribeResourceJobsCount.WithLabelValues("failure", "timedout_azure").Inc()
		if err != nil {
			s.logger.Error(fmt.Sprintf("failed to update timed out DescribeResourceJobs on %s:", r), zap.Error(err))
		}
	}
}

func (s *Scheduler) cleanupOldResources(res DescribeJobResult) (int64, error) {
	var searchAfter []any

	isCostResourceType := false
	if strings.ToLower(res.DescribeJob.ResourceType) == "microsoft.costmanagement/costbyresourcetype" ||
		strings.ToLower(res.DescribeJob.ResourceType) == "aws::costexplorer::byservicedaily" {
		isCostResourceType = true
	}

	var additionalFilters []map[string]any
	if isCostResourceType {
		additionalFilters = append(additionalFilters, map[string]any{
			"range": map[string]any{"cost_date": map[string]any{"lt": time.Now().AddDate(0, -2, -1).UnixMilli()}},
		})
	}

	deletedCount := 0
	s.logger.Info("starting to delete old resources",
		zap.Uint("jobId", res.JobID),
		zap.String("connection_id", res.DescribeJob.SourceID),
		zap.String("resource_type", res.DescribeJob.ResourceType),
	)
	for {
		esResp, err := es.GetResourceIDsForAccountResourceTypeFromES(
			s.es,
			res.DescribeJob.SourceID,
			res.DescribeJob.ResourceType,
			additionalFilters,
			searchAfter,
			1000)
		if err != nil {
			CleanupJobCount.WithLabelValues("failure").Inc()
			s.logger.Error("CleanJob failed",
				zap.Error(err))
			return 0, err
		}

		if len(esResp.Hits.Hits) == 0 {
			break
		}
		var msgs []*confluent_kafka.Message
		task := es.DeleteTask{
			DiscoveryJobID: res.JobID,
			ConnectionID:   res.DescribeJob.SourceID,
			ResourceType:   res.DescribeJob.ResourceType,
			Connector:      res.DescribeJob.SourceType,
		}

		for _, hit := range esResp.Hits.Hits {
			searchAfter = hit.Sort
			esResourceID := hit.Source.ResourceID

			exists := false
			for _, describedResourceID := range res.DescribedResourceIDs {
				if esResourceID == describedResourceID {
					exists = true
					break
				}
			}

			if !exists || isCostResourceType {
				OldResourcesDeletedCount.WithLabelValues(string(res.DescribeJob.SourceType)).Inc()
				resource := es2.Resource{
					ID:           esResourceID,
					SourceID:     res.DescribeJob.SourceID,
					ResourceType: res.DescribeJob.ResourceType,
					SourceType:   res.DescribeJob.SourceType,
				}
				keys, idx := resource.KeysAndIndex()
				msg := kafka.Msg(kafka.HashOf(keys...), nil, idx, s.kafkaResourcesTopic, confluent_kafka.PartitionAny)
				msgs = append(msgs, msg)
				task.DeletingResources = append(task.DeletingResources, es.DeletingResource{
					Key:        msg.Key,
					ResourceID: esResourceID,
					Index:      idx,
				})

				lookupResource := es2.LookupResource{
					ResourceID:   esResourceID,
					SourceID:     res.DescribeJob.SourceID,
					ResourceType: res.DescribeJob.ResourceType,
					SourceType:   res.DescribeJob.SourceType,
				}
				lookUpKeys, lookUpIdx := lookupResource.KeysAndIndex()
				msg = kafka.Msg(kafka.HashOf(lookUpKeys...), nil, lookUpIdx, s.kafkaResourcesTopic, confluent_kafka.PartitionAny)
				msgs = append(msgs, msg)
				task.DeletingResources = append(task.DeletingResources, es.DeletingResource{
					Key:        msg.Key,
					ResourceID: esResourceID,
					Index:      idx,
				})

				if err != nil {
					CleanupJobCount.WithLabelValues("failure").Inc()
					s.logger.Error("CleanJob failed",
						zap.Error(err))
					return 0, err
				}
			}
		}

		i := 0
		for {
			if s.conf.ElasticSearch.IsOpenSearch {
				taskKeys, taskIdx := task.KeysAndIndex()
				task.EsID = kafka.HashOf(taskKeys...)
				task.EsIndex = taskIdx
				err = pipeline.SendToPipeline(s.conf.ElasticSearch.IngestionEndpoint, []kafka.Doc{task})
			} else {
				_, err = kafka.SyncSend(s.logger, s.kafkaProducer, msgs, nil)
			}

			if err != nil {
				s.logger.Error("failed to send delete message to kafka",
					zap.Uint("jobId", res.JobID),
					zap.String("connection_id", res.DescribeJob.SourceID),
					zap.String("resource_type", res.DescribeJob.ResourceType),
					zap.Error(err))
				if i > 10 {
					CleanupJobCount.WithLabelValues("failure").Inc()
					return 0, err
				}
				i++
				continue
			}
			break
		}

		deletedCount += len(msgs)
	}
	s.logger.Info("deleted old resources",
		zap.Uint("jobId", res.JobID),
		zap.String("connection_id", res.DescribeJob.SourceID),
		zap.String("resource_type", res.DescribeJob.ResourceType),
		zap.Int("deleted_count", deletedCount))

	CleanupJobCount.WithLabelValues("successful").Inc()
	return int64(deletedCount), nil
}

func (s *Scheduler) cleanupDescribeResourcesForConnections(connectionIds []string) {
	for _, connectionId := range connectionIds {
		var searchAfter []any
		for {
			esResp, err := es.GetResourceIDsForAccountFromES(s.es, connectionId, searchAfter, 1000)
			if err != nil {
				s.logger.Error("failed to get resource ids from es", zap.Error(err))
				break
			}

			if len(esResp.Hits.Hits) == 0 {
				break
			}
			var msgs []*confluent_kafka.Message
			for _, hit := range esResp.Hits.Hits {
				searchAfter = hit.Sort

				resource := es2.Resource{
					ID:           hit.Source.ResourceID,
					SourceID:     hit.Source.SourceID,
					ResourceType: strings.ToLower(hit.Source.ResourceType),
					SourceType:   hit.Source.SourceType,
				}
				keys, idx := resource.KeysAndIndex()
				key := kafka.HashOf(keys...)
				resource.EsID = key
				resource.EsIndex = idx
				msg := kafka.Msg(key, nil, idx, s.kafkaResourcesTopic, confluent_kafka.PartitionAny)
				msgs = append(msgs, msg)

				lookupResource := es2.LookupResource{
					ResourceID:   hit.Source.ResourceID,
					SourceID:     hit.Source.SourceID,
					ResourceType: strings.ToLower(hit.Source.ResourceType),
					SourceType:   hit.Source.SourceType,
				}
				keys, idx = lookupResource.KeysAndIndex()
				key = kafka.HashOf(keys...)
				lookupResource.EsID = key
				lookupResource.EsIndex = idx
				msg = kafka.Msg(key, nil, idx, s.kafkaResourcesTopic, confluent_kafka.PartitionAny)
				msgs = append(msgs, msg)
			}
			_, err = kafka.SyncSend(s.logger, s.kafkaProducer, msgs, nil)
			if err != nil {
				s.logger.Error("failed to send delete message to kafka", zap.Error(err))
				break
			}
			s.logger.Info("deleted old resources", zap.Int("deleted_count", len(msgs)), zap.String("connection_id", connectionId))
		}
	}

	return
}
