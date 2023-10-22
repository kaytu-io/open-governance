package analytics

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"reflect"
	"regexp"
	"strings"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/kaytu-io/kaytu-engine/pkg/analytics/db"
	"github.com/kaytu-io/kaytu-engine/pkg/analytics/es/resource"
	"github.com/kaytu-io/kaytu-engine/pkg/analytics/es/spend"
	authApi "github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	describeApi "github.com/kaytu-io/kaytu-engine/pkg/describe/api"
	describeClient "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpclient"
	inventoryClient "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
	onboardApi "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	onboardClient "github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"go.uber.org/zap"
)

type JobStatus string

const (
	JobCreated              JobStatus = "CREATED"
	JobInProgress           JobStatus = "IN_PROGRESS"
	JobCompletedWithFailure JobStatus = "COMPLETED_WITH_FAILURE"
	JobCompleted            JobStatus = "COMPLETED"
)

type Job struct {
	JobID                uint
	ResourceCollectionId *string
}

type JobResult struct {
	JobID  uint
	Status JobStatus
	Error  string
}

func (j *Job) Do(
	db db.Database,
	conf WorkerConfig,
	kfkProducer *confluent_kafka.Producer,
	kfkTopic string,
	onboardClient onboardClient.OnboardServiceClient,
	schedulerClient describeClient.SchedulerServiceClient,
	inventoryClient inventoryClient.InventoryServiceClient,
	logger *zap.Logger,
) JobResult {
	result := JobResult{
		JobID:  j.JobID,
		Status: JobCompleted,
		Error:  "",
	}
	fail := func(err error) JobResult {
		result.Error = err.Error()
		result.Status = JobCompletedWithFailure
		return result
	}

	var encodedResourceCollectionFilter *string
	if j.ResourceCollectionId != nil {
		rc, err := inventoryClient.GetResourceCollection(&httpclient.Context{UserRole: authApi.InternalRole},
			*j.ResourceCollectionId)
		if err != nil {
			return fail(err)
		}
		filtersJson, err := json.Marshal(rc.Filters)
		if err != nil {
			return fail(err)
		}
		filtersEncoded := base64.StdEncoding.EncodeToString(filtersJson)
		encodedResourceCollectionFilter = &filtersEncoded
	}

	err := steampipe.PopulateSteampipeConfig(conf.ElasticSearch, source.CloudAWS)
	if err != nil {
		logger.Error("failed to populate steampipe config for aws plugin", zap.Error(err))
		return fail(err)
	}
	err = steampipe.PopulateSteampipeConfig(conf.ElasticSearch, source.CloudAzure)
	if err != nil {
		logger.Error("failed to populate steampipe config for azure plugin", zap.Error(err))
		return fail(err)
	}
	err = steampipe.PopulateKaytuPluginSteampipeConfig(conf.ElasticSearch, conf.Steampipe)
	if err != nil {
		logger.Error("failed to populate steampipe config for kaytu plugin", zap.Error(err))
		return fail(err)
	}

	steampipeConn, err := steampipe.StartSteampipeServiceAndGetConnection(logger)
	if err != nil {
		return fail(err)
	}
	logger.Info("Connected to the steampipe database")
	defer steampipeConn.Conn().Close()
	defer steampipe.StopSteampipeService(logger)

	err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyAccountID, "all")
	if err != nil {
		logger.Error("failed to set steampipe context config for account id", zap.Error(err), zap.String("account_id", "all"))
		return fail(err)
	}
	if encodedResourceCollectionFilter != nil {
		err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyResourceCollectionFilters, *encodedResourceCollectionFilter)
		if err != nil {
			logger.Error("failed to set steampipe context config for resource collection filters", zap.Error(err), zap.String("resource_collection_filters", *encodedResourceCollectionFilter))
			return fail(err)
		}
	}

	if err := j.Run(db, steampipeConn, kfkProducer, kfkTopic, schedulerClient, onboardClient, logger); err != nil {
		fail(err)
	}
	return result
}

func (j *Job) Run(
	dbc db.Database,
	steampipeDB *steampipe.Database,
	kfkProducer *confluent_kafka.Producer,
	kfkTopic string,
	schedulerClient describeClient.SchedulerServiceClient,
	onboardClient onboardClient.OnboardServiceClient,
	logger *zap.Logger) error {
	startTime := time.Now()
	metrics, err := dbc.ListMetrics(true)
	if err != nil {
		return err
	}

	connectionCache := map[string]onboardApi.Connection{}

	for _, metric := range metrics {
		switch metric.Type {
		case db.MetricTypeAssets:
			s := map[string]describeApi.DescribeStatus{}
			for _, resourceType := range metric.Tables {
				status, err := schedulerClient.GetDescribeStatus(&httpclient.Context{UserRole: authApi.InternalRole}, resourceType)
				if err != nil {
					return err
				}

				for _, st := range status {
					if v, ok := s[st.ConnectionID]; ok {
						if st.Status != describeApi.DescribeResourceJobSucceeded {
							v.Status = st.Status
							s[st.ConnectionID] = v
						}
					} else {
						s[st.ConnectionID] = st
					}
				}
			}

			var status []describeApi.DescribeStatus
			for _, v := range s {
				status = append(status, v)
			}

			err = j.DoAssetMetric(
				steampipeDB,
				kfkProducer,
				kfkTopic,
				onboardClient,
				logger,
				metric,
				connectionCache,
				startTime,
				status,
			)
			if err != nil {
				return err
			}
		case db.MetricTypeSpend:
			// We do not support spend metrics for resource collections
			if j.ResourceCollectionId != nil {
				continue
			}
			awsStatus, err := schedulerClient.GetDescribeStatus(&httpclient.Context{UserRole: authApi.InternalRole}, "AWS::CostExplorer::ByServiceDaily")
			if err != nil {
				return err
			}

			azureStatus, err := schedulerClient.GetDescribeStatus(&httpclient.Context{UserRole: authApi.InternalRole}, "Microsoft.CostManagement/CostByResourceType")
			if err != nil {
				return err
			}

			s := map[string]describeApi.DescribeStatus{}
			for _, st := range append(awsStatus, azureStatus...) {
				if v, ok := s[st.ConnectionID]; ok {
					if st.Status != describeApi.DescribeResourceJobSucceeded {
						v.Status = st.Status
						s[st.ConnectionID] = v
					}
				} else {
					s[st.ConnectionID] = st
				}
			}

			var status []describeApi.DescribeStatus
			for _, v := range s {
				status = append(status, v)
			}

			err = j.DoSpendMetric(
				steampipeDB,
				kfkProducer,
				kfkTopic,
				onboardClient,
				logger,
				metric,
				connectionCache,
				status,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *Job) DoAssetMetric(
	steampipeDB *steampipe.Database,
	kfkProducer *confluent_kafka.Producer,
	kfkTopic string,
	onboardClient onboardClient.OnboardServiceClient,
	logger *zap.Logger,
	metric db.AnalyticMetric,
	connectionCache map[string]onboardApi.Connection,
	startTime time.Time,
	status []describeApi.DescribeStatus,
) error {
	connectionResultMap := map[string]resource.ConnectionMetricTrendSummary{}
	providerResultMap := map[string]resource.ConnectorMetricTrendSummary{}
	regionResultMap := map[string]resource.RegionMetricTrendSummary{}

	fmt.Println("assets ==== " + metric.Query)
	res, err := steampipeDB.QueryAll(context.TODO(), metric.Query)
	if err != nil {
		return err
	}

	connectorCount := map[string]int64{}
	connectorSuccessCount := map[string]int64{}
	for _, st := range status {
		connectorCount[st.Connector]++
		if st.Status == describeApi.DescribeResourceJobSucceeded {
			connectorSuccessCount[st.Connector]++
		}
	}

	for _, record := range res.Data {
		if len(record) != 3 {
			return fmt.Errorf("invalid query: %s", metric.Query)
		}

		sourceID, ok := record[0].(string)
		if !ok {
			return fmt.Errorf("invalid format for sourceID: [%s] %v", reflect.TypeOf(record[0]), record[0])
		}
		region, ok := record[1].(string)
		if !ok {
			return fmt.Errorf("invalid format for region: [%s] %v", reflect.TypeOf(record[1]), record[1])
		}
		count, ok := record[2].(int64)
		if !ok {
			return fmt.Errorf("invalid format for count: [%s] %v", reflect.TypeOf(record[2]), record[2])
		}

		var conn *onboardApi.Connection
		if cached, ok := connectionCache[sourceID]; ok {
			conn = &cached
		} else {
			conn, err = onboardClient.GetSource(&httpclient.Context{UserRole: authApi.AdminRole}, sourceID)
			if err != nil {
				if strings.Contains(err.Error(), "source not found") {
					continue
				}
				return fmt.Errorf("GetSource id=%s err=%v", sourceID, err)
			}
			if conn == nil {
				return fmt.Errorf("connection not found: %s", sourceID)
			}

			connectionCache[sourceID] = *conn
		}

		isJobSuccessful := true
		for _, st := range status {
			if st.ConnectionID == conn.ID.String() {
				if st.Status == describeApi.DescribeResourceJobFailed || st.Status == describeApi.DescribeResourceJobTimeout {
					isJobSuccessful = false
				}
			}
		}

		if v, ok := connectionResultMap[conn.ID.String()]; ok {
			v.ResourceCount += int(count)
			connectionResultMap[conn.ID.String()] = v
		} else {
			vn := resource.ConnectionMetricTrendSummary{
				ConnectionID:    conn.ID,
				ConnectionName:  conn.ConnectionName,
				Connector:       conn.Connector,
				EvaluatedAt:     startTime.UnixMilli(),
				Date:            startTime.Format("2006-01-02"),
				Month:           startTime.Format("2006-01"),
				Year:            startTime.Format("2006"),
				MetricID:        metric.ID,
				MetricName:      metric.Name,
				ResourceCount:   int(count),
				IsJobSuccessful: isJobSuccessful,

				ResourceCollection: j.ResourceCollectionId,
			}
			connectionResultMap[conn.ID.String()] = vn
		}

		if v, ok := providerResultMap[conn.Connector.String()]; ok {
			v.ResourceCount += int(count)
			providerResultMap[conn.Connector.String()] = v
		} else {
			vn := resource.ConnectorMetricTrendSummary{
				Connector:                  conn.Connector,
				EvaluatedAt:                startTime.UnixMilli(),
				Date:                       startTime.Format("2006-01-02"),
				Month:                      startTime.Format("2006-01"),
				Year:                       startTime.Format("2006"),
				MetricID:                   metric.ID,
				MetricName:                 metric.Name,
				ResourceCount:              int(count),
				TotalConnections:           connectorCount[string(conn.Connector)],
				TotalSuccessfulConnections: connectorSuccessCount[string(conn.Connector)],

				ResourceCollection: j.ResourceCollectionId,
			}
			providerResultMap[conn.Connector.String()] = vn
		}

		regionKey := region + "-" + conn.ID.String()
		if v, ok := regionResultMap[regionKey]; ok {
			v.ResourceCount += int(count)
			regionResultMap[regionKey] = v
		} else {
			vn := resource.RegionMetricTrendSummary{
				Region:         region,
				ConnectionID:   conn.ID,
				ConnectionName: conn.ConnectionName,
				Connector:      conn.Connector,
				EvaluatedAt:    startTime.UnixMilli(),
				Date:           startTime.Format("2006-01-02"),
				Month:          startTime.Format("2006-01"),
				Year:           startTime.Format("2006"),
				MetricID:       metric.ID,
				MetricName:     metric.Name,
				ResourceCount:  int(count),

				ResourceCollection: j.ResourceCollectionId,
			}
			regionResultMap[regionKey] = vn
		}
	}

	var msgs []kafka.Doc
	for _, item := range connectionResultMap {
		msgs = append(msgs, item)
	}
	for _, item := range providerResultMap {
		msgs = append(msgs, item)
	}
	for _, item := range regionResultMap {
		msgs = append(msgs, item)
	}
	if err := kafka.DoSend(kfkProducer, kfkTopic, -1, msgs, logger, nil); err != nil {
		return err
	}

	fmt.Printf("Write %d region docs, %d provider docs, %d connection docs\n", len(regionResultMap), len(providerResultMap), len(connectionResultMap))
	return nil
}

func (j *Job) DoSpendMetric(
	steampipeDB *steampipe.Database,
	kfkProducer *confluent_kafka.Producer,
	kfkTopic string,
	onboardClient onboardClient.OnboardServiceClient,
	logger *zap.Logger,
	metric db.AnalyticMetric,
	connectionCache map[string]onboardApi.Connection,
	status []describeApi.DescribeStatus,
) error {
	connectionResultMap := map[string]spend.ConnectionMetricTrendSummary{}
	providerResultMap := map[string]spend.ConnectorMetricTrendSummary{}

	query := metric.Query

	fmt.Println("spend ==== " + query)
	res, err := steampipeDB.QueryAll(context.TODO(), query)
	if err != nil {
		return err
	}

	connectorCount := map[string]int64{}
	connectorSuccessCount := map[string]int64{}
	for _, st := range status {
		connectorCount[st.Connector]++
		if st.Status == describeApi.DescribeResourceJobSucceeded {
			connectorSuccessCount[st.Connector]++
		}
	}

	for _, record := range res.Data {
		if len(record) != 3 {
			return fmt.Errorf("invalid query: %s", query)
		}

		connectionID, ok := record[0].(string)
		if !ok {
			return fmt.Errorf("invalid format for connectionID: [%s] %v", reflect.TypeOf(record[0]), record[0])
		}
		date, ok := record[1].(string)
		if !ok {
			return fmt.Errorf("invalid format for date: [%s] %v", reflect.TypeOf(record[1]), record[1])
		}
		sum, ok := record[2].(float64)
		if !ok {
			return fmt.Errorf("invalid format for sum: [%s] %v", reflect.TypeOf(record[2]), record[2])
		}
		if metric.ID == "spend_aws_marketplace" {
			fmt.Println("++++", record, sum)
		}

		var conn *onboardApi.Connection
		if cached, ok := connectionCache[connectionID]; ok {
			conn = &cached
		} else {
			conn, err = onboardClient.GetSource(&httpclient.Context{UserRole: authApi.AdminRole}, connectionID)
			if err != nil {
				if strings.Contains(err.Error(), "source not found") {
					continue
				}
				return fmt.Errorf("GetSource id=%s err=%v", connectionID, err)
			}
			if conn == nil {
				return fmt.Errorf("connection not found: %s", connectionID)
			}

			connectionCache[connectionID] = *conn
		}

		isJobSuccessful := true
		for _, st := range status {
			if st.ConnectionID == conn.ID.String() {
				if st.Status == describeApi.DescribeResourceJobFailed || st.Status == describeApi.DescribeResourceJobTimeout {
					isJobSuccessful = false
				}
			}
		}

		if r, err := regexp.Compile("^\\d+$"); err == nil && r.MatchString(date) {
			date = date[:4] + "-" + date[4:6] + "-" + date[6:]
		}

		dateTimestamp, err := time.Parse("2006-01-02", date)
		if err != nil {
			return fmt.Errorf("failed to parse date %s due to %v", date, err)
		}

		y, m, d := dateTimestamp.Date()
		startTime := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
		endTime := time.Date(y, m, d, 23, 59, 59, 0, time.UTC)

		connKey := conn.ID.String() + dateTimestamp.Format("2006-01-02")
		if v, ok := connectionResultMap[connKey]; ok {
			v.CostValue += sum
			connectionResultMap[connKey] = v
			if metric.ID == "spend_aws_marketplace" {
				fmt.Println("++++", sum, v.CostValue)
			}
		} else {
			vn := spend.ConnectionMetricTrendSummary{
				ConnectionID:    conn.ID,
				ConnectionName:  conn.ConnectionName,
				Connector:       conn.Connector,
				Date:            dateTimestamp.Format("2006-01-02"),
				DateEpoch:       dateTimestamp.UnixMilli(),
				Month:           dateTimestamp.Format("2006-01"),
				Year:            dateTimestamp.Format("2006"),
				MetricID:        metric.ID,
				MetricName:      metric.Name,
				CostValue:       sum,
				PeriodStart:     startTime.UnixMilli(),
				PeriodEnd:       endTime.UnixMilli(),
				IsJobSuccessful: isJobSuccessful,
				EvaluatedAt:     time.Now().UnixMilli(),
			}
			connectionResultMap[connKey] = vn
			if metric.ID == "spend_aws_marketplace" {
				fmt.Println("++++", sum, v.CostValue)
			}
		}

		providerKey := conn.Connector.String() + dateTimestamp.Format("2006-01-02")
		if v, ok := providerResultMap[providerKey]; ok {
			v.CostValue += sum
			providerResultMap[providerKey] = v
		} else {
			vn := spend.ConnectorMetricTrendSummary{
				Connector:                  conn.Connector,
				Date:                       dateTimestamp.Format("2006-01-02"),
				DateEpoch:                  dateTimestamp.UnixMilli(),
				Month:                      dateTimestamp.Format("2006-01"),
				Year:                       dateTimestamp.Format("2006"),
				MetricID:                   metric.ID,
				MetricName:                 metric.Name,
				CostValue:                  sum,
				PeriodStart:                startTime.UnixMilli(),
				PeriodEnd:                  endTime.UnixMilli(),
				TotalConnections:           connectorCount[string(conn.Connector)],
				TotalSuccessfulConnections: connectorSuccessCount[string(conn.Connector)],
				EvaluatedAt:                time.Now().UnixMilli(),
			}
			providerResultMap[providerKey] = vn
		}
	}

	var msgs []kafka.Doc
	for _, item := range connectionResultMap {
		if item.MetricID == "spend_aws_marketplace" {
			b, _ := json.Marshal(item)
			fmt.Println("++++", string(b))
		}
		msgs = append(msgs, item)
	}
	for _, item := range providerResultMap {
		msgs = append(msgs, item)
	}
	if err := kafka.DoSend(kfkProducer, kfkTopic, -1, msgs, logger, nil); err != nil {
		return err
	}

	fmt.Printf("Write %d provider docs, %d connection docs\n", len(providerResultMap), len(connectionResultMap))
	return nil
}
