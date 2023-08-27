package reporter

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	"github.com/kaytu-io/kaytu-engine/pkg/config"
	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpclient"
	api2 "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	onboardClient "github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"go.uber.org/zap"
)

//go:embed queries-aws.json
var awsQueriesStr string
var awsQueries []Query

//go:embed queries-azure.json
var azureQueriesStr string
var azureQueries []Query

type Query struct {
	ListQuery string   `json:"list"`
	GetQuery  string   `json:"get"`
	KeyFields []string `json:"keyFields"`
	TableName string   `json:"tableName"`
}

type TriggerQueryRequest struct {
	Query  Query  `json:"query"`
	Source string `json:"source"`
}

type QueryMismatch struct {
	KeyColumn      string `json:"keyColumn"`
	ConflictColumn string `json:"conflictColumn"`
	Steampipe      string `json:"steampipe"`
	Elasticsearch  string `json:"elasticsearch"`
}

type TriggerQueryResponse struct {
	TotalRows          int             `json:"totalRows"`
	NotMatchingColumns []string        `json:"notMatchingColumns"`
	Mismatches         []QueryMismatch `json:"messages"`
}

type JobConfig struct {
	Steampipe       config.Postgres
	SteampipeES     config.Postgres
	Onboard         config.KaytuService
	ScheduleMinutes int
}

type Job struct {
	steampipe       *steampipe.Database
	esSteampipe     *steampipe.Database
	onboardClient   onboardClient.OnboardServiceClient
	logger          *zap.Logger
	ScheduleMinutes int
}

var ReporterJobsCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "kaytu",
	Subsystem: "reporter",
	Name:      "job_total",
	Help:      "Count of reporter jobs",
}, []string{"table_name", "status"})

func New(config JobConfig) (*Job, error) {
	if content, err := os.ReadFile("/queries-aws.json"); err == nil {
		awsQueriesStr = string(content)
	}
	if content, err := os.ReadFile("/queries-azure.json"); err == nil {
		azureQueriesStr = string(content)
	}

	if err := json.Unmarshal([]byte(awsQueriesStr), &awsQueries); err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(azureQueriesStr), &azureQueries); err != nil {
		return nil, err
	}

	installCmd := exec.Command("steampipe", "plugin", "install", "steampipe")
	err := installCmd.Run()
	if err != nil {
		return nil, err
	}

	installCmd = exec.Command("steampipe", "plugin", "install", "aws")
	err = installCmd.Run()
	if err != nil {
		return nil, err
	}

	installCmd = exec.Command("steampipe", "plugin", "install", "azure")
	err = installCmd.Run()
	if err != nil {
		return nil, err
	}

	installCmd = exec.Command("steampipe", "plugin", "install", "azuread")
	err = installCmd.Run()
	if err != nil {
		return nil, err
	}

	s2, err := steampipe.NewSteampipeDatabase(steampipe.Option{
		Host: config.SteampipeES.Host,
		Port: config.SteampipeES.Port,
		User: config.SteampipeES.Username,
		Pass: config.SteampipeES.Password,
		Db:   config.SteampipeES.DB,
	})
	if err != nil {
		return nil, err
	}

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	onboard := onboardClient.NewOnboardServiceClient(config.Onboard.BaseURL, nil)

	if config.ScheduleMinutes <= 0 {
		config.ScheduleMinutes = 5
	}

	return &Job{
		steampipe:       nil,
		esSteampipe:     s2,
		onboardClient:   onboard,
		logger:          logger,
		ScheduleMinutes: config.ScheduleMinutes,
	}, nil
}

func (j *Job) Run() {
	fmt.Println("starting scheduling")
	for _, q := range awsQueries {
		j.logger.Info("loaded aws query ", zap.String("listQuery", q.ListQuery))
	}
	for _, q := range azureQueries {
		j.logger.Info("loaded azure query ", zap.String("listQuery", q.ListQuery))
	}

	for {
		//fmt.Println("starting job")
		if err := j.RunRandomJob(); err != nil {
			j.logger.Error("failed to run job", zap.Error(err))
			time.Sleep(time.Minute)
		} else {
			time.Sleep(time.Duration(j.ScheduleMinutes) * time.Minute)
		}
	}
}

func (j *Job) RunRandomJob() error {
	account, err := j.RandomAccount()
	if err != nil {
		return err
	}

	query := j.RandomQuery(account.Connector)
	err, _ = j.RunJob(account, query)
	return err
}

func (j *Job) RunJob(account *api2.Connection, query *Query) (error, *TriggerQueryResponse) {
	defer func() {
		if r := recover(); r != nil {
			j.logger.Error("panic", zap.Error(fmt.Errorf("%v", r)))
		}
	}()

	awsCred, azureCred, err := j.onboardClient.GetSourceFullCred(&httpclient.Context{
		UserRole: api.KaytuAdminRole,
	}, account.ID.String())
	if err != nil {
		return err, nil
	}

	err = j.PopulateSteampipe(account, awsCred, azureCred)
	if err != nil {
		return err, nil
	}

	cmd := exec.Command("steampipe", "service", "stop", "--force")
	err = cmd.Start()
	if err != nil {
		return err, nil
	}
	time.Sleep(5 * time.Second)
	//NOTE: stop must be called twice. it's not a mistake
	cmd = exec.Command("steampipe", "service", "stop", "--force")
	err = cmd.Start()
	if err != nil {
		return err, nil
	}
	time.Sleep(5 * time.Second)

	cmd = exec.Command("steampipe", "service", "start", "--database-listen", "network", "--database-port",
		"9193", "--database-password", "abcd")
	err = cmd.Run()
	if err != nil {
		return err, nil
	}
	time.Sleep(5 * time.Second)

	s1, err := steampipe.NewSteampipeDatabase(steampipe.Option{
		Host: "localhost",
		Port: "9193",
		User: "steampipe",
		Pass: "abcd",
		Db:   "steampipe",
	})
	if err != nil {
		return err, nil
	}
	defer s1.Conn().Close()

	j.steampipe = s1

	j.logger.Info("running query", zap.String("account", account.ConnectionID), zap.String("query", query.ListQuery))
	listQuery := strings.ReplaceAll(query.ListQuery, "%ACCOUNT_ID%", account.ConnectionID)
	listQuery = strings.ReplaceAll(listQuery, "%KAYTU_ACCOUNT_ID%", account.ID.String())
	steampipeRows, err := j.steampipe.Conn().Query(context.Background(), listQuery)
	if err != nil {
		return err, nil
	}
	defer steampipeRows.Close()

	var mismatches []QueryMismatch
	var columns []string
	rowCount := 0
	for steampipeRows.Next() {
		rowCount++
		steampipeRow, err := steampipeRows.Values()
		if err != nil {
			return err, nil
		}

		steampipeRecord := map[string]interface{}{}
		for idx, field := range steampipeRows.FieldDescriptions() {
			steampipeRecord[string(field.Name)] = steampipeRow[idx]
		}

		getQuery := strings.ReplaceAll(query.GetQuery, "%ACCOUNT_ID%", account.ConnectionID)
		getQuery = strings.ReplaceAll(getQuery, "%KAYTU_ACCOUNT_ID%", account.ID.String())

		var keyValues []interface{}
		for _, f := range query.KeyFields {
			keyValues = append(keyValues, steampipeRecord[f])
		}

		esRows, err := j.esSteampipe.Conn().Query(context.Background(), getQuery, keyValues...)
		if err != nil {
			return err, nil
		}

		found := false

		for esRows.Next() {
			esRow, err := esRows.Values()
			if err != nil {
				return err, nil
			}

			found = true

			esRecord := map[string]interface{}{}
			for idx, field := range esRows.FieldDescriptions() {
				esRecord[string(field.Name)] = esRow[idx]
			}

			for k, v := range steampipeRecord {
				v2 := esRecord[k]

				j1, err := json.Marshal(v)
				if err != nil {
					return err, nil
				}

				j2, err := json.Marshal(v2)
				if err != nil {
					return err, nil
				}

				sj1 := strings.ToLower(string(j1))
				sj2 := strings.ToLower(string(j2))

				if sj1 == "null" {
					sj1 = "{}"
				}
				if sj2 == "null" {
					sj2 = "{}"
				}

				if sj1 != sj2 {
					if compareJsons(j2, j1) {
						ReporterJobsCount.WithLabelValues(query.TableName, "Succeeded").Inc()
						continue
					}
					ReporterJobsCount.WithLabelValues(query.TableName, "Failed").Inc()
					hasColumn := false
					for _, c := range columns {
						if c == k {
							hasColumn = true
							break
						}
					}
					if !hasColumn {
						columns = append(columns, k)
					}
					mismatches = append(mismatches, QueryMismatch{
						KeyColumn:      fmt.Sprintf("%v", keyValues),
						ConflictColumn: k,
						Steampipe:      sj1,
						Elasticsearch:  sj2,
					})
					if k != "etag" && k != "tags" {
						j.logger.Warn("inconsistency in data",
							zap.String("get-query", query.GetQuery),
							zap.String("accountID", account.ConnectionID),
							zap.String("steampipe", sj1),
							zap.String("es", sj2),
							zap.String("conflictColumn", k),
							zap.String("keyColumns", fmt.Sprintf("%v", keyValues)),
						)
					}
				} else {
					ReporterJobsCount.WithLabelValues(query.TableName, "Succeeded").Inc()
				}
			}
		}

		if !found {
			mismatches = append(mismatches, QueryMismatch{
				KeyColumn:      fmt.Sprintf("%v", keyValues),
				ConflictColumn: "",
				Steampipe:      "",
				Elasticsearch:  "Record Not Found",
			})
			ReporterJobsCount.WithLabelValues(query.TableName, "Failed").Inc()
			j.logger.Warn("record not found",
				zap.String("get-query", query.GetQuery),
				zap.String("accountID", account.ConnectionID),
				zap.String("keyColumns", fmt.Sprintf("%v", keyValues)),
			)
		}
	}

	j.logger.Info("Done", zap.Int("rowCount", rowCount))

	return nil, &TriggerQueryResponse{
		TotalRows:          rowCount,
		NotMatchingColumns: columns,
		Mismatches:         mismatches,
	}
}

func (j *Job) RandomAccount() (*api2.Connection, error) {
	srcs, err := j.onboardClient.ListSources(&httpclient.Context{
		UserRole: api.AdminRole,
	}, nil)
	if err != nil {
		return nil, err
	}

	idx := rand.Intn(len(srcs))
	return &srcs[idx], nil
}

func (j *Job) RandomQuery(sourceType source.Type) *Query {
	switch sourceType {
	case source.CloudAWS:
		idx := rand.Intn(len(awsQueries))
		return &awsQueries[idx]
	case source.CloudAzure:
		idx := rand.Intn(len(azureQueries))
		return &azureQueries[idx]
	}
	return nil
}

func (j *Job) PopulateSteampipe(account *api2.Connection, awsCred *api2.AWSCredentialConfig, azureCred *api2.AzureCredentialConfig) error {
	dirname, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	filePath := path.Join(dirname, ".steampipe", "config", "steampipe.spc")
	os.MkdirAll(filepath.Dir(filePath), os.ModePerm)

	if awsCred != nil {
		credFilePath := path.Join(dirname, ".aws", "credentials")
		os.MkdirAll(filepath.Dir(credFilePath), os.ModePerm)
		content := fmt.Sprintf(`
[default]
aws_access_key_id = %s
aws_secret_access_key = %s
`,
			awsCred.AccessKey, awsCred.SecretKey)
		err = os.WriteFile(credFilePath, []byte(content), os.ModePerm)
		if err != nil {
			return err
		}

		configFilePath := path.Join(dirname, ".aws", "config")
		os.MkdirAll(filepath.Dir(configFilePath), os.ModePerm)

		assumeRoleConfigs := ""
		if awsCred.AssumeRoleName != "" && awsCred.AccountId != account.ConnectionID {
			assumeRoleConfigs = fmt.Sprintf("role_arn = arn:aws:iam::%s:role/%s\n", awsCred.AccountId, awsCred.AssumeRoleName)
			if awsCred.ExternalId != nil {
				assumeRoleConfigs += fmt.Sprintf("external_id = %s\n", *awsCred.ExternalId)
			}
		}
		content = fmt.Sprintf(`
[default]
region = us-east-1

[profile reporter]
region = us-east-1
source_profile = default
%s
`,
			assumeRoleConfigs)
		err = os.WriteFile(configFilePath, []byte(content), os.ModePerm)

		//os.Setenv("AWS_ACCESS_KEY_ID", awsCred.AccessKey)
		//os.Setenv("AWS_SECRET_ACCESS_KEY", awsCred.SecretKey)
		content = `
connection "aws" {
  plugin  = "aws"
  regions = ["*"]
  profile = "reporter"
}
`
		filePath = dirname + "/.steampipe/config/aws.spc"
		return os.WriteFile(filePath, []byte(content), os.ModePerm)
	}

	if azureCred != nil {
		content := fmt.Sprintf(`
connection "azure" {
  plugin = "azure"
  tenant_id       = "%s"
  subscription_id = "%s"
  client_id       = "%s"
  client_secret   = "%s"
}
`,
			azureCred.TenantId, account.ConnectionID, azureCred.ClientId, azureCred.ClientSecret)
		filePath = dirname + "/.steampipe/config/azure.spc"
		err = os.WriteFile(filePath, []byte(content), os.ModePerm)
		if err != nil {
			return err
		}

		content = fmt.Sprintf(`
connection "azuread" {
  plugin = "azuread"
  tenant_id       = "%s"
  client_id       = "%s"
  client_secret   = "%s"
}
`,
			azureCred.TenantId, azureCred.ClientId, azureCred.ClientSecret)
		filePath = dirname + "/.steampipe/config/azuread.spc"
		return os.WriteFile(filePath, []byte(content), os.ModePerm)
	}

	return nil
}

// json2 should be es and json1 should be steampipe
func compareJsons(j1, j2 []byte) bool {
	var o1 map[string]interface{}
	err := json.Unmarshal(j1, &o1)
	if err != nil {
		return false
	}

	var o2 map[string]interface{}
	err = json.Unmarshal(j2, &o2)
	if err != nil {
		return false
	}

	for k, v := range o1 {
		if v2, ok := o2[k]; ok {
			if v2 != v {
				return false
			}
		} else {
			return false
		}
	}
	return true
}
