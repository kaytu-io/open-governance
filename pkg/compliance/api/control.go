package api

import (
	"github.com/kaytu-io/kaytu-util/pkg/source"
	inventoryApi "github.com/kaytu-io/open-governance/pkg/inventory/api"
	"github.com/kaytu-io/open-governance/pkg/types"
	"time"
)

type Control struct {
	ID          string              `json:"id" example:"azure_cis_v140_1_1"`
	Title       string              `json:"title" example:"1.1 Ensure that multi-factor authentication status is enabled for all privileged users"`
	Description string              `json:"description" example:"Enable multi-factor authentication for all user credentials who have write access to Azure resources. These include roles like 'Service Co-Administrators', 'Subscription Owners', 'Contributors'."`
	Tags        map[string][]string `json:"tags"`

	Explanation       string `json:"explanation" example:"Multi-factor authentication adds an additional layer of security by requiring users to enter a code from a mobile device or phone in addition to their username and password when signing into Azure."`
	NonComplianceCost string `json:"nonComplianceCost" example:"Non-compliance to this control could result in several costs including..."`
	UsefulExample     string `json:"usefulExample" example:"Access to resources must be closely controlled to prevent malicious activity like data theft..."`

	CliRemediation          string `json:"cliRemediation" example:"To enable multi-factor authentication for a user, run the following command..."`
	ManualRemediation       string `json:"manualRemediation" example:"To enable multi-factor authentication for a user, run the following command..."`
	GuardrailRemediation    string `json:"guardrailRemediation" example:"To enable multi-factor authentication for a user, run the following command..."`
	ProgrammaticRemediation string `json:"programmaticRemediation" example:"To enable multi-factor authentication for a user, run the following command..."`

	Connector          []source.Type         `json:"connector" example:"Azure"`
	Enabled            bool                  `json:"enabled" example:"true"`
	DocumentURI        string                `json:"documentURI" example:"benchmarks/azure_cis_v140_1_1.md"`
	Query              *Query                `json:"query"`
	Severity           types.FindingSeverity `json:"severity" example:"low"`
	ManualVerification bool                  `json:"manualVerification" example:"true"`
	Managed            bool                  `json:"managed" example:"true"`
	CreatedAt          time.Time             `json:"createdAt" example:"2020-01-01T00:00:00Z"`
	UpdatedAt          time.Time             `json:"updatedAt" example:"2020-01-01T00:00:00Z"`
}

type ControlSummary struct {
	Control      Control                    `json:"control"`
	ResourceType *inventoryApi.ResourceType `json:"resourceType"`

	Benchmarks []Benchmark `json:"benchmarks"`

	Passed                bool      `json:"passed"`
	FailedResourcesCount  int       `json:"failedResourcesCount"`
	TotalResourcesCount   int       `json:"totalResourcesCount"`
	FailedConnectionCount int       `json:"failedConnectionCount"`
	TotalConnectionCount  int       `json:"totalConnectionCount"`
	CostOptimization      *float64  `json:"costOptimization"`
	EvaluatedAt           time.Time `json:"evaluatedAt"`
}

type ControlTrendDatapoint struct {
	Timestamp             int `json:"timestamp" example:"1686346668"` // Time
	FailedResourcesCount  int `json:"failedResourcesCount"`
	TotalResourcesCount   int `json:"totalResourcesCount"`
	FailedConnectionCount int `json:"failedConnectionCount"`
	TotalConnectionCount  int `json:"totalConnectionCount"`
}

type ControlsFilterSummaryRequest struct {
	Connector       []string            `json:"connector"`
	Severity        []string            `json:"severity"`
	RootBenchmark   []string            `json:"root_benchmark"`
	ParentBenchmark []string            `json:"parent_benchmark"`
	HasParameters   *bool               `json:"has_parameters"`
	PrimaryTable    []string            `json:"primary_table"`
	ListOfTables    []string            `json:"list_of_tables"`
	Tags            map[string][]string `json:"tags"`
	TagsRegex       *string             `json:"tags_regex"`
	FindingFilters  *FindingFilters     `json:"finding_filters"`
}

type ListControlsFilterRequest struct {
	Connector       []string            `json:"connector"`
	Severity        []string            `json:"severity"`
	RootBenchmark   []string            `json:"root_benchmark"`
	ParentBenchmark []string            `json:"parent_benchmark"`
	HasParameters   *bool               `json:"has_parameters"`
	PrimaryTable    []string            `json:"primary_table"`
	ListOfTables    []string            `json:"list_of_tables"`
	Tags            map[string][]string `json:"tags"`
	TagsRegex       *string             `json:"tags_regex"`
	FindingFilters  *FindingFilters     `json:"finding_filters"`
	FindingSummary  bool                `json:"finding_summary"`
	Cursor          *int64              `json:"cursor"`
	PerPage         *int64              `json:"per_page"`
}

type ListControlsFilterResponse struct {
	Items      []ListControlsFilterResultControl `json:"items"`
	TotalCount int                               `json:"total_count"`
}

type ListControlsFilterResultControl struct {
	ID          string                `json:"id"`
	Title       string                `json:"title"`
	Description string                `json:"description"`
	Connector   []source.Type         `json:"connector"`
	Severity    types.FindingSeverity `json:"severity"`
	Tags        map[string][]string   `json:"tags"`
	Query       struct {
		PrimaryTable *string          `json:"primary_table"`
		ListOfTables []string         `json:"list_of_tables"`
		Parameters   []QueryParameter `json:"parameters"`
	} `json:"query"`
	FindingsSummary map[string]int64 `json:"findings_summary"`
}

type ControlsFilterSummaryResult struct {
	ControlsCount int64               `json:"controls_count"`
	Connector     []string            `json:"connector"`
	Severity      []string            `json:"severity"`
	Tags          map[string][]string `json:"tags"`
	PrimaryTable  []string            `json:"primary_table"`
	ListOfTables  []string            `json:"list_of_tables"`
}

type ListControlsFilterResult struct {
	Controls []ListControlsFilterResultControl `json:"controls"`
	Summary  struct {
		Connector    []string            `json:"connector"`
		Severity     []string            `json:"severity"`
		Tags         map[string][]string `json:"tags"`
		PrimaryTable []string            `json:"primary_table"`
		ListOfTables []string            `json:"list_of_tables"`
	} `json:"summary"`
}

type ControlTagsResult struct {
	Key          string
	UniqueValues []string
}

type BenchmarkTagsResult struct {
	Key          string
	UniqueValues []string
}

type GetControlDetailsResponse struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Connector   []string `json:"connector"`
	Severity    string   `json:"severity"`
	Query       struct {
		Engine         string   `json:"engine"`
		QueryToExecute string   `json:"queryToExecute"`
		PrimaryTable   *string  `json:"primaryTable"`
		ListOfTables   []string `json:"listOfTables"`
	} `json:"query"`
	Tags       map[string][]string `json:"tags"`
	Benchmarks *struct {
		Roots    []string `json:"roots"`
		FullPath []string `json:"fullPath"`
	} `json:"benchmarks"`
}
