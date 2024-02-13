package model

import (
	"github.com/jackc/pgtype"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

type JobSequencerStatus string

const (
	JobSequencerWaitingForDependencies JobSequencerStatus = "WaitingForDependencies"
	JobSequencerFinished               JobSequencerStatus = "FINISHED"
	JobSequencerFailed                 JobSequencerStatus = "Failed"
)

type JobSequencerJobType string

const (
	JobSequencerJobTypeBenchmarkRunner     JobSequencerJobType = "BenchmarkRunner"
	JobSequencerJobTypeBenchmark           JobSequencerJobType = "Benchmark"
	JobSequencerJobTypeBenchmarkSummarizer JobSequencerJobType = "BenchmarkSummarizer"
	JobSequencerJobTypeDescribe            JobSequencerJobType = "Describe"
	JobSequencerJobTypeAnalytics           JobSequencerJobType = "Analytics"
)

type JobSequencerJobTypeBenchmarkRunnerParameters struct {
	BenchmarkID   string
	ControlIDs    []string
	ConnectionIDs []string
}

type JobSequencer struct {
	gorm.Model
	DependencyList    pq.Int64Array `gorm:"type:bigint[]"`
	DependencySource  JobSequencerJobType
	NextJob           JobSequencerJobType
	NextJobParameters *pgtype.JSONB
	Status            JobSequencerStatus
}
