package api

import "gitlab.com/keibiengine/keibi-engine/pkg/source"

type ListInsightResultsRequest struct {
	Provider   *source.Type `json:"provider"`
	SourceID   *string      `json:"sourceID"`
	ExecutedAt *int64       `json:"executedAt"`
}

type ListInsightResultsResponse struct {
	Results []InsightResult `json:"results"`
}

type InsightResult struct {
	QueryID          uint   `json:"queryID"`
	SmartQueryID     uint   `json:"smartQueryID"`
	Query            string `json:"query"`
	Category         string `json:"category"`
	Provider         string `json:"provider"`
	SourceID         string `json:"sourceID"`
	Description      string `json:"description"`
	ExecutedAt       int64  `json:"executedAt"`
	Result           int64  `json:"result"`
	LastDayValue     *int64 `json:"lastDayValue"`
	LastWeekValue    *int64 `json:"lastWeekValue"`
	LastMonthValue   *int64 `json:"lastMonthValue"`
	LastQuarterValue *int64 `json:"lastQuarterValue"`
	LastYearValue    *int64 `json:"lastYearValue"`
}

type GetInsightResultTrendRequest struct {
	QueryID  uint         `json:"queryID"`
	Provider *source.Type `json:"provider"`
	SourceID *string      `json:"sourceID"`
}

type GetInsightResultTrendResponse struct {
	Trend []TrendDataPoint `json:"trend"`
}
