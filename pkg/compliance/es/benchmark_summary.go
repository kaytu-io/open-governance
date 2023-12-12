package es

import (
	"context"
	"encoding/json"
	"fmt"
	types2 "github.com/kaytu-io/kaytu-engine/pkg/compliance/summarizer/types"
	"github.com/kaytu-io/kaytu-engine/pkg/types"
	"sort"
	"strings"
	"time"

	"github.com/kaytu-io/kaytu-util/pkg/kaytu-es-sdk"
	"go.uber.org/zap"
)

type TrendDatapoint struct {
	DateEpoch int64
	Score     float64
}

type FetchBenchmarkSummaryTrendAggregatedResponse struct {
	Aggregations struct {
		BenchmarkIDGroup struct {
			Buckets []struct {
				Key                   string `json:"key"`
				EvaluatedAtRangeGroup struct {
					Buckets []struct {
						From      float64 `json:"from"`
						To        float64 `json:"to"`
						DocCount  int     `json:"doc_count"`
						HitSelect struct {
							Hits struct {
								Hits []struct {
									Source types2.BenchmarkSummary `json:"_source"`
								} `json:"hits"`
							} `json:"hits"`
						} `json:"hit_select"`
					} `json:"buckets"`
				} `json:"evaluated_at_range_group"`
			} `json:"buckets"`
		} `json:"benchmark_id_group"`
	} `json:"aggregations"`
}

func FetchBenchmarkSummaryTrendByConnectionID(logger *zap.Logger, client kaytu.Client, benchmarkIDs []string, connectionIDs []string, from, to time.Time) (map[string][]TrendDatapoint, error) {
	pathFilters := make([]string, 0, len(connectionIDs)+4)
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.key")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.from")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.to")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.hit_select.hits.hits._source.Connections.BenchmarkResult.Result.SecurityScore")
	for _, connectionID := range connectionIDs {
		pathFilters = append(pathFilters,
			fmt.Sprintf("aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.hit_select.hits.hits._source.Connections.Connections.%s.Result.SecurityScore", connectionID))
	}

	query := make(map[string]any)

	startTimeUnix := from.Truncate(24 * time.Hour).Unix()
	endTimeUnix := to.Truncate(24 * time.Hour).Add(24 * time.Hour).Unix()
	step := int64((time.Hour * 24).Seconds())
	ranges := make([]map[string]any, 0, (endTimeUnix-startTimeUnix)/int64(step))
	for i := int64(0); i*step < endTimeUnix-startTimeUnix; i++ {
		ranges = append(ranges, map[string]any{
			"from": startTimeUnix + i*step,
			"to":   startTimeUnix + (i+1)*step,
		})
	}

	filters := make([]any, 0)
	filters = append(filters, map[string]any{
		"range": map[string]any{
			"EvaluatedAtEpoch": map[string]any{
				"lte": to.Unix(),
				"gte": from.Unix(),
			},
		},
	})
	if len(benchmarkIDs) > 0 {
		filters = append(filters, map[string]any{
			"terms": map[string][]string{
				"BenchmarkID": benchmarkIDs,
			},
		})
	}
	query["query"] = map[string]any{
		"bool": map[string]any{
			"filter": filters,
		},
	}
	query["size"] = 0

	query["aggs"] = map[string]any{
		"benchmark_id_group": map[string]any{
			"terms": map[string]any{
				"field": "BenchmarkID",
				"size":  10000,
			},
			"aggs": map[string]any{
				"evaluated_at_range_group": map[string]any{
					"range": map[string]any{
						"field":  "EvaluatedAtEpoch",
						"ranges": ranges,
					},
					"aggs": map[string]any{
						"hit_select": map[string]any{
							"top_hits": map[string]any{
								"sort": map[string]any{
									"EvaluatedAtEpoch": "desc",
								},
								"size": 1,
							},
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err))
		return nil, err
	}

	logger.Info("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.String("query", string(queryBytes)), zap.String("pathFilters", strings.Join(pathFilters, ",")))
	var response FetchBenchmarkSummaryTrendAggregatedResponse
	err = client.SearchWithFilterPath(context.Background(), types.BenchmarkSummaryIndex, string(queryBytes), pathFilters, &response)
	if err != nil {
		logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err), zap.String("query", string(queryBytes)))
		return nil, err
	}

	trend := make(map[string][]TrendDatapoint)
	for _, bucket := range response.Aggregations.BenchmarkIDGroup.Buckets {
		benchmarkID := bucket.Key
		for _, rangeBucket := range bucket.EvaluatedAtRangeGroup.Buckets {
			date := int64(rangeBucket.To)
			if err != nil {
				logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err), zap.String("query", string(queryBytes)))
				return nil, err
			}
			trendDataPoint := TrendDatapoint{}
			for _, hit := range rangeBucket.HitSelect.Hits.Hits {
				trendDataPoint.DateEpoch = date
				if len(connectionIDs) > 0 {
					for _, connectionID := range connectionIDs {
						if connection, ok := hit.Source.Connections.Connections[connectionID]; ok {
							trendDataPoint.Score += connection.Result.SecurityScore
						}
					}
				} else {
					trendDataPoint.Score += hit.Source.Connections.BenchmarkResult.Result.SecurityScore
				}
			}
			if trendDataPoint.DateEpoch != 0 {
				trend[benchmarkID] = append(trend[benchmarkID], trendDataPoint)
			}
		}
		sort.Slice(trend[benchmarkID], func(i, j int) bool {
			return trend[benchmarkID][i].DateEpoch < trend[benchmarkID][j].DateEpoch
		})
	}

	return trend, nil
}

func FetchBenchmarkSummaryTrendByResourceCollectionAndConnectionID(logger *zap.Logger, client kaytu.Client, benchmarkIDs []string, connectionIDs []string, resourceCollections []string, from, to time.Time) (map[string][]TrendDatapoint, error) {
	if len(resourceCollections) == 0 {
		return nil, fmt.Errorf("resource collections cannot be empty")
	}
	pathFilters := make([]string, 0, (len(connectionIDs)+1)*len(resourceCollections)+3)
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.key")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.from")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.to")
	for _, resourceCollection := range resourceCollections {
		pathFilters = append(pathFilters, fmt.Sprintf("aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.hit_select.hits.hits._source.ResourceCollections.%s.BenchmarkResult.Result.SecurityScore", resourceCollection))
		for _, connectionID := range connectionIDs {
			pathFilters = append(pathFilters,
				fmt.Sprintf("aggregations.benchmark_id_group.buckets.evaluated_at_range_group.buckets.hit_select.hits.hits._source.ResourceCollections.%s.Connections.%s.Result.SecurityScore", resourceCollection, connectionID))
		}
	}

	query := make(map[string]any)

	startTimeUnix := from.Truncate(24 * time.Hour).Unix()
	endTimeUnix := to.Truncate(24 * time.Hour).Add(24 * time.Hour).Unix()
	step := int64((time.Hour * 24).Seconds())
	ranges := make([]map[string]any, 0, (endTimeUnix-startTimeUnix)/int64(step))
	for i := int64(0); i*step < endTimeUnix-startTimeUnix; i++ {
		ranges = append(ranges, map[string]any{
			"from": startTimeUnix + i*step,
			"to":   startTimeUnix + (i+1)*step,
		})
	}

	filters := make([]any, 0)
	filters = append(filters, map[string]any{
		"range": map[string]any{
			"EvaluatedAtEpoch": map[string]any{
				"lte": to.Unix(),
				"gte": from.Unix(),
			},
		},
	})
	if len(benchmarkIDs) > 0 {
		filters = append(filters, map[string]any{
			"terms": map[string][]string{
				"BenchmarkID": benchmarkIDs,
			},
		})
	}
	query["query"] = map[string]any{
		"bool": map[string]any{
			"filter": filters,
		},
	}
	query["size"] = 0

	query["aggs"] = map[string]any{
		"benchmark_id_group": map[string]any{
			"terms": map[string]any{
				"field": "BenchmarkID",
				"size":  10000,
			},
			"aggs": map[string]any{
				"evaluated_at_range_group": map[string]any{
					"range": map[string]any{
						"field":  "EvaluatedAtEpoch",
						"ranges": ranges,
					},
					"aggs": map[string]any{
						"hit_select": map[string]any{
							"top_hits": map[string]any{
								"sort": map[string]any{
									"EvaluatedAtEpoch": "desc",
								},
								"size": 1,
							},
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err))
		return nil, err
	}

	logger.Info("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.String("query", string(queryBytes)), zap.String("pathFilters", strings.Join(pathFilters, ",")))
	var response FetchBenchmarkSummaryTrendAggregatedResponse
	err = client.SearchWithFilterPath(context.Background(), types.BenchmarkSummaryIndex, string(queryBytes), pathFilters, &response)
	if err != nil {
		logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err), zap.String("query", string(queryBytes)))
		return nil, err
	}

	trend := make(map[string][]TrendDatapoint)
	for _, bucket := range response.Aggregations.BenchmarkIDGroup.Buckets {
		benchmarkID := bucket.Key
		for _, rangeBucket := range bucket.EvaluatedAtRangeGroup.Buckets {
			date := int64(rangeBucket.To)
			if err != nil {
				logger.Error("FetchBenchmarkSummaryTrendByConnectionIDAtTime", zap.Error(err), zap.String("query", string(queryBytes)))
				return nil, err
			}
			trendDataPoint := TrendDatapoint{}
			for _, hit := range rangeBucket.HitSelect.Hits.Hits {
				trendDataPoint.DateEpoch = date
				for _, resourceCollection := range hit.Source.ResourceCollections {
					if len(connectionIDs) > 0 {
						for _, connectionID := range connectionIDs {
							if connection, ok := resourceCollection.Connections[connectionID]; ok {
								trendDataPoint.Score += connection.Result.SecurityScore
							}
						}
					} else {
						trendDataPoint.Score += resourceCollection.BenchmarkResult.Result.SecurityScore
					}
				}
			}
			if trendDataPoint.DateEpoch != 0 {
				trend[benchmarkID] = append(trend[benchmarkID], trendDataPoint)
			}
		}
		sort.Slice(trend[benchmarkID], func(i, j int) bool {
			return trend[benchmarkID][i].DateEpoch < trend[benchmarkID][j].DateEpoch
		})
	}

	return trend, nil
}

func FetchBenchmarkSummaryTrend(logger *zap.Logger, client kaytu.Client, benchmarkIDs []string, connectionIDs, resourceCollections []string, from, to time.Time) (map[string][]TrendDatapoint, error) {
	if len(resourceCollections) > 0 {
		return FetchBenchmarkSummaryTrendByResourceCollectionAndConnectionID(logger, client, benchmarkIDs, connectionIDs, resourceCollections, from, to)
	}
	return FetchBenchmarkSummaryTrendByConnectionID(logger, client, benchmarkIDs, connectionIDs, from, to)
}

type ListBenchmarkSummariesAtTimeResponse struct {
	Aggregations struct {
		Summaries struct {
			Buckets []struct {
				Key        string `json:"key"`
				DocCount   int    `json:"doc_count"`
				LastResult struct {
					Hits struct {
						Hits []struct {
							Source types2.BenchmarkSummary `json:"_source"`
						} `json:"hits"`
					} `json:"hits"`
				} `json:"last_result"`
			} `json:"buckets"`
		} `json:"summaries"`
	} `json:"aggregations"`
}

func ListBenchmarkSummariesAtTime(logger *zap.Logger, client kaytu.Client,
	benchmarkIDs []string, connectionIDs []string, resourceCollections []string,
	timeAt time.Time) (map[string]types2.BenchmarkSummary, error) {

	idx := types.BenchmarkSummaryIndex

	includes := []string{"Connections.BenchmarkResult.Result", "EvaluatedAtEpoch"}
	if len(connectionIDs) > 0 {
		includes = append(includes, "Connections.Connections")
	}
	if len(resourceCollections) > 0 {
		includes = append(includes, "ResourceCollections")
	}
	pathFilters := make([]string, 0, len(connectionIDs)+(len(resourceCollections)*(len(connectionIDs)+1))+2)
	pathFilters = append(pathFilters, "aggregations.summaries.buckets.key")
	pathFilters = append(pathFilters, "aggregations.summaries.buckets.last_result.hits.hits._source.EvaluatedAtEpoch")
	pathFilters = append(pathFilters, "aggregations.summaries.buckets.last_result.hits.hits._source.Connections.BenchmarkResult.Result")
	for _, connectionID := range connectionIDs {
		pathFilters = append(pathFilters,
			fmt.Sprintf("aggregations.summaries.buckets.last_result.hits.hits._source.Connections.Connections.%s.Result", connectionID))
	}
	for _, resourceCollection := range resourceCollections {
		pathFilters = append(pathFilters,
			fmt.Sprintf("aggregations.summaries.buckets.last_result.hits.hits._source.ResourceCollections.%s.BenchmarkResult.Result", resourceCollection))
		for _, connectionID := range connectionIDs {
			pathFilters = append(pathFilters,
				fmt.Sprintf("aggregations.summaries.buckets.last_result.hits.hits._source.ResourceCollections.%s.Connections.%s.Result", resourceCollection, connectionID))
		}
	}

	request := map[string]any{
		"aggs": map[string]any{
			"summaries": map[string]any{
				"terms": map[string]any{
					"field": "BenchmarkID",
				},
				"aggs": map[string]any{
					"last_result": map[string]any{
						"top_hits": map[string]any{
							"sort": []map[string]any{
								{
									"JobID": "desc",
								},
							},
							"_source": map[string]any{
								"includes": includes,
							},
							"size": 1,
						},
					},
				},
			},
		},
		"size": 0,
	}

	filters := make([]any, 0)
	filters = append(filters, map[string]any{
		"range": map[string]any{
			"EvaluatedAtEpoch": map[string]any{
				"lte": timeAt.Unix(),
			},
		},
	})
	if len(benchmarkIDs) > 0 {
		filters = append(filters, map[string]any{
			"terms": map[string][]string{
				"BenchmarkID": benchmarkIDs,
			},
		})
	}

	request["query"] = map[string]any{
		"bool": map[string]any{
			"filter": filters,
		},
	}

	query, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	logger.Info("FetchBenchmarkSummariesByConnectionIDAtTime", zap.String("query", string(query)), zap.String("index", idx))

	var response ListBenchmarkSummariesAtTimeResponse
	err = client.SearchWithFilterPath(context.Background(), idx, string(query), pathFilters, &response)
	if err != nil {
		return nil, err
	}

	benchmarkSummaries := make(map[string]types2.BenchmarkSummary)
	for _, summary := range response.Aggregations.Summaries.Buckets {
		for _, hit := range summary.LastResult.Hits.Hits {
			benchmarkSummaries[summary.Key] = hit.Source
		}
	}
	return benchmarkSummaries, nil
}

type BenchmarkSummaryResponse struct {
	Aggregations struct {
		LastResult struct {
			Hits struct {
				Hits []struct {
					Source types2.BenchmarkSummary `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"last_result"`
	} `json:"aggregations"`
}

func BenchmarkConnectionSummary(logger *zap.Logger, client kaytu.Client, benchmarkID string) (map[string]types2.ResultGroup, int64, error) {
	includes := []string{"Connections.Connections", "EvaluatedAtEpoch"}
	pathFilters := make([]string, 0, 2)
	pathFilters = append(pathFilters, "aggregations.last_result.hits.hits._source.Connections.Connections.*.Result")
	pathFilters = append(pathFilters, "aggregations.last_result.hits.hits._source.EvaluatedAtEpoch")
	request := map[string]any{
		"aggs": map[string]any{
			"last_result": map[string]any{
				"top_hits": map[string]any{
					"sort": []map[string]any{
						{
							"JobID": "desc",
						},
					},
					"_source": map[string]any{
						"includes": includes,
					},
					"size": 1,
				},
			},
		},
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"term": map[string]any{
							"BenchmarkID": benchmarkID,
						},
					},
				},
			},
		},
		"size": 0,
	}

	queryBytes, err := json.Marshal(request)
	if err != nil {
		return nil, -1, err
	}

	logger.Info("BenchmarkConnectionSummary", zap.String("query", string(queryBytes)))
	var resp BenchmarkSummaryResponse
	err = client.SearchWithFilterPath(context.Background(), types.BenchmarkSummaryIndex, string(queryBytes), pathFilters, &resp)
	if err != nil {
		return nil, -1, err
	}

	for _, res := range resp.Aggregations.LastResult.Hits.Hits {
		return res.Source.Connections.Connections, res.Source.EvaluatedAtEpoch, nil
	}
	return nil, 0, nil
}

func BenchmarkControlSummary(logger *zap.Logger, client kaytu.Client, benchmarkID string, connectionIDs []string) (map[string]types2.ControlResult, int64, error) {
	includes := []string{"Connections.BenchmarkResult.Controls", "EvaluatedAtEpoch"}
	if len(connectionIDs) > 0 {
		includes = append(includes, "Connections.Connections")
	}

	pathFilters := make([]string, 0, len(connectionIDs)+2)
	pathFilters = append(pathFilters, "aggregations.last_result.hits.hits._source.Connections.BenchmarkResult.Controls")
	pathFilters = append(pathFilters, "aggregations.last_result.hits.hits._source.EvaluatedAtEpoch")
	for _, connectionID := range connectionIDs {
		pathFilters = append(pathFilters,
			fmt.Sprintf("aggregations.last_result.hits.hits._source.Connections.Connections.%s.Controls", connectionID))
	}

	request := map[string]any{
		"aggs": map[string]any{
			"last_result": map[string]any{
				"top_hits": map[string]any{
					"sort": []map[string]any{
						{
							"JobID": "desc",
						},
					},
					"_source": map[string]any{
						"includes": includes,
					},
					"size": 1,
				},
			},
		},
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"term": map[string]any{
							"BenchmarkID": benchmarkID,
						},
					},
				},
			},
		},
		"size": 0,
	}

	queryBytes, err := json.Marshal(request)
	if err != nil {
		return nil, -1, err
	}

	logger.Info("BenchmarkControlSummary", zap.String("query", string(queryBytes)), zap.String("pathFilters", strings.Join(pathFilters, ",")))
	var resp BenchmarkSummaryResponse
	err = client.SearchWithFilterPath(context.Background(), types.BenchmarkSummaryIndex,
		string(queryBytes), pathFilters, &resp)
	if err != nil {
		return nil, -1, err
	}

	evAt := int64(0)
	result := make(map[string]types2.ControlResult)
	for _, res := range resp.Aggregations.LastResult.Hits.Hits {
		if len(connectionIDs) > 0 {
			for _, connectionID := range connectionIDs {
				if connection, ok := res.Source.Connections.Connections[connectionID]; ok {
					for key, controlRes := range connection.Controls {
						if v, ok := result[key]; !ok {
							result[key] = controlRes
						} else {
							v.FailedResourcesCount += controlRes.FailedResourcesCount
							v.FailedConnectionCount += controlRes.FailedConnectionCount
							v.TotalResourcesCount += controlRes.TotalResourcesCount
							v.TotalConnectionCount += controlRes.TotalConnectionCount
							v.Passed = v.Passed && controlRes.Passed
							result[key] = v
						}
					}
					evAt = res.Source.EvaluatedAtEpoch
				}
			}
		} else {
			result = res.Source.Connections.BenchmarkResult.Controls
			evAt = res.Source.EvaluatedAtEpoch
			break
		}
	}
	return result, evAt, nil
}

type BenchmarksControlSummaryResponse struct {
	Aggregations struct {
		BenchmarkIDGroup struct {
			Buckets []struct {
				Key          string `json:"key"`
				LatestResult any    `json:"latest_result"`
				LastResult   struct {
					Hits struct {
						Hits []struct {
							Source types2.BenchmarkSummary `json:"_source"`
						} `json:"hits"`
					} `json:"hits"`
				} `json:"last_result"`
			}
		}
	}
}

func BenchmarksControlSummary(logger *zap.Logger, client kaytu.Client, benchmarkIDs []string, connectionIDs []string) (map[string]types2.ControlResult, map[string]int64, error) {
	includes := []string{"Connections.BenchmarkResult.Controls", "EvaluatedAtEpoch"}
	if len(connectionIDs) > 0 {
		includes = append(includes, "Connections.Connections")
	}

	pathFilters := make([]string, 0, len(connectionIDs)+2)
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.key")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.last_result.hits.hits._source.EvaluatedAtEpoch")
	pathFilters = append(pathFilters, "aggregations.benchmark_id_group.buckets.last_result.hits.hits._source.Connections.BenchmarkResult.Controls")
	for _, connectionID := range connectionIDs {
		pathFilters = append(pathFilters,
			fmt.Sprintf("aggregations.benchmark_id_group.buckets.last_result.hits.hits._source.Connections.Connections.%s.Controls", connectionID))
	}

	request := map[string]any{
		"aggs": map[string]any{
			"benchmark_id_group": map[string]any{
				"terms": map[string]any{
					"field": "BenchmarkID",
					"size":  10000,
				},
				"aggs": map[string]any{
					"last_result": map[string]any{
						"top_hits": map[string]any{
							"sort": []map[string]any{
								{
									"JobID": "desc",
								},
							},
							"_source": map[string]any{
								"includes": includes,
							},
							"size": 1,
						},
					},
				},
			},
		},
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []map[string]any{
					{
						"terms": map[string][]string{
							"BenchmarkID": benchmarkIDs,
						},
					},
				},
			},
		},
		"size": 0,
	}

	queryBytes, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}

	logger.Info("BenchmarksControlSummary", zap.String("query", string(queryBytes)), zap.String("pathFilters", strings.Join(pathFilters, ",")))
	var resp BenchmarksControlSummaryResponse
	err = client.SearchWithFilterPath(context.Background(), types.BenchmarkSummaryIndex, string(queryBytes), pathFilters, &resp)
	if err != nil {
		logger.Error("BenchmarksControlSummary", zap.Error(err))
		return nil, nil, err
	}

	perBenchmarkEvAt := make(map[string]int64)
	perBenchmarkResult := make(map[string]map[string]types2.ControlResult)
	for _, bucket := range resp.Aggregations.BenchmarkIDGroup.Buckets {
		benchmarkID := bucket.Key
		perBenchmarkEvAt[benchmarkID] = int64(0)
		perBenchmarkResult[benchmarkID] = make(map[string]types2.ControlResult)
		for _, res := range bucket.LastResult.Hits.Hits {
			if len(connectionIDs) > 0 {
				for _, connectionID := range connectionIDs {
					if connection, ok := res.Source.Connections.Connections[connectionID]; ok {
						for key, controlRes := range connection.Controls {
							if v, ok := perBenchmarkResult[benchmarkID][key]; !ok {
								perBenchmarkResult[benchmarkID][key] = controlRes
							} else {
								v.FailedResourcesCount += controlRes.FailedResourcesCount
								v.FailedConnectionCount += controlRes.FailedConnectionCount
								v.TotalResourcesCount += controlRes.TotalResourcesCount
								v.TotalConnectionCount += controlRes.TotalConnectionCount
								v.Passed = v.Passed && controlRes.Passed
								perBenchmarkResult[benchmarkID][key] = v
							}
						}
						perBenchmarkEvAt[benchmarkID] = res.Source.EvaluatedAtEpoch
					}
				}
			} else {
				perBenchmarkResult[benchmarkID] = res.Source.Connections.BenchmarkResult.Controls
				perBenchmarkEvAt[benchmarkID] = res.Source.EvaluatedAtEpoch
				break
			}
		}
	}

	evAt := make(map[string]int64)
	result := make(map[string]types2.ControlResult)
	for benchmarkID, controlResults := range perBenchmarkResult {
		for key, controlRes := range controlResults {
			if _, ok := result[key]; !ok || evAt[key] < perBenchmarkEvAt[benchmarkID] {
				result[key] = controlRes
				evAt[key] = perBenchmarkEvAt[benchmarkID]
			}
		}
	}
	return result, evAt, nil
}
