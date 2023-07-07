package api

type BenchmarkTimeResponse struct {
	Aggregations BenchmarkTimeAggregation `json:"aggregations"`
}
type BenchmarkTimeAggregation struct {
	ReportTime ReportTimeAggregate `json:"reportTime"`
}
type ReportTimeAggregate struct {
	Buckets []Bucket `json:"buckets"`
}
type Bucket struct {
	Key      int64 `json:"key"`
	DocCount int64 `json:"doc_count"`
}

func FilterIsEmpty(filter []string) bool {
	return filter == nil || len(filter) == 0
}

func BuildSort(sorts []ResourceSortItem) []map[string]string {
	var result []map[string]string
	for _, item := range sorts {
		dir := string(item.Direction)
		field := ""
		switch item.Field {
		case SortFieldResourceID:
			field = "resource_id"
		case SortFieldConnector:
			field = "source_type"
		case SortFieldResourceType:
			field = "resource_type"
		case SortFieldResourceGroup:
			field = "resource_group"
		case SortFieldLocation:
			field = "location"
		case SortFieldConnectionID:
			field = "source_id"
		}
		result = append(result, map[string]string{field: dir})
	}
	return result
}
