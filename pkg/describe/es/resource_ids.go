package es

import (
	"context"
	"encoding/json"

	"gitlab.com/keibiengine/keibi-engine/pkg/keibi-es-sdk"
)

type ResourceIdentifierFetchResponse struct {
	Hits ResourceIdentifierFetchHits `json:"hits"`
}
type ResourceIdentifierFetchHits struct {
	Total keibi.SearchTotal            `json:"total"`
	Hits  []ResourceIdentifierFetchHit `json:"hits"`
}
type ResourceIdentifierFetchHit struct {
	ID      string         `json:"_id"`
	Score   float64        `json:"_score"`
	Index   string         `json:"_index"`
	Type    string         `json:"_type"`
	Version int64          `json:"_version,omitempty"`
	Source  LookupResource `json:"_source"`
	Sort    []interface{}  `json:"sort"`
}

func GetResourceIDsForAccountResourceTypeFromES(client keibi.Client, sourceID, resourceType string, searchAfter []interface{}, from int) (*ResourceIdentifierFetchResponse, error) {
	terms := map[string][]string{
		"source_id":     {sourceID},
		"resource_type": {resourceType},
	}

	root := map[string]interface{}{}
	root["search_after"] = searchAfter
	root["from"] = from

	root["sort"] = []map[string]interface{}{
		{
			"_id": "desc",
		},
	}

	boolQuery := make(map[string]interface{})
	var filters []map[string]interface{}
	for k, vs := range terms {
		filters = append(filters, map[string]interface{}{
			"terms": map[string][]string{
				k: vs,
			},
		})
	}
	boolQuery["filter"] = filters
	root["query"] = map[string]interface{}{
		"bool": boolQuery,
	}

	queryBytes, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}

	var response ResourceIdentifierFetchResponse
	err = client.Search(context.Background(), InventorySummaryIndex,
		string(queryBytes), &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}
