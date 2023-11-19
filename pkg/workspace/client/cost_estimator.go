package client

import (
	"encoding/json"
	"fmt"
	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpclient"
	"net/http"
)

type CostEstimatorPricesClient interface {
	GetAzure(ctx *httpclient.Context, resourceType string, req any) (float64, error)
	GetAWS(ctx *httpclient.Context, resourceType string, req any) (float64, error)
}

type costEstimatorClient struct {
	baseURL string
}

func NewCostEstimatorClient(baseURL string) CostEstimatorPricesClient {
	return &costEstimatorClient{baseURL: baseURL}
}

func (s *costEstimatorClient) GetAzure(ctx *httpclient.Context, resourceType string, req any) (float64, error) {
	url := fmt.Sprintf("%s/api/v1/costestimator/azure/%s", s.baseURL, resourceType)

	payload, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	var response float64
	if _, err := httpclient.DoRequest(http.MethodGet, url, ctx.ToHeaders(), payload, &response); err != nil {
		return 0, err
	}
	return response, nil
}

func (s *costEstimatorClient) GetAWS(ctx *httpclient.Context, resourceType string, req any) (float64, error) {
	url := fmt.Sprintf("%s/api/v1/costestimator/aws/%s", s.baseURL, resourceType)

	payload, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	var response float64
	if _, err := httpclient.DoRequest(http.MethodGet, url, ctx.ToHeaders(), payload, &response); err != nil {
		return 0, err
	}
	return response, nil
}
