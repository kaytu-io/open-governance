package backend

import (
	"github.com/opengovern/opengovernance/pkg/workspace/costestimator/product"
)

//go:generate mockgen -destination=../mock/backend.go -mock_names=Backend=Backend -package mock github.com/opengovern/opengovernance/pkg/workspace/costestimator/backend Backend

// Backend represents a storage method used to store pricing data. It must include concrete implementations
// of all repositories.
type Backend interface {
	Products() product.Repository
}
