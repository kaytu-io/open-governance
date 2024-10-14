package onboard

import (
	"context"

	"github.com/opengovern/og-azure-describer/azure"
	"go.uber.org/zap"

	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscoverAzureSubscriptions(t *testing.T) {
	subs, err := discoverAzureSubscriptions(context.Background(), zap.NewNop(), azure.AuthConfig{
		TenantID:     "",
		ClientID:     "",
		ClientSecret: "",
	})
	require.NoError(t, err)
	require.NotEmpty(t, subs)
}
