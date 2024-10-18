package integrations

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/labstack/echo/v4"
	"github.com/opengovern/og-util/pkg/api"
	"github.com/opengovern/og-util/pkg/httpserver"
	"github.com/opengovern/og-util/pkg/vault"
	"github.com/opengovern/opengovernance/services/integration-v2/api/models"
	"github.com/opengovern/opengovernance/services/integration-v2/db"
	integration_type "github.com/opengovern/opengovernance/services/integration-v2/integration-type"
	models2 "github.com/opengovern/opengovernance/services/integration-v2/models"
	"go.uber.org/zap"
	"net/http"
)

type API struct {
	vault    vault.VaultSourceConfig
	logger   *zap.Logger
	database db.Database
}

func New(
	vault vault.VaultSourceConfig,
	database db.Database,
	logger *zap.Logger,
) API {
	return API{
		vault:    vault,
		database: database,
		logger:   logger.Named("credentials"),
	}
}

// Create godoc
//
//	@Summary		Create credential
//	@Description	Create credential
//	@Security		BearerToken
//	@Tags			credentials
//	@Produce		json
//	@Success		200
//	@Param			request	body		entity.CreateRequest	true	"Request"
//	@Router			/integration/api/v1/integrations [post]
func (h API) Create(c echo.Context) error {
	var req models.CreateRequest

	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	jsonData, err := json.Marshal(req.Credentials)
	if err != nil {
		h.logger.Error("failed to marshal json data", zap.Error(err))
		return echo.NewHTTPError(http.StatusBadRequest, "failed to marshal json data")
	}

	if _, ok := integration_type.IntegrationTypes[req.IntegrationType]; !ok {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid integration type")
	}
	createCredentialFunction := integration_type.IntegrationTypes[req.IntegrationType]
	integration, mapData, err := createCredentialFunction(req.CredentialType, jsonData)

	if integration == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to marshal json data")
	}

	err = integration.HealthCheck()
	if err != nil {
		h.logger.Error("healthcheck failed", zap.Error(err))
		return echo.NewHTTPError(http.StatusBadRequest, "healthcheck failed")
	}

	integrations, err := integration.GetIntegrations()

	secret, err := h.vault.Encrypt(c.Request().Context(), mapData)
	if err != nil {
		h.logger.Error("failed to encrypt secret", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to encrypt config")
	}

	credentialID := uuid.New()

	credentialMetadataJsonb := pgtype.JSONB{}
	err = credentialMetadataJsonb.Set([]byte(""))
	err = h.database.CreateCredential(&models2.Credential{
		ID:             credentialID,
		Secret:         secret,
		CredentialType: req.CredentialType,
		Metadata:       credentialMetadataJsonb,
	})
	if err != nil {
		h.logger.Error("failed to create credential", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to create credential")
	}

	for _, i := range integrations {
		i.CredentialID = credentialID

		metadata, err := integration.GetMetadata()
		if err != nil {
			h.logger.Error("failed to get metadata", zap.Error(err))
		}
		metadataJsonData, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		integrationMetadataJsonb := pgtype.JSONB{}
		err = integrationMetadataJsonb.Set(metadataJsonData)
		i.Metadata = integrationMetadataJsonb

		annotations, err := integration.GetAnnotations()
		if err != nil {
			h.logger.Error("failed to get annotations", zap.Error(err))
		}
		annotationsJsonData, err := json.Marshal(annotations)
		if err != nil {
			return err
		}
		integrationAnnotationsJsonb := pgtype.JSONB{}
		err = integrationAnnotationsJsonb.Set(annotationsJsonData)
		i.Annotations = integrationAnnotationsJsonb

		err = h.database.CreateIntegration(&i)
		if err != nil {
			h.logger.Error("failed to create credential", zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to create credential")
		}
	}

	return c.NoContent(http.StatusOK)
}

// Delete godoc
//
//	@Summary		Delete credential
//	@Description	Delete credential
//	@Security		BearerToken
//	@Tags			credentials
//	@Produce		json
//	@Success		200
//	@Param			credentialId	path	string	true	"IntegrationID"
//	@Router			/integration/api/v1/integrations/{integrationId} [delete]
func (h API) Delete(c echo.Context) error {
	integrationId, err := uuid.Parse(c.Param("integrationId"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	err = h.database.DeleteIntegration(integrationId)
	if err != nil {
		h.logger.Error("failed to delete credential", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to delete credential")
	}

	return c.NoContent(http.StatusOK)
}

// List godoc
//
//	@Summary		List integrations
//	@Description	List integrations
//	@Security		BearerToken
//	@Tags			credentials
//	@Produce		json
//	@Success		200				{object}	models.ListResponse
//	@Router			/integration/api/v1/integrations [get]
func (h API) List(c echo.Context) error {
	integrations, err := h.database.ListIntegration()
	if err != nil {
		h.logger.Error("failed to list credentials", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to list credential")
	}

	var items []models.IntegrationItem
	for _, integration := range integrations {
		item, err := integration.ToApi()
		if err != nil {
			h.logger.Error("failed to convert integration to API model", zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to convert integration to API model")
		}
		items = append(items, *item)
	}

	return c.JSON(http.StatusOK, models.ListResponse{
		Integrations: items,
		TotalCount:   len(items),
	})
}

// Get godoc
//
//	@Summary		Get credential
//	@Description	Get credential
//	@Security		BearerToken
//	@Tags			credentials
//	@Produce		json
//	@Success		200
//	@Param			credentialId	path	string	true	"CredentialID"
//	@Router			/integration/api/v1/integrations/{integrationId} [get]
func (h API) Get(c echo.Context) error {
	integrationId, err := uuid.Parse(c.Param("integrationId"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	integration, err := h.database.GetIntegration(integrationId)
	if err != nil {
		h.logger.Error("failed to get credential", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get credential")
	}

	item, err := integration.ToApi()
	if err != nil {
		h.logger.Error("failed to convert integration to API model", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to convert integration to API model")
	}
	return c.JSON(http.StatusOK, item)
}

// Update godoc
//
//	@Summary		Get credential
//	@Description	Get credential
//	@Security		BearerToken
//	@Tags			credentials
//	@Produce		json
//	@Success		200
//	@Param			credentialId	path	string	true	"CredentialID"
//	@Param			request	body		entity.CreateRequest	true	"Request"
//	@Router			/integration/api/v1/integrations/{integrationId} [post]
func (h API) Update(c echo.Context) error {
	credId, err := uuid.Parse(c.Param("integrationId"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	var req models.CreateRequest

	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	credential, err := h.database.GetCredential(credId)
	if err != nil {
		h.logger.Error("failed to get credential", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get credential")
	}

	credentials, err := h.vault.Decrypt(c.Request().Context(), credential.Secret)
	if err != nil {
		h.logger.Error("failed to decrypt secret", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to decrypt config")
	}

	for k, v := range req.Credentials {
		credentials[k] = v
	}

	secret, err := h.vault.Encrypt(c.Request().Context(), credentials)
	if err != nil {
		h.logger.Error("failed to encrypt secret", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to encrypt config")
	}

	err = h.database.UpdateCredential(credId, secret)
	if err != nil {
		h.logger.Error("failed to update credential", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to update credential")
	}

	return c.JSON(http.StatusOK, models.CredentialItem{
		ID:             credential.ID.String(),
		CredentialType: credential.CredentialType,
	})
}

func (h API) Register(g *echo.Group) {
	g.GET("", httpserver.AuthorizeHandler(h.List, api.ViewerRole))
	g.POST("", httpserver.AuthorizeHandler(h.Create, api.EditorRole))
	g.DELETE("/:integrationId", httpserver.AuthorizeHandler(h.Delete, api.EditorRole))
	g.GET("/:integrationId", httpserver.AuthorizeHandler(h.Get, api.ViewerRole))
	g.POST("/:integrationId", httpserver.AuthorizeHandler(h.Update, api.EditorRole))
}
