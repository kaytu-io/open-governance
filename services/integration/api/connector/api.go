package connector

import (
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/opengovern/og-util/pkg/api"
	"github.com/opengovern/og-util/pkg/fp"
	httpserver2 "github.com/opengovern/og-util/pkg/httpserver"
	"github.com/opengovern/og-util/pkg/source"
	"github.com/opengovern/opengovernance/services/integration/api/entity"
	"github.com/opengovern/opengovernance/services/integration/model"
	"github.com/opengovern/opengovernance/services/integration/service"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"net/http"
)

type API struct {
	connectionSvc service.Connection
	connectorSvc  service.Connector
	tracer        trace.Tracer
	logger        *zap.Logger
}

func New(
	connectionSvc service.Connection,
	connectorSvc service.Connector,
	logger *zap.Logger,
) API {
	return API{
		connectionSvc: connectionSvc,
		connectorSvc:  connectorSvc,
		tracer:        otel.GetTracerProvider().Tracer("integration.http.connector"),
		logger:        logger.Named("source"),
	}
}

// List godoc
//
//	@Summary		List connectors
//	@Description	Returns list of all connectors
//	@Security		BearerToken
//	@Tags			connectors
//	@Produce		json
//	@Success		200	{object}	[]entity.ConnectorCount
//	@Router			/integration/api/v1/connectors [get]
func (h API) List(c echo.Context) error {
	ctx, span := h.tracer.Start(c.Request().Context(), "new_ListConnectors", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	connectors, err := h.connectorSvc.List(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	var res []entity.ConnectorCount

	for _, c := range connectors {
		span.AddEvent("information", trace.WithAttributes(
			attribute.String("connector name", string(c.Name)),
		))

		var count int64
		count, err = h.connectionSvc.Count(ctx, fp.Optional(c.Name), nil)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

		tags := make(map[string]any)
		err = json.Unmarshal(c.Tags, &tags)
		if err != nil {
			return err
		}

		res = append(res, entity.ConnectorCount{
			Connector: entity.Connector{
				Name:                string(c.Name),
				Label:               c.Label,
				ShortDescription:    c.ShortDescription,
				Description:         c.Description,
				Direction:           c.Direction,
				Status:              c.Status,
				Logo:                c.Logo,
				AutoOnboardSupport:  c.AutoOnboardSupport,
				AllowNewConnections: c.AllowNewConnections,
				MaxConnectionLimit:  c.MaxConnectionLimit,
				Tags:                tags,
				Tier:                entity.Tier(c.Tier),
			},
			ConnectionCount: count,
		})
	}

	return c.JSON(http.StatusOK, res)
}

// CatalogMetrics godoc
//
//	@Summary		List catalog metrics
//	@Description	Retrieving the list of metrics for catalog page.
//	@Security		BearerToken
//	@Tags			integration
//	@Produce		json
//	@Param			connector		query		[]source.Type			false	"Connector"
//	@Param			credentialType	query		[]entity.CredentialType	false	"filter by credential type"
//	@Success		200				{object}	entity.CatalogMetrics
//	@Router			/integration/api/v1/connectors/metrics [get]
func (h API) CatalogMetrics(c echo.Context) error {
	ctx := otel.GetTextMapPropagator().Extract(c.Request().Context(), propagation.HeaderCarrier(c.Request().Header))

	var metrics entity.CatalogMetrics

	ctx, span := h.tracer.Start(ctx, "catalog-metrics", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	connectors := source.ParseTypes(httpserver2.QueryArrayParam(c, "connector"))

	credentialTypes := model.ParseCredentialTypes(httpserver2.QueryArrayParam(c, "credentialType"))

	connections, err := h.connectionSvc.ListWithFilter(ctx, connectors, credentialTypes, nil, nil, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return echo.NewHTTPError(http.StatusInternalServerError)
	}
	span.End()

	for _, connection := range connections {
		metrics.TotalConnections++
		if connection.LifecycleState.IsEnabled() {
			metrics.ConnectionsEnabled++
		}

		switch connection.HealthState {
		case source.HealthStatusHealthy:
			metrics.HealthyConnections++
		case source.HealthStatusUnhealthy:
			metrics.UnhealthyConnections++
		}

		if connection.LifecycleState == model.ConnectionLifecycleStateInProgress {
			metrics.InProgressConnections++
		}
	}

	return c.JSON(http.StatusOK, metrics)
}

func (s API) Register(g *echo.Group) {
	g.GET("", httpserver2.AuthorizeHandler(s.List, api.ViewerRole))
	g.GET("/metrics", httpserver2.AuthorizeHandler(s.CatalogMetrics, api.ViewerRole))
}
