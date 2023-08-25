package httpserver

import (
	"context"
	"github.com/brpaz/echozap"
	"github.com/kaytu-io/kaytu-util/pkg/metrics"
	"github.com/labstack/echo-contrib/jaegertracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"io"

	"go.uber.org/zap"
	"gopkg.in/go-playground/validator.v9"
)

type Routes interface {
	Register(router *echo.Echo)
}

func Register(logger *zap.Logger, routes Routes) (*echo.Echo, io.Closer) {
	e := echo.New()
	e.HideBanner = true

	e.Use(middleware.Recover())
	e.Use(echozap.ZapLogger(logger))

	metrics.AddEchoMiddleware(e)

	e.Pre(middleware.RemoveTrailingSlash())

	c := jaegertracing.New(e, nil)

	e.Validator = customValidator{
		validate: validator.New(),
	}

	routes.Register(e)

	return e, c
}

func RegisterAndStart(logger *zap.Logger, address string, routes Routes) error {
	e, c := Register(logger, routes)

	defer c.Close()

	tp := trace.NewTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
		}
	}()
	otel.SetTracerProvider(tp)
	e.Use(otelecho.Middleware("kaytu-engine"))

	return e.Start(address)
}

type customValidator struct {
	validate *validator.Validate
}

func (v customValidator) Validate(i interface{}) error {
	return v.validate.Struct(i)
}

func QueryArrayParam(ctx echo.Context, paramName string) []string {
	var values []string
	for k, v := range ctx.QueryParams() {
		if k == paramName || k == paramName+"[]" {
			values = append(values, v...)
		}
	}
	return values
}
