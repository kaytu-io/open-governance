package healthz

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type Healthz struct{}

// Handle shows server is up and running.
func (h Healthz) Handle(c echo.Context) error {
	return c.NoContent(http.StatusNoContent)
}

// Register registers the routes of healthz handler on given echo group.
func (h Healthz) Register(g *echo.Group) {
	g.GET("/healthz", h.Handle)
}
