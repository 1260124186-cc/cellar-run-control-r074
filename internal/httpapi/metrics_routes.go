package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

func registerMetricsRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("GET /v1/metrics", func(response http.ResponseWriter, _ *http.Request) {
		// The dashboard is a read-only fixed snapshot; building it performs no
		// business write and does not advance the snapshot revision.
		writeJSON(response, http.StatusOK, app.Dashboard())
	})
}
