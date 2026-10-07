package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

func registerMetricsRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("GET /v1/metrics/dashboard", func(response http.ResponseWriter, _ *http.Request) {
		dashboard, err := app.Dashboard()
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, dashboard)
	})
}
