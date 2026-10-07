package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

func NewHandler(app *service.Service) http.Handler {
	mux := http.NewServeMux()
	registerFormulaRoutes(mux, app)
	registerVesselRoutes(mux, app)
	registerRunRoutes(mux, app)
	registerMetricsRoutes(mux, app)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]any{
			"status":   "ok",
			"revision": app.Revision(),
		})
	})
	// Recovery must remain inside request counting: a handler panic is rendered
	// as a 500 by recovery and is still counted as one failed request.
	return withRequestCount(app, withRecovery(mux))
}

// statusRecorder captures the status code so the counting middleware can
// classify the outcome without touching the response body.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

// withRequestCount records one success/failure per served request using the
// lock-free registry, adding no business-lock contention to observation writes.
func withRequestCount(app *service.Service, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		recorder := &statusRecorder{ResponseWriter: response, status: 0}
		next.ServeHTTP(recorder, request)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		app.Metrics().RecordRequest(status < http.StatusBadRequest)
	})
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeJSON(response, http.StatusInternalServerError, errorResponse{
					Error: errorBody{
						Code:   "internal_failure",
						Detail: "request could not be completed",
					},
				})
			}
		}()
		next.ServeHTTP(response, request)
	})
}
