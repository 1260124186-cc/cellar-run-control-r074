package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/metrics"
)

// statusRecorder captures the final status code of a request so the
// metrics middleware can count the outcome exactly once.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.status = status
	r.wrote = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(p)
}

// withRequestMetrics counts every completed request exactly once, after
// the handler has finished. Recording is a constant-time counter update,
// so it does not slow down the request itself.
func withRequestMetrics(recorder *metrics.Recorder, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		wrapped := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		next.ServeHTTP(wrapped, request)
		recorder.ObserveRequest(wrapped.status)
	})
}
