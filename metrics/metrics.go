package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RunServer start a http server that print metrics in /metrics
func RunServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(addr, nil)
}
