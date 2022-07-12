package openmetrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bsm/pgpq"
)

// Client defines pgpq client (only needed methods).
type Client interface {
	Stats(context.Context) ([]*pgpq.Stat, error)
}

// NewHandler constructs a new handler to serve queue metrics in OpenMetrics format.
func NewHandler(client Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats, err := client.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")

		if len(stats) == 0 {
			return
		}

		_, _ = fmt.Fprintf(w, "# TYPE queue_len gauge\n")
		_, _ = fmt.Fprintf(w, "# HELP queue_len Queue length per namespace.\n")
		for _, s := range stats {
			_, _ = fmt.Fprintf(w, "queue_len{namespace=%q} %d\n", s.Namespace, s.Len)
		}

		now := time.Now()
		_, _ = fmt.Fprintf(w, "# TYPE queue_oldest_message_age_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "# HELP queue_oldest_message_age_seconds Oldest message age in seconds.\n")
		for _, s := range stats {
			age := now.Sub(s.MinCreatedAt)
			_, _ = fmt.Fprintf(w, "queue_oldest_message_age_seconds{namespace=%q} %d\n", s.Namespace, int(age.Seconds()))
		}
	})
}
