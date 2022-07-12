package openmetrics

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/bsm/pgpq"
)

// Client defines pgpq client (only needed methods).
type Client interface {
	Len(context.Context, ...pgpq.ScopeOption) (int64, error)
	MinCreatedAt(context.Context, ...pgpq.ScopeOption) (time.Time, error)
}

// NewHandler constructs a new handler to serve queue metrics in OpenMetrics format.
// It will serve stats either for provided namespaces or for default (empty) one.
func NewHandler(client Client, namespaces ...string) http.Handler {
	if len(namespaces) == 0 {
		namespaces = append(namespaces, "") // default namespace
	}

	return &handler{
		client:     client,
		namespaces: unique(namespaces),
	}
}

// ----------------------------------------------------------------------------

type handler struct {
	client     Client
	namespaces []string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	lens, ages, err := h.fetchStats(r.Context(), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")

	_, _ = fmt.Fprintf(w, "# TYPE queue_len gauge\n")
	_, _ = fmt.Fprintf(w, "# HELP queue_len Queue length per namespace.\n")
	for i, ns := range h.namespaces {
		_, _ = fmt.Fprintf(w, "queue_len{namespace=%q} %d\n", ns, lens[i])
	}

	_, _ = fmt.Fprintf(w, "# TYPE queue_oldest_message_age_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "# HELP queue_oldest_message_age_seconds Oldest message age in seconds.\n")
	for i, ns := range h.namespaces {
		_, _ = fmt.Fprintf(w, "queue_oldest_message_age_seconds{namespace=%q} %d\n", ns, ages[i])
	}
}

func (h *handler) fetchStats(ctx context.Context, now time.Time) (lens, ages []int64, _ error) {
	lens = make([]int64, 0, len(h.namespaces))
	ages = make([]int64, 0, len(h.namespaces))
	for _, ns := range h.namespaces {
		n, err := h.client.Len(ctx, pgpq.WithNamespace(ns))
		if err != nil {
			return nil, nil, fmt.Errorf("len: %w", err)
		}
		lens = append(lens, n)

		createdAt, err := h.client.MinCreatedAt(ctx, pgpq.WithNamespace(ns))
		if err != nil {
			return nil, nil, fmt.Errorf("age: %w", err)
		}
		age := now.Sub(createdAt).Seconds()
		ages = append(ages, int64(age))
	}
	return lens, ages, nil
}

// ----------------------------------------------------------------------------

func unique(ss []string) []string {
	if len(ss) == 0 {
		return ss
	}

	sort.Strings(ss)

	res := ss[:0]
	for _, s := range ss {
		if n := len(res); n > 0 && s == res[n-1] {
			continue
		}
		res = append(res, s)
	}
	return res
}
