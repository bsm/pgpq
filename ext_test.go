package pgpq

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
)

// SchemaVersion returns the current schema version.
func (c *Client) SchemaVersion(ctx context.Context) (string, error) {
	var version string
	if err := c.db.
		QueryRowContext(ctx, `SELECT value FROM pgpq_meta_info WHERE name = $1`, "schema_version").
		Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

// SetCurrentTime sets the (mock) current time for this Client.
func (c *Client) SetCurrentTime(t time.Time) {
	clk := clock.NewMock()
	clk.Set(t)
	c.clock = clk
}
