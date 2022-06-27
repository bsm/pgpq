package pgpq

import "context"

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
