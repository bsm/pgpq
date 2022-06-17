package pgpq

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/Masterminds/semver/v3"
)

//go:embed schema.sql
var embedFS embed.FS

const targetVersion = 1

func validateConn(ctx context.Context, db *sql.DB) error {
	if err := checkServerVersion(ctx, db); err != nil {
		return err
	}

	version, err := schemaVersion(ctx, db)
	if err != nil {
		return err
	}

	// Create schema if version is 0, migrate otherwise.
	if version == 0 {
		err = createSchema(ctx, db)
	} else {
		err = migrateSchema(ctx, db, version, targetVersion)
	}
	return err
}

var serverVersionRequired, _ = semver.NewConstraint(">= 9.5")

func checkServerVersion(ctx context.Context, db *sql.DB) error {
	var value string
	if err := db.QueryRowContext(ctx, `SELECT split_part(version(), ' ', 2)`).Scan(&value); err != nil {
		return fmt.Errorf("version check failed with %w", err)
	}

	version, err := semver.NewVersion(value)
	if err != nil {
		return fmt.Errorf("unexpected database version %q", value)
	} else if !serverVersionRequired.Check(version) {
		return fmt.Errorf("postgres server version %q is not meeting requirement %q", value, serverVersionRequired)
	}
	return nil
}

// tableExists returns true if a table exists.
func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var value string
	err := db.QueryRowContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_name = $1
	`, table).Scan(&value)

	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("table check failed with %w", err)
	}
	return true, nil
}

// schemaVersion returns the stored schema version.
func schemaVersion(ctx context.Context, db *sql.DB) (version int32, err error) {
	if ok, err := tableExists(ctx, db, "meta_info"); err != nil {
		return 0, err
	} else if !ok {
		return 0, nil
	}

	if err = db.QueryRowContext(ctx, `
		SELECT COALESCE(value::int, 0) AS version
		FROM meta_info
		WHERE name = $1
	`, "schema_version").Scan(&version); err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("schema check failed with %w", err)
	}
	return
}

func createSchema(ctx context.Context, db *sql.DB) error {
	rawSQL, err := embedFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("schema creation failed with %w", err)
	}

	_, err = db.ExecContext(ctx, string(rawSQL))
	if err != nil {
		return fmt.Errorf("schema creation failed with %w", err)
	}

	return nil
}

func migrateSchema(ctx context.Context, db *sql.DB, current, target int32) error {
	if current == target {
		return nil
	}
	return createSchema(ctx, db)
}
