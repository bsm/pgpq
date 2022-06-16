package pgpq_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	. "github.com/bsm/pgpq"
	"github.com/google/uuid"
)

var (
	mockUUID = uuid.MustParse("28667ce4-1999-4af4-9ff2-1757b3844048")
	mockNow  = time.Now().UTC().Truncate(24 * time.Hour)
)

var (
	testDB *testDBConn
	client *Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	url := "postgres://localhost/pgpq_test"
	if v := os.Getenv("DATABASE_URL"); v != "" {
		url = v
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		panic(err)
	}
	testDB = &testDBConn{DB: db}

	client, err = Wrap(ctx, db)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	if err := client.Close(); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	os.Exit(code)
}

func assertEqual(t *testing.T, got, exp interface{}) {
	t.Helper()

	expj, _ := json.Marshal(exp)
	gotj, _ := json.Marshal(got)
	if !bytes.Equal(expj, gotj) {
		t.Fatalf("\nexpected: %s,\n     got: %s", expj, gotj)
	}
}

type testDBConn struct {
	*sql.DB
}

func (db *testDBConn) Truncate(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `TRUNCATE TABLE tasks`)
	return err
}

func normTaskDetails(tds ...*TaskDetails) {
	for _, td := range tds {
		td.CreatedAt = td.CreatedAt.UTC()
		td.UpdatedAt = td.UpdatedAt.UTC()

		if !td.CreatedAt.IsZero() {
			td.CreatedAt = td.CreatedAt.Truncate(24 * time.Hour)
		}
		if !td.UpdatedAt.IsZero() {
			td.UpdatedAt = td.UpdatedAt.Truncate(24 * time.Hour)
		}
	}
}
