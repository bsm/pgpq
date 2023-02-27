package pgpq_test

import (
	"bytes"
	"context"
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

var client *Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	url := "postgres://localhost/pgpq_test?sslmode=disable&timezone=UTC"
	if v := os.Getenv("DATABASE_URL"); v != "" {
		url = v
	}

	var err error
	client, err = Connect(ctx, url)
	if err != nil {
		panic(err)
	}
	client.SetCurrentTime(mockNow)

	code := m.Run()
	if err := client.Close(); err != nil {
		panic(err)
	}

	os.Exit(code)
}

func Test_schemaVersion(t *testing.T) {
	version, err := client.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp, got := "4", version; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func assertEqual(t *testing.T, got, exp interface{}) {
	t.Helper()

	expj, _ := json.Marshal(exp)
	gotj, _ := json.Marshal(got)
	if !bytes.Equal(expj, gotj) {
		t.Fatalf("\nexpected: %s,\n     got: %s", expj, gotj)
	}
}

func truncate(ctx context.Context, t *testing.T) {
	if err := client.Truncate(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := client.Truncate(ctx, WithNamespace("baz")); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func seedTriple(ctx context.Context, t *testing.T) (*Task, *Task, *Task) {
	t.Helper()

	truncate(ctx, t)

	task1 := &Task{
		ID:       mockUUID,
		Priority: 3,
		Payload:  []byte(`{"foo":1}`),
	}
	task2 := &Task{
		Priority: 2,
		Payload:  []byte(`{"bar":2}`),
	}
	task3 := &Task{
		Namespace: "baz",
	}

	if err := client.Push(ctx, task1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := client.Push(ctx, task2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := client.Push(ctx, task3); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	return task1, task2, task3
}

func seedDelayed(ctx context.Context, t *testing.T, notBefore time.Time) *Task {
	t.Helper()

	task := &Task{
		NotBefore: notBefore,
	}

	if err := client.Push(ctx, task); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	return task
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

func timeTravel(now time.Time, cb func()) {
	client.SetCurrentTime(now)
	defer client.SetCurrentTime(mockNow) // revert to normal in the end

	cb()
}
