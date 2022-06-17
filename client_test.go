package pgpq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	. "github.com/bsm/pgpq"
	"github.com/google/uuid"
)

func TestClient(t *testing.T) {
	ctx := context.Background()
	if err := client.Truncate(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	task1 := &Task{
		ID:       mockUUID,
		Priority: 3,
		Payload:  []byte(`{"foo":1}`),
	}
	task2 := &Task{
		Priority: 2,
		Payload:  []byte(`{"bar":2}`),
	}

	if err := client.Push(ctx, task1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := client.Push(ctx, task2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	t.Run("Push", func(t *testing.T) {
		if exp, got := mockUUID, task1.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		if got := task2.ID; got == uuid.Nil {
			t.Errorf("did not expect %v", got)
		} else if exp, got := 4, int(task2.ID.Version()); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("Push duplicate", func(t *testing.T) {
		if err := client.Push(ctx, task1); !errors.Is(err, ErrDuplicateID) {
			t.Errorf("expected %v, got %v", ErrDuplicateID, err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		td1, err := client.Get(ctx, task1.ID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := task1.ID, td1.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		} else if td1.CreatedAt.IsZero() {
			t.Errorf("expected to be set")
		} else if td1.UpdatedAt.IsZero() {
			t.Errorf("expected to be set")
		}

		if _, err := client.Get(ctx, uuid.New()); !errors.Is(err, ErrNoTask) {
			t.Errorf("expected %v, got %v", ErrNoTask, err)
		}
	})

	t.Run("List", func(t *testing.T) {
		tasks, err := client.List(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 2, len(tasks); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		normTaskDetails(tasks...)
		assertEqual(t, tasks, []*TaskDetails{
			{
				Task:      Task{ID: task1.ID, Priority: 3, Payload: json.RawMessage(`{"foo":1}`)},
				CreatedAt: mockNow,
				UpdatedAt: mockNow,
			},
			{
				Task:      Task{ID: task2.ID, Priority: 2, Payload: json.RawMessage(`{"bar":2}`)},
				CreatedAt: mockNow,
				UpdatedAt: mockNow,
			},
		})
	})

	t.Run("Shift", func(t *testing.T) {
		// shift task #1
		claim1, err := client.Shift(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer claim1.Release(ctx)

		// check claim
		if exp, got := task1.ID, claim1.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// shift task #2
		claim2, err := client.Shift(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer claim2.Release(ctx)

		// check claim
		if exp, got := task2.ID, claim2.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// no more tasks left
		if _, err := client.Shift(ctx); !errors.Is(err, ErrNoTask) {
			t.Errorf("expected %v, got %v", ErrNoTask, err)
		}

		// update claim
		claim2.Payload = []byte(`{"baz":3}`)
		claim2.Priority = 9
		if err := claim2.Update(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// shift task #2 again
		claim3, err := client.Shift(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer claim3.Release(ctx)

		// check claim
		if exp, got := task2.ID, claim3.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		} else if exp, got := int16(9), claim3.Priority; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		} else if exp, got := `{"baz": 3}`, string(claim3.Payload); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// no more tasks left
		if _, err := client.Shift(ctx); !errors.Is(err, ErrNoTask) {
			t.Errorf("expected %v, got %v", ErrNoTask, err)
		}
	})

	t.Run("Shift then Done", func(t *testing.T) {
		// shift a task
		claim, err := client.Shift(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer claim.Release(ctx)

		if tasks, err := client.List(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 2, len(tasks); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		if err := claim.Done(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if tasks, err := client.List(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 1, len(tasks); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}
