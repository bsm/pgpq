package pgpq_test

import (
	"context"
	"encoding/json"
	"testing"

	. "github.com/bsm/pgpq"
	"github.com/google/uuid"
)

func TestClient(t *testing.T) {
	ctx := context.Background()
	if err := testDB.Truncate(ctx); err != nil {
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
			t.Fatalf("expected %v, got %v", exp, got)
		}
		if got := task2.ID; got == uuid.Nil {
			t.Fatalf("did not expect %v", got)
		} else if exp, got := 4, int(task2.ID.Version()); exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}
	})

	t.Run("Push duplicate", func(t *testing.T) {
		if err := client.Push(ctx, task1); err != ErrDuplicateID {
			t.Fatalf("expected %v, got %v", ErrDuplicateID, err)
		}
	})

	t.Run("List", func(t *testing.T) {
		tasks, err := client.List(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 2, len(tasks); exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
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
		defer claim1.NAck(ctx)

		// check claim
		if exp, got := task1.ID, claim1.ID; exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}

		// shift task #2
		claim2, err := client.Shift(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		defer claim2.NAck(ctx)

		// check claim
		if exp, got := task2.ID, claim2.ID; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// no more tasks left
		_, err = client.Shift(ctx)
		if exp := ErrNoTasks; err != exp {
			t.Fatalf("expected %v, got %v", exp, err)
		}

		// NACK claim
		if err := claim2.NAck(ctx); err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// shift task #2 again
		claim3, err := client.Shift(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		defer claim3.NAck(ctx)

		// check claim
		if exp, got := task2.ID, claim3.ID; exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}
		if exp, got := 1, claim3.Attempts; exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}

		// no more tasks left
		_, err = client.Shift(ctx)
		if exp := ErrNoTasks; err != exp {
			t.Fatalf("expected %v, got %v", exp, err)
		}
	})

	t.Run("Shift then Ack", func(t *testing.T) {
		// shift a task
		claim, err := client.Shift(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer claim.NAck(ctx)

		if tasks, err := client.List(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 2, len(tasks); exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}

		if err := claim.Ack(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if tasks, err := client.List(ctx); err != nil {
			t.Fatalf("expected no error, got %v", err)
		} else if exp, got := 1, len(tasks); exp != got {
			t.Fatalf("expected %v, got %v", exp, got)
		}
	})
}
