package pgpq_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	. "github.com/bsm/pgpq"
	"github.com/google/uuid"
)

func TestClient_Push(t *testing.T) {
	ctx := context.Background()
	task1, task2, _ := seedTriple(ctx, t)

	if exp, got := mockUUID, task1.ID; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if got := task2.ID; got == uuid.Nil {
		t.Errorf("did not expect %v", got)
	} else if exp, got := 4, int(task2.ID.Version()); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func TestClient_Push_duplicate(t *testing.T) {
	ctx := context.Background()
	task1, _, _ := seedTriple(ctx, t)

	if err := client.Push(ctx, task1); !errors.Is(err, ErrDuplicateID) {
		t.Errorf("expected %v, got %v", ErrDuplicateID, err)
	}
}

func TestClient_Get(t *testing.T) {
	ctx := context.Background()
	task1, _, _ := seedTriple(ctx, t)

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
}

func TestClient_List(t *testing.T) {
	ctx := context.Background()
	task1, task2, task3 := seedTriple(ctx, t)

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

	// with namespace
	tasks, err = client.List(ctx, WithNamespace("baz"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp, got := 1, len(tasks); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	normTaskDetails(tasks...)
	assertEqual(t, tasks, []*TaskDetails{
		{
			Task:      Task{ID: task3.ID, Namespace: "baz", Payload: json.RawMessage(`{}`)},
			CreatedAt: mockNow,
			UpdatedAt: mockNow,
		},
	})

	// bad namespace
	if _, err := client.List(ctx, WithNamespace("日本国")); err == nil || err.Error() != `namespace "日本国" contains non-ASCII characters` {
		t.Errorf("expected error, got %v", err)
	}
}

func TestClient_Claim(t *testing.T) {
	ctx := context.Background()
	_, task2, _ := seedTriple(ctx, t)

	// shift task #2
	claim1, err := client.Claim(ctx, task2.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer claim1.Release(ctx)

	// check claim
	if exp, got := task2.ID, claim1.ID; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	// try again
	if _, err := client.Claim(ctx, task2.ID); !errors.Is(err, ErrNoTask) {
		t.Errorf("expected %v, got %v", ErrNoTask, err)
	}

	// try non-existent
	if _, err := client.Claim(ctx, uuid.New()); !errors.Is(err, ErrNoTask) {
		t.Errorf("expected %v, got %v", ErrNoTask, err)
	}
}

func TestClient_Shift(t *testing.T) {
	ctx := context.Background()
	task1, task2, task3 := seedTriple(ctx, t)

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
	claim2, err = client.Shift(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer claim2.Release(ctx)

	// check claim
	if exp, got := task2.ID, claim2.ID; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	} else if exp, got := int16(9), claim2.Priority; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	} else if exp, got := `{"baz": 3}`, string(claim2.Payload); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	// no more tasks left
	if _, err := client.Shift(ctx); !errors.Is(err, ErrNoTask) {
		t.Errorf("expected %v, got %v", ErrNoTask, err)
	}

	// with namespace
	claim3, err := client.Shift(ctx, WithNamespace("baz"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer claim3.Release(ctx)

	if exp, got := task3.ID, claim3.ID; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	// bad namespace
	if _, err := client.Shift(ctx, WithNamespace("日本国")); err == nil || err.Error() != `namespace "日本国" contains non-ASCII characters` {
		t.Errorf("expected error, got %v", err)
	}
}

func TestClient_Shift_thenDone(t *testing.T) {
	ctx := context.Background()
	seedTriple(ctx, t)

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
}

func TestClient_Len(t *testing.T) {
	ctx := context.Background()
	seedTriple(ctx, t)

	if got, err := client.Len(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := int64(2); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if got, err := client.Len(ctx, WithNamespace("baz")); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := int64(1); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if _, err := client.Len(ctx, WithNamespace("日本国")); err == nil || err.Error() != `namespace "日本国" contains non-ASCII characters` {
		t.Errorf("expected error, got %v", err)
	}

	if err := doTask(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got, err := client.Len(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := int64(1); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if err := doTask(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got, err := client.Len(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := int64(0); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func TestClient_MinCreatedAt(t *testing.T) {
	ctx := context.Background()
	task1, task2, task3 := seedTriple(ctx, t)

	td1, err := client.Get(ctx, task1.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	td2, err := client.Get(ctx, task2.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	td3, err := client.Get(ctx, task3.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got, err := client.MinCreatedAt(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := td1.CreatedAt; !exp.Equal(got) {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if got, err := client.MinCreatedAt(ctx, WithNamespace("baz")); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := td3.CreatedAt; !exp.Equal(got) {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if _, err := client.MinCreatedAt(ctx, WithNamespace("日本国")); err == nil || err.Error() != `namespace "日本国" contains non-ASCII characters` {
		t.Errorf("expected error, got %v", err)
	}

	if err := doTask(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if got, err := client.MinCreatedAt(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if exp := td2.CreatedAt; !exp.Equal(got) {
		t.Errorf("expected %v, got %v", exp, got)
	}

	if err := doTask(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := client.MinCreatedAt(ctx); !errors.Is(err, ErrNoTask) {
		t.Errorf("expected %v, got %v", ErrNoTask, err)
	}
}

func TestClient_Stats(t *testing.T) {
	ctx := context.Background()
	task1, _, task3 := seedTriple(ctx, t)

	td1, err := client.Get(ctx, task1.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	td3, err := client.Get(ctx, task3.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	exp := []*Stat{
		{Namespace: "", Len: 2, MinCreatedAt: td1.CreatedAt},
		{Namespace: "baz", Len: 1, MinCreatedAt: td3.CreatedAt},
	}
	if got, err := client.Stats(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	} else if !reflect.DeepEqual(exp, got) {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func doTask(ctx context.Context) error {
	claim, err := client.Shift(ctx)
	if err != nil {
		return err
	}
	defer claim.Release(ctx)

	return claim.Done(ctx)
}
