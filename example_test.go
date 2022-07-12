package pgpq_test

import (
	"context"
	"fmt"
	"os"

	"github.com/bsm/pgpq"
	"github.com/google/uuid"
)

func Example() {
	ctx := context.Background()
	url := "postgres://localhost/pgpq_test?sslmode=disable" // `?sslmode=verify-ca` recommended for production
	if v := os.Getenv("DATABASE_URL"); v != "" {
		url = v
	}

	// connect to postgres
	client, err := pgpq.Connect(ctx, url)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// truncate the queue, for testing only
	if err := client.Truncate(ctx); err != nil {
		panic(err)
	}

	// push three tasks into the queue
	if err := client.Push(ctx, &pgpq.Task{
		Priority: 3,
		Payload:  []byte(`{"foo":1}`),
	}); err != nil {
		panic(err)
	}
	if err := client.Push(ctx, &pgpq.Task{
		ID:       uuid.MustParse("28667ce4-1999-4af4-9ff2-1757b3844048"), // custom UUID
		Priority: 4,
		Payload:  []byte(`{"bar":2}`),
	}); err != nil {
		panic(err)
	}
	if err := client.Push(ctx, &pgpq.Task{
		Payload: []byte(`{"baz":3}`),
	}); err != nil {
		panic(err)
	}

	// shift the task with the highest priority from the queue
	claim, err := client.Shift(ctx)
	if err != nil {
		panic(err)
	}
	defer claim.Release(ctx)

	// print ID and payload
	fmt.Println(claim.ID.String())
	fmt.Println(string(claim.Payload))

	// mark task done and remove from the queue
	if err := claim.Done(ctx); err != nil {
		panic(err)
	}

	// Output:
	// 28667ce4-1999-4af4-9ff2-1757b3844048
	// {"bar": 2}
}
