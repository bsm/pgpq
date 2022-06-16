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
	url := "postgres://localhost/pgpq_test"
	if v := os.Getenv("DATABASE_URL"); v != "" {
		url = v
	}

	// connect to postgres
	client, err := pgpq.Connect(ctx, url)
	if err != nil {
		panic(err)
	}
	defer client.Close()

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

	// acquire a claim to the first item in the queue
	claim, err := client.Shift(ctx)
	if err != nil {
		panic(err)
	}
	defer claim.NAck(ctx)

	// print ID and payload
	fmt.Println(claim.ID.String())
	fmt.Println(string(claim.Payload))

	// ack the claim, remove task from the queue
	if err := claim.Ack(ctx); err != nil {
		panic(err)
	}

	// Output:
	// 28667ce4-1999-4af4-9ff2-1757b3844048
	// {"bar": 2}
}
