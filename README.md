# PGPQ

[![Go Reference](https://pkg.go.dev/badge/github.com/bsm/pgpq.svg)](https://pkg.go.dev/github.com/bsm/pgpq)
[![Test](https://github.com/bsm/pgpq/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/pgpq/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Priority queues with Postgres, implemented in [Go](https://golang.org).

## Example:

```go
import (
	"context"
	"fmt"
	"os"

	"github.com/bsm/pgpq"
	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()

	// connection URL:
	// - use `sslmode=verify-ca` for production
	// - use `default_query_exec_mode=simple_protocol` in combination with proxies
	url := "postgres://localhost/pgpq_test?sslmode=disable"

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

	// push some tasks into the queue
	if err := client.Push(ctx, &pgpq.Task{
		Priority:	3,
		Payload:	[]byte(`{"foo":1}`),
	}); err != nil {
		panic(err)
	}
	if err := client.Push(ctx, &pgpq.Task{
		ID:		uuid.MustParse("28667ce4-1999-4af4-9ff2-1757b3844048"),	// custom UUID
		Priority:	4,
		Payload:	[]byte(`{"bar":2}`),
	}); err != nil {
		panic(err)
	}
	if err := client.Push(ctx, &pgpq.Task{
		Payload: []byte(`{"baz":3}`),
	}); err != nil {
		panic(err)
	}
	if err := client.Push(ctx, &pgpq.Task{
		Payload:	[]byte(`{"baz":4}`),
		NotBefore:	time.Now().Add(time.Minute),	// delay this task for 1m
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

}
```
