package pgpq

import (
	"context"
	"database/sql"
	"errors"

	"github.com/bsm/minisql"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Client implements a queue client.
type Client struct {
	db    *sql.DB
	ownDB bool
}

// Connect connects to a PG instance using a URL.
// Example:
//   postgres://user:secret@test.host:5432/mydb?sslmode=verify-ca
func Connect(ctx context.Context, url string) (*Client, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	client, err := Wrap(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	client.ownDB = true
	return client, nil
}

// Wrap wraps a database/sql.DB instance. Please note that calling Close() will not close
// the underlying connection.
func Wrap(ctx context.Context, db *sql.DB) (*Client, error) {
	if err := validateConn(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Client{db: db}, nil
}

// Push pushes a task into the queue. It may return ErrDuplicateID.
func (c *Client) Push(ctx context.Context, task *Task) error {
	query := minisql.Pooled().UsePlaceholder(minisql.Dollar)
	defer minisql.Release(query)

	query.AppendString(`INSERT INTO tasks (`)
	if task.ID != uuid.Nil {
		query.AppendString("id,")
	}
	query.AppendString("priority,payload) VALUES (")

	if task.ID != uuid.Nil {
		query.AppendValue(task.ID)
		query.AppendByte(',')
	}
	query.AppendValue(task.Priority)
	query.AppendByte(',')
	query.AppendValue(task.Payload)
	query.AppendString(") RETURNING id")

	if err := query.QueryRowContext(ctx, c.db).Scan(&task.ID); err != nil {
		var dbErr *pq.Error
		if errors.As(err, &dbErr) && dbErr.Code == "23505" && dbErr.Constraint == "tasks_pkey" {
			return ErrDuplicateID
		}
		return err
	}
	return nil
}

// Shift locks and returns the task with the highest priority. It may return
// ErrNoTasks.
func (c *Client) Shift(ctx context.Context) (*Claim, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	claim := &Claim{tx: tx}
	row := tx.QueryRowContext(ctx, `
		SELECT
			id,
			priority,
			payload,
			created_at,
			updated_at,
			attempts
		FROM tasks
		ORDER BY
			priority DESC,
			updated_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`)
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if err == sql.ErrNoRows {
			return nil, ErrNoTasks
		}
		return nil, err
	}
	return claim, nil
}

// List lists all tasks in the queue.
func (c *Client) List(ctx context.Context, opts ...ListOption) ([]*TaskDetails, error) {
	query := minisql.Pooled().UsePlaceholder(minisql.Dollar)
	defer minisql.Release(query)

	opt := new(listOptions)
	opt.Set(opts...)
	limit := opt.GetLimit()

	query.AppendString(`
		SELECT
			id,
			priority,
			payload,
			created_at,
			updated_at,
			attempts
		FROM tasks
		ORDER BY
			priority DESC,
			updated_at ASC
		LIMIT `)
	query.AppendValue(limit)
	query.AppendString(` OFFSET `)
	query.AppendValue(opt.Offset)

	rows, err := query.QueryContext(ctx, c.db)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := make([]*TaskDetails, 0, limit)
	for rows.Next() {
		task := new(TaskDetails)
		if err := task.scan(rows); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.ownDB {
		return c.db.Close()
	}
	return nil
}
