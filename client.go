package pgpq

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Client implements a queue client.
type Client struct {
	db   *sql.DB
	stmt struct {
		push, pushWithID, get, shift, list, update, remove *sql.Stmt
	}
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

// Wrap wraps an existing database/sql.DB instance. Please note that calling
// Close() will not close the underlying connection.
func Wrap(ctx context.Context, db *sql.DB) (*Client, error) {
	if err := validateConn(ctx, db); err != nil {
		return nil, err
	}

	c := &Client{db: db}
	if err := c.prepareStmt(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// Truncate truncates the queue and deletes all tasks. Intended for testing,
// please use with care.
func (c *Client) Truncate(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `TRUNCATE TABLE tasks`)
	return err
}

// Push pushes a task into the queue. It may return ErrDuplicateID.
func (c *Client) Push(ctx context.Context, task *Task) error {
	var row *sql.Row
	if task.ID == uuid.Nil {
		row = c.stmt.push.QueryRowContext(ctx, task.Priority, task.Payload)
	} else {
		row = c.stmt.pushWithID.QueryRowContext(ctx, task.ID, task.Priority, task.Payload)
	}

	if err := row.Scan(&task.ID); err != nil {
		var dbErr *pq.Error
		if errors.As(err, &dbErr) && dbErr.Code == "23505" && dbErr.Constraint == "tasks_pkey" {
			return ErrDuplicateID
		}
		return err
	}
	return nil
}

// Get returns a task by ID. It may return ErrNotFound.
func (c *Client) Get(ctx context.Context, id uuid.UUID) (*TaskDetails, error) {
	td := new(TaskDetails)
	row := c.stmt.get.QueryRowContext(ctx, id)
	if err := td.scan(row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return td, nil
}

// Shift locks and returns the task with the highest priority. It may return
// ErrNotFound.
func (c *Client) Shift(ctx context.Context) (*Claim, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	claim := &Claim{tx: tx, update: c.stmt.update, remove: c.stmt.remove}
	row := tx.
		StmtContext(ctx, c.stmt.shift).
		QueryRowContext(ctx)
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return claim, nil
}

// List lists all tasks in the queue.
func (c *Client) List(ctx context.Context, opts ...ListOption) ([]*TaskDetails, error) {
	opt := new(listOptions)
	opt.Set(opts...)
	limit := opt.GetLimit()

	rows, err := c.stmt.list.QueryContext(ctx, limit, opt.Offset)
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
	var err error

	for _, stmt := range []*sql.Stmt{
		c.stmt.push,
		c.stmt.pushWithID,
		c.stmt.get,
		c.stmt.shift,
		c.stmt.list,
		c.stmt.update,
		c.stmt.remove,
	} {
		if stmt != nil {
			if e := stmt.Close(); e != nil {
				err = e
			}
		}
	}

	if c.ownDB {
		if e := c.db.Close(); e != nil {
			err = c.db.Close()
		}
	}

	return err
}

func (c *Client) prepareStmt(ctx context.Context) (err error) {
	if c.stmt.push, err = c.db.PrepareContext(ctx, stmtPush); err != nil {
		return
	} else if c.stmt.pushWithID, err = c.db.PrepareContext(ctx, stmtPushWithID); err != nil {
		return
	} else if c.stmt.get, err = c.db.PrepareContext(ctx, stmtGet); err != nil {
		return
	} else if c.stmt.shift, err = c.db.PrepareContext(ctx, stmtShift); err != nil {
		return
	} else if c.stmt.list, err = c.db.PrepareContext(ctx, stmtList); err != nil {
		return
	} else if c.stmt.update, err = c.db.PrepareContext(ctx, stmtUpdate); err != nil {
		return
	} else if c.stmt.remove, err = c.db.PrepareContext(ctx, stmtRemove); err != nil {
		return
	}
	return
}
