package pgpq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Client implements a queue client.
type Client struct {
	db   *sql.DB
	stmt struct {
		push, pushWithID, get, claim, shift, list, update, done *sql.Stmt
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
func (c *Client) Truncate(ctx context.Context, opts ...ScopeOption) error {
	opt := new(scopeOptions)
	opt.Set(opts...)
	if err := opt.Namespace.validate(); err != nil {
		return err
	}

	_, err := c.db.ExecContext(ctx, `DELETE FROM pgpq_tasks WHERE namespace = $1`, opt.Namespace)
	return err
}

// Len returns the queue length. This includes pending and running tasks.
func (c *Client) Len(ctx context.Context, opts ...ScopeOption) (int64, error) {
	var cnt int64

	opt := new(scopeOptions)
	opt.Set(opts...)
	if err := opt.Namespace.validate(); err != nil {
		return cnt, err
	}

	if err := c.db.
		QueryRowContext(ctx, `SELECT COUNT(*) FROM pgpq_tasks WHERE namespace = $1`, opt.Namespace).
		Scan(&cnt); err != nil {
		return cnt, err
	}
	return cnt, nil
}

// MinCreatedAt returns created timestamp of the oldest task in the queue. It
// may return ErrNoTask.
func (c *Client) MinCreatedAt(ctx context.Context, opts ...ScopeOption) (time.Time, error) {
	var ts pq.NullTime

	opt := new(scopeOptions)
	opt.Set(opts...)
	if err := opt.Namespace.validate(); err != nil {
		return ts.Time, err
	}

	if err := c.db.
		QueryRowContext(ctx, `SELECT MIN(created_at) FROM pgpq_tasks WHERE namespace = $1`, opt.Namespace).
		Scan(&ts); err != nil {
		return ts.Time, err
	} else if !ts.Valid {
		return ts.Time, ErrNoTask
	}
	return ts.Time, nil
}

// Push pushes a task into the queue. It may return ErrDuplicateID.
func (c *Client) Push(ctx context.Context, task *Task) error {
	if err := task.validate(); err != nil {
		return err
	}

	if len(task.Payload) == 0 {
		task.Payload = json.RawMessage{'{', '}'}
	}

	var row *sql.Row
	if task.ID == uuid.Nil {
		row = c.stmt.push.QueryRowContext(ctx, task.Namespace, task.Priority, task.Payload)
	} else {
		row = c.stmt.pushWithID.QueryRowContext(ctx, task.ID, task.Namespace, task.Priority, task.Payload)
	}

	if err := row.Scan(&task.ID); err != nil {
		var dbErr *pq.Error
		if errors.As(err, &dbErr) && dbErr.Code == "23505" && dbErr.Constraint == "pgpq_tasks_pkey" {
			return ErrDuplicateID
		}
		return err
	}
	return nil
}

// Get returns a task by ID. It may return ErrNoTask.
func (c *Client) Get(ctx context.Context, id uuid.UUID) (*TaskDetails, error) {
	td := new(TaskDetails)
	row := c.stmt.get.QueryRowContext(ctx, id)
	if err := td.scan(row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoTask
		}
		return nil, err
	}
	return td, nil
}

// Claim locks and returns the task with the given ID. It may return ErrNoTask.
func (c *Client) Claim(ctx context.Context, id uuid.UUID) (*Claim, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	claim := &Claim{tx: tx, update: c.stmt.update, done: c.stmt.done}
	row := tx.
		StmtContext(ctx, c.stmt.claim).
		QueryRowContext(ctx, id)
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoTask
		}
		return nil, err
	}
	return claim, nil
}

// Shift locks and returns the task with the highest priority. It may return
// ErrNoTask.
func (c *Client) Shift(ctx context.Context, opts ...ScopeOption) (*Claim, error) {
	opt := new(scopeOptions)
	opt.Set(opts...)
	if err := opt.Namespace.validate(); err != nil {
		return nil, err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	claim := &Claim{tx: tx, update: c.stmt.update, done: c.stmt.done}
	row := tx.
		StmtContext(ctx, c.stmt.shift).
		QueryRowContext(ctx, opt.Namespace)
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoTask
		}
		return nil, err
	}
	return claim, nil
}

// List lists all tasks in the queue.
func (c *Client) List(ctx context.Context, opts ...ListOption) ([]*TaskDetails, error) {
	opt := new(listOptions)
	opt.Set(opts...)
	if err := opt.Namespace.validate(); err != nil {
		return nil, err
	}
	limit := opt.GetLimit()

	rows, err := c.stmt.list.QueryContext(ctx, opt.Namespace, limit, opt.Offset)
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
		c.stmt.claim,
		c.stmt.shift,
		c.stmt.list,
		c.stmt.update,
		c.stmt.done,
	} {
		if stmt != nil {
			if e := stmt.Close(); e != nil {
				err = e
			}
		}
	}

	if c.ownDB {
		if e := c.db.Close(); e != nil {
			err = e
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
	} else if c.stmt.claim, err = c.db.PrepareContext(ctx, stmtClaim); err != nil {
		return
	} else if c.stmt.shift, err = c.db.PrepareContext(ctx, stmtShift); err != nil {
		return
	} else if c.stmt.list, err = c.db.PrepareContext(ctx, stmtList); err != nil {
		return
	} else if c.stmt.update, err = c.db.PrepareContext(ctx, stmtUpdate); err != nil {
		return
	} else if c.stmt.done, err = c.db.PrepareContext(ctx, stmtDone); err != nil {
		return
	}
	return
}
