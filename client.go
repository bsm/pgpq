package pgpq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // support pgx connections
)

// Client implements a queue client.
type Client struct {
	db    *sql.DB
	opt   *scopeOptions
	ownDB bool
	clock clock.Clock
}

// Connect connects to a PG instance using a URL.
// Example:
//
//	postgres://user:secret@test.host:5432/mydb?sslmode=verify-ca
func Connect(ctx context.Context, url string, opts ...ScopeOption) (*Client, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}

	client, err := Wrap(ctx, db, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	client.ownDB = true
	return client, nil
}

// Wrap wraps an existing database/sql.DB instance. Please note that calling
// Close() will not close the underlying connection.
func Wrap(ctx context.Context, db *sql.DB, opts ...ScopeOption) (*Client, error) {
	opt := &scopeOptions{}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return nil, err
	}

	if err := validateConn(ctx, db); err != nil {
		return nil, err
	}

	c := &Client{db: db, opt: opt, clock: clock.New()}
	return c, nil
}

// Truncate truncates the queue and deletes all tasks. Intended for testing,
// please use with care.
func (c *Client) Truncate(ctx context.Context, opts ...ScopeOption) error {
	opt := &scopeOptions{Namespace: c.opt.Namespace}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return err
	}

	_, err := c.db.ExecContext(ctx, `DELETE FROM pgpq_tasks WHERE namespace = $1`, opt.Namespace)
	return err
}

// Len returns the queue length. This counts all the non-delayed tasks.
func (c *Client) Len(ctx context.Context, opts ...ScopeOption) (int64, error) {
	var cnt int64

	opt := &scopeOptions{Namespace: c.opt.Namespace}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return cnt, err
	}

	if err := c.db.
		QueryRowContext(ctx, `SELECT COUNT(*) FROM pgpq_tasks WHERE namespace = $1 AND not_before <= $2`,
			opt.Namespace,
			c.clock.Now(),
		).
		Scan(&cnt); err != nil {
		return cnt, err
	}
	return cnt, nil
}

// MinCreatedAt returns created timestamp of the oldest non-delayed task in the queue.
// It may return ErrNoTask.
func (c *Client) MinCreatedAt(ctx context.Context, opts ...ScopeOption) (time.Time, error) {
	var ts sql.NullTime

	opt := &scopeOptions{Namespace: c.opt.Namespace}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return ts.Time, err
	}

	if err := c.db.
		QueryRowContext(ctx, `SELECT MIN(created_at) FROM pgpq_tasks WHERE namespace = $1 AND not_before <= $2`,
			opt.Namespace,
			c.clock.Now(),
		).
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

	if task.Namespace == "" && c.opt.Namespace != "" {
		task.Namespace = string(c.opt.Namespace)
	}
	if len(task.Payload) == 0 {
		task.Payload = json.RawMessage{'{', '}'}
	}

	now := c.clock.Now()

	var row *sql.Row
	if task.ID == uuid.Nil {
		row = c.db.QueryRowContext(ctx, stmtPush, task.Namespace, task.Priority, unsafeString(task.Payload), coalesceTime(task.NotBefore, unixZero), now, now)
	} else {
		row = c.db.QueryRowContext(ctx, stmtPushWithID, task.ID, task.Namespace, task.Priority, unsafeString(task.Payload), coalesceTime(task.NotBefore, unixZero), now, now)
	}

	if err := row.Scan(&task.ID); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "pgpq_tasks_pkey" {
			return ErrDuplicateID
		}
		return err
	}
	return nil
}

// Get returns a task by ID. It may return ErrNoTask.
func (c *Client) Get(ctx context.Context, id uuid.UUID) (*TaskDetails, error) {
	td := new(TaskDetails)
	row := c.db.QueryRowContext(ctx, stmtGet, id)
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

	claim := c.newClaim(tx)
	row := tx.QueryRowContext(ctx, stmtClaim, id)
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoTask
		}
		return nil, err
	}
	return claim, nil
}

// Shift locks and returns the non-delayed task with the highest priority.
// It may return ErrNoTask.
func (c *Client) Shift(ctx context.Context, opts ...ScopeOption) (*Claim, error) {
	opt := &scopeOptions{Namespace: c.opt.Namespace}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return nil, err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	claim := c.newClaim(tx)
	row := tx.
		QueryRowContext(ctx, stmtShift, opt.Namespace, c.clock.Now())
	if err := claim.TaskDetails.scan(row); err != nil {
		_ = tx.Rollback()

		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoTask
		}
		return nil, err
	}
	return claim, nil
}

// List lists all tasks (incl. delayed) in the queue.
func (c *Client) List(ctx context.Context, opts ...ListOption) ([]*TaskDetails, error) {
	opt := &listOptions{Namespace: c.opt.Namespace}
	opt.set(opts...)
	if err := opt.validate(); err != nil {
		return nil, err
	}
	limit := opt.getLimit()

	rows, err := c.db.QueryContext(ctx, stmtList, opt.Namespace, limit, opt.Offset)
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
	if c.ownDB {
		if e := c.db.Close(); e != nil {
			err = e
		}
	}

	return err
}

func (c *Client) newClaim(tx *sql.Tx) *Claim {
	return &Claim{
		tx:    tx,
		clock: c.clock,
	}
}
