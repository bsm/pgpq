package pgpq

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrDuplicateID occurs when a task with the same ID already exists.
	ErrDuplicateID = errors.New("duplicate ID")
	// ErrNoTasks is returned when there are no tasks in the queue.
	ErrNoTasks = errors.New("no tasks in the queue")
)

// ----------------------------------------------------------------------------

type listOptions struct {
	Offset int64
	Limit  int64
}

func (o *listOptions) GetLimit() int64 {
	if o.Limit == 0 {
		return 100
	}
	return o.Limit
}

func (o *listOptions) Set(opts ...ListOption) {
	for _, opt := range opts {
		opt.applyOption(o)
	}
}

// ListOption can be applied when listing tasks.
type ListOption interface {
	applyOption(*listOptions)
}

type listOptionFunc func(*listOptions)

func (f listOptionFunc) applyOption(o *listOptions) { f(o) }

// WithOffset applies an offset to the list.
func WithOffset(v int64) ListOption {
	return listOptionFunc(func(o *listOptions) { o.Offset = v })
}

// WithLimit applies a limit to the list. Default: 100.
func WithLimit(v int64) ListOption {
	return listOptionFunc(func(o *listOptions) { o.Limit = v })
}

// ----------------------------------------------------------------------------

// Task contains the task definition.
type Task struct {
	ID       uuid.UUID
	Priority int16
	Payload  json.RawMessage
}

// TaskDetails contains detailed task information.
type TaskDetails struct {
	Task
	CreatedAt time.Time
	UpdatedAt time.Time
	Attempts  int // number of previous claims
}

func (td *TaskDetails) scan(rows interface{ Scan(...interface{}) error }) error {
	return rows.Scan(
		&td.ID,
		&td.Priority,
		&td.Payload,
		&td.CreatedAt,
		&td.UpdatedAt,
		&td.Attempts,
	)
}
