package pgpq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrDuplicateID occurs when a task with the same ID already exists.
	ErrDuplicateID = errors.New("duplicate ID")
	// ErrNoTask is returned when tasks cannot be found.
	ErrNoTask = errors.New("no task")
)

// ----------------------------------------------------------------------------

type listOptions struct {
	Offset    int64
	Limit     int64
	Namespace namespace
}

func (o *listOptions) getLimit() int64 {
	if o.Limit == 0 {
		return 100
	}
	return o.Limit
}

func (o *listOptions) set(opts ...ListOption) {
	for _, opt := range opts {
		opt.applyListOption(o)
	}
}

func (o *listOptions) validate() error {
	return o.Namespace.validate()
}

// ListOption can be applied when listing tasks.
type ListOption interface {
	applyListOption(*listOptions)
}

type listOptionFunc func(*listOptions)

func (f listOptionFunc) applyListOption(o *listOptions) { f(o) }

// WithOffset applies an offset to the list.
func WithOffset(v int64) ListOption {
	return listOptionFunc(func(o *listOptions) { o.Offset = v })
}

// WithLimit applies a limit to the list. Default: 100.
func WithLimit(v int64) ListOption {
	return listOptionFunc(func(o *listOptions) { o.Limit = v })
}

// ----------------------------------------------------------------------------

type scopeOptions struct {
	Namespace namespace
}

func (o *scopeOptions) set(opts ...ScopeOption) {
	for _, opt := range opts {
		opt.applyScopeOption(o)
	}
}

func (o *scopeOptions) validate() error {
	return o.Namespace.validate()
}

// ScopeOption can be applied when scoping results.
type ScopeOption interface {
	applyScopeOption(*scopeOptions)
}

// ----------------------------------------------------------------------------

type namespace string

func (ns namespace) validate() error {
	for i := range ns {
		if b := ns[i]; b >= 0x80 {
			return fmt.Errorf("namespace %q contains non-ASCII characters", ns)
		}
	}
	return nil
}

func (ns namespace) applyListOption(o *listOptions)   { o.Namespace = ns }
func (ns namespace) applyScopeOption(o *scopeOptions) { o.Namespace = ns }

// NamespaceOption can be used in different methods.
type NamespaceOption interface {
	ListOption
	ScopeOption
}

// WithNamespace restricts a client to a particular namespace. Namespaces must
// contain ASCII characters only.
func WithNamespace(ns string) NamespaceOption {
	return namespace(ns)
}

// ----------------------------------------------------------------------------

// Task contains the task definition.
type Task struct {
	ID        uuid.UUID
	Namespace string
	Priority  int16
	Payload   json.RawMessage
	NotBefore time.Time
}

func (t *Task) validate() error {
	return namespace(t.Namespace).validate()
}

// TaskDetails contains detailed task information.
type TaskDetails struct {
	Task
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (td *TaskDetails) scan(rows interface{ Scan(...interface{}) error }) error {
	return rows.Scan(
		&td.ID,
		&td.Namespace,
		&td.Priority,
		&td.Payload,
		&td.NotBefore,
		&td.CreatedAt,
		&td.UpdatedAt,
	)
}
