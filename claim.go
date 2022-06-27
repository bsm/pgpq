package pgpq

import (
	"context"
	"database/sql"
)

// Claim contains a claim on a task. The owner of the claim has an exclusive
// lock on the task. You must call either Release, Update or Done to release the
// claim.
type Claim struct {
	TaskDetails
	tx           *sql.Tx
	update, done *sql.Stmt
}

// Release releases the claim and returns the task back to the queue.
func (tc *Claim) Release(_ context.Context) error {
	return tc.tx.Rollback()
}

// Update updates Namespace, Payload, Priority, UpdatedAt and returns the task
// back to the queue.
func (tc *Claim) Update(ctx context.Context) error {
	if err := tc.validate(); err != nil {
		return err
	}

	_, err := tc.tx.
		StmtContext(ctx, tc.update).
		ExecContext(ctx, tc.Namespace, tc.Priority, tc.Payload, tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}

// Done marks the task as done and removes it from the queue.
func (tc *Claim) Done(ctx context.Context) error {
	_, err := tc.tx.
		StmtContext(ctx, tc.done).
		ExecContext(ctx, tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}
