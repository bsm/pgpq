package pgpq

import (
	"context"
	"database/sql"

	"github.com/benbjohnson/clock"
)

// Claim contains a claim on a task. The owner of the claim has an exclusive
// lock on the task. You must call either Release, Update or Done to release the
// claim.
type Claim struct {
	TaskDetails
	tx    *sql.Tx
	clock clock.Clock
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

	_, err := tc.tx.ExecContext(ctx, stmtUpdate, tc.Namespace, tc.Priority, unsafeString(tc.Payload), coalesceTime(tc.NotBefore, unixZero), tc.clock.Now(), tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}

// Done marks the task as done and removes it from the queue.
func (tc *Claim) Done(ctx context.Context) error {
	_, err := tc.tx.ExecContext(ctx, stmtDone, tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}
