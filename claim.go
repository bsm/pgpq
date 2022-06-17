package pgpq

import (
	"context"
	"database/sql"
)

// Claim contains a claim on a task. The owner of the claim has an exclusive
// lock on the task. You must call either Ack or NAck to release the
// claim.
type Claim struct {
	TaskDetails
	tx             *sql.Tx
	update, remove *sql.Stmt
}

// Release releases the claim and returns the task back to the queue.
func (tc *Claim) Rollback(ctx context.Context) error {
	return tc.tx.Rollback()
}

// Update updates Payload, Priority and UpdatedAt and returns the task back to the queue.
func (tc *Claim) Update(ctx context.Context) error {
	_, err := tc.tx.
		StmtContext(ctx, tc.update).
		ExecContext(ctx, tc.Priority, tc.Payload, tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}

// Remove removes the task from the queue.
func (tc *Claim) Remove(ctx context.Context) error {
	_, err := tc.tx.
		StmtContext(ctx, tc.remove).
		ExecContext(ctx, tc.ID)
	if err != nil {
		return err
	}

	return tc.tx.Commit()
}
