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
	tx *sql.Tx
}

// NAck returns the task back to the queue.
func (tc *Claim) NAck(ctx context.Context) error {
	_, err := tc.tx.ExecContext(ctx, `
		UPDATE tasks
		SET attempts = attempts + 1,
				updated_at = NOW()
		WHERE id = $1
	`, tc.ID)
	if err != nil {
		_ = tc.tx.Rollback()
		return err
	}

	return tc.tx.Commit()
}

// Ack removes the task from the queue.
func (tc *Claim) Ack(ctx context.Context) error {
	_, err := tc.tx.ExecContext(ctx, `
		DELETE FROM tasks
		WHERE id = $1
	`, tc.ID)
	if err != nil {
		_ = tc.tx.Rollback()
		return err
	}

	return tc.tx.Commit()
}
