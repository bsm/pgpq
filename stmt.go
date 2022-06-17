package pgpq

const (
	stmtPush = `
		INSERT INTO tasks (priority, payload)
		VALUES ($1, $2)
		RETURNING id
	`

	stmtPushWithID = `
		INSERT INTO tasks (id, priority, payload)
		VALUES ($1, $2, $3)
		RETURNING id
	`

	stmtGet = `
		SELECT
			id,
			priority,
			payload,
			created_at,
			updated_at
		FROM tasks
		WHERE id = $1
	`

	stmtShift = `
		SELECT
			id,
			priority,
			payload,
			created_at,
			updated_at
		FROM tasks
		ORDER BY
			priority DESC,
			updated_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	stmtList = `
		SELECT
			id,
			priority,
			payload,
			created_at,
			updated_at
		FROM tasks
		ORDER BY
			priority DESC,
			updated_at ASC
		LIMIT $1
		OFFSET $2
	`

	stmtUpdate = `
		UPDATE tasks
		SET
			priority = $1,
			payload  = $2,
			updated_at = NOW()
		WHERE id = $3
	`

	stmtRemove = `
		DELETE FROM tasks
		WHERE id = $1
	`
)
