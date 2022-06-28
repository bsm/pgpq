package pgpq

const (
	stmtPush = `
		INSERT INTO pgpq_tasks (namespace, priority, payload)
		VALUES ($1, $2, $3)
		RETURNING id
	`

	stmtPushWithID = `
		INSERT INTO pgpq_tasks (id, namespace, priority, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`

	stmtGet = `
		SELECT
			id,
			namespace,
			priority,
			payload,
			created_at,
			updated_at
		FROM pgpq_tasks
		WHERE id = $1
	`

	stmtShift = `
		SELECT
			id,
			namespace,
			priority,
			payload,
			created_at,
			updated_at
		FROM pgpq_tasks
		WHERE namespace = $1
		ORDER BY
			priority DESC,
			updated_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	stmtClaim = `
		SELECT
			id,
			namespace,
			priority,
			payload,
			created_at,
			updated_at
		FROM pgpq_tasks
		WHERE id = $1
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	stmtList = `
		SELECT
			id,
			namespace,
			priority,
			payload,
			created_at,
			updated_at
		FROM pgpq_tasks
		WHERE namespace = $1
		ORDER BY
			priority DESC,
			updated_at ASC
		LIMIT $2
		OFFSET $3
	`

	stmtUpdate = `
		UPDATE pgpq_tasks
		SET
			namespace  = $1,
			priority   = $2,
			payload    = $3,
			updated_at = NOW()
		WHERE id = $4
	`

	stmtDone = `
		DELETE FROM pgpq_tasks
		WHERE id = $1
	`
)
