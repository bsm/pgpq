--
-- Automated script, we do not need NOTICE and WARNING
--
SET
  client_min_messages TO ERROR;

--
-- Require pgcrypto extension
--
CREATE EXTENSION IF NOT EXISTS pgcrypto;

--
-- Tasks table
--
CREATE TABLE IF NOT EXISTS pgpq_tasks (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  namespace TEXT COLLATE "C" NOT NULL DEFAULT '',
  priority SMALLINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  payload JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_pgpq_tasks_namespace ON pgpq_tasks (namespace ASC);

CREATE INDEX IF NOT EXISTS idx_pgpq_tasks_created_at ON pgpq_tasks (created_at ASC);

CREATE INDEX IF NOT EXISTS idx_pgpq_tasks_priority_order ON pgpq_tasks (priority DESC, updated_at ASC);

--
-- Meta info table
--
CREATE TABLE IF NOT EXISTS pgpq_meta_info (
  name VARCHAR(128) NOT NULL,
  value VARCHAR(512) NOT NULL,
  PRIMARY KEY (name)
);

INSERT INTO
  pgpq_meta_info
VALUES
  ('schema_version', '3') ON CONFLICT (name) DO
UPDATE
SET
  value = EXCLUDED.value;
