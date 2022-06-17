--
-- Automated script, we do not need NOTICE and WARNING
--
SET
  client_min_messages TO ERROR;

--
-- Tasks table
--
CREATE TABLE IF NOT EXISTS tasks (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  priority SMALLINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  attempts INT NOT NULL DEFAULT 0,
  payload JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_tasks_priority_order ON tasks (priority DESC, updated_at ASC);

--
-- Meta info table
--
CREATE TABLE IF NOT EXISTS meta_info (
  name VARCHAR(128) NOT NULL,
  value VARCHAR(512) NOT NULL,
  PRIMARY KEY (name)
);

INSERT INTO
  meta_info
VALUES
  ('schema_version', '1') ON CONFLICT (name) DO NOTHING;
