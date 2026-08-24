CREATE TABLE IF NOT EXISTS ai_generated_content (
  id BIGSERIAL PRIMARY KEY,

  entity_type TEXT NOT NULL,
  entity_id INT NOT NULL,

  content_type TEXT NOT NULL,

  content TEXT,

  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN (
      'pending',
      'generating',
      'generated',
      'failed',
      'disabled',
      'waiting_for_data'
    )),

  model TEXT,

  temperature NUMERIC(4,2),

  input_tokens INT,
  output_tokens INT,

  input_hash TEXT,

  error_message TEXT,

  generated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  updated_at TIMESTAMP NOT NULL DEFAULT now(),

  UNIQUE (
    entity_type,
    entity_id,
    content_type
  )
);

ALTER TABLE ai_generated_content
  DROP CONSTRAINT IF EXISTS ai_generated_content_status_check;

ALTER TABLE ai_generated_content
  ADD CONSTRAINT ai_generated_content_status_check
  CHECK (status IN ('pending', 'generating', 'generated', 'failed', 'disabled', 'waiting_for_data'));

CREATE INDEX IF NOT EXISTS idx_ai_generated_content_entity
ON ai_generated_content (
  entity_type,
  entity_id
);

CREATE INDEX IF NOT EXISTS idx_ai_generated_content_status
ON ai_generated_content (
  status
);
