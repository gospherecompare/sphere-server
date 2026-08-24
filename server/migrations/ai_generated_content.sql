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
      'failed'
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

CREATE INDEX IF NOT EXISTS idx_ai_generated_content_entity
ON ai_generated_content (
  entity_type,
  entity_id
);

CREATE INDEX IF NOT EXISTS idx_ai_generated_content_status
ON ai_generated_content (
  status
);
