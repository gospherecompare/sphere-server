const { db } = require("../../db");

const getAiContent = async (entityType, entityId, contentType) => {
  try {
    const result = await db.query(
      `
        SELECT
          id,
          entity_type,
          entity_id,
          content_type,
          content,
          status,
          model,
          temperature,
          input_tokens,
          output_tokens,
          input_hash,
          generated_at,
          updated_at
        FROM ai_generated_content
        WHERE entity_type = $1
          AND entity_id = $2
          AND content_type = $3
        LIMIT 1
      `,
      [entityType, entityId, contentType],
    );

    return result.rows[0] || null;
  } catch (error) {
    console.error("Error fetching AI content:", error.message);
    return null;
  }
};

const saveAiContent = async ({
  entityType,
  entityId,
  contentType,
  content,
  status,
  model,
  temperature,
  inputTokens,
  outputTokens,
  inputHash,
  errorMessage = null,
}) => {
  try {
    const result = await db.query(
      `
        INSERT INTO ai_generated_content (
          entity_type,
          entity_id,
          content_type,
          content,
          status,
          model,
          temperature,
          input_tokens,
          output_tokens,
          input_hash,
          error_message,
          generated_at,
          updated_at
        )
        VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9, $10,
          $11,
          CASE WHEN $5 = 'generated' THEN now() ELSE NULL END,
          now()
        )
        ON CONFLICT (
          entity_type,
          entity_id,
          content_type
        )
        DO UPDATE SET
          content = EXCLUDED.content,
          status = EXCLUDED.status,
          model = EXCLUDED.model,
          temperature = EXCLUDED.temperature,
          input_tokens = EXCLUDED.input_tokens,
          output_tokens = EXCLUDED.output_tokens,
          input_hash = EXCLUDED.input_hash,
          error_message = EXCLUDED.error_message,
          generated_at = EXCLUDED.generated_at,
          updated_at = now()
        RETURNING *
      `,
      [
        entityType,
        entityId,
        contentType,
        content,
        status,
        model,
        temperature,
        inputTokens,
        outputTokens,
        inputHash,
        errorMessage,
      ],
    );

    return result.rows[0];
  } catch (error) {
    console.error("Error saving AI content:", error.message);
    throw error;
  }
};

module.exports = {
  getAiContent,
  saveAiContent,
};
