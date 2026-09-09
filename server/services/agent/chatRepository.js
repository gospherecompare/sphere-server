"use strict";

const createChatRepository = ({ db }) => {
  if (!db || typeof db.query !== "function")
    throw new Error("db.query is required");

  const createSession = async ({ sessionId, userId, context }) => {
    if (sessionId) {
      const existing = await db.query(
        "SELECT id FROM ai_chat_sessions WHERE id = $1 LIMIT 1",
        [sessionId],
      );
      if (existing.rows[0]) return existing.rows[0].id;
    }
    const result = await db.query(
      `INSERT INTO ai_chat_sessions (user_id, route, domain, state_json)
       VALUES ($1, $2, $3, $4::jsonb) RETURNING id`,
      [
        userId || null,
        context?.route || null,
        context?.domain || null,
        JSON.stringify(context || {}),
      ],
    );
    return result.rows[0].id;
  };

  const saveTurn = async ({ sessionId, input, response }) => {
    await db.query(
      `INSERT INTO ai_chat_messages
        (session_id, role, message, intent, entities_json, tool_used, response_json, model_version)
       VALUES ($1, 'user', $2, $3, $4::jsonb, $5, NULL, NULL),
              ($1, 'assistant', $6, $7, NULL, $8, $9::jsonb, 'deterministic-v1')`,
      [
        sessionId,
        input.message,
        response.intent,
        JSON.stringify(response.entities || {}),
        response.tool,
        response.message,
        response.intent,
        response.tool,
        JSON.stringify(response),
      ],
    );
    await db.query(
      `UPDATE ai_chat_sessions SET route = $2, domain = $3, state_json = $4::jsonb, updated_at = now() WHERE id = $1`,
      [
        sessionId,
        input.context?.route || null,
        response.domain || null,
        JSON.stringify({
          ...input.context,
          currentResults: response.items || [],
        }),
      ],
    );
  };

  return { createSession, saveTurn };
};

module.exports = { createChatRepository };
