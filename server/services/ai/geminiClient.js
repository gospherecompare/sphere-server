const { GoogleGenAI } = require("@google/genai");
const { db } = require("../../db");

const REQUEST_TIMEOUT_MS = 55_000;
const MIN_REQUEST_INTERVAL_MS = 15_000;
let nextRequestAt = 0;

const readGeminiConfig = async () => {
  const result = await db.query(
    `SELECT api_key, model
     FROM gemini_ai_config
     WHERE id = 1
     LIMIT 1`,
  );
  const row = result.rows[0];
  const apiKey = String(row?.api_key || "").trim();
  if (!apiKey) {
    throw new Error("Gemini API key is not configured in admin settings");
  }
  return {
    apiKey,
    model: String(row?.model || "gemini-3.6-flash").trim(),
  };
};

const waitForGeminiSlot = async () => {
  const delay = Math.max(0, nextRequestAt - Date.now());
  if (delay > 0) {
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
  nextRequestAt = Date.now() + MIN_REQUEST_INTERVAL_MS;
};

const generateContent = async ({ systemInstruction, prompt, requestId }) => {
  let response;
  let activeModel;
  try {
    const { apiKey, model } = await readGeminiConfig();
    activeModel = model;
    const ai = new GoogleGenAI({ apiKey });
    await waitForGeminiSlot();
    const providerRequest = ai.interactions.create({
      model,
      input: `${systemInstruction}\n\n${prompt}`,
    });
    response = await Promise.race([
      providerRequest,
      new Promise((_, reject) =>
        setTimeout(
          () =>
            reject(
              Object.assign(new Error("Gemini request timed out"), {
                code: "AI_PROVIDER_TIMEOUT",
              }),
            ),
          REQUEST_TIMEOUT_MS,
        ),
      ),
    ]);
  } catch (error) {
    const status = error?.status || error?.statusCode || null;
    const code = error?.code || error?.error?.code || null;
    const details = [
      status ? `status ${status}` : null,
      code ? `code ${code}` : null,
    ].filter(Boolean);
    const providerError = new Error(
      `Gemini request failed${details.length ? ` (${details.join(", ")})` : ""}: ${error?.message || "Unknown provider error"}`,
    );
    providerError.status = status;
    providerError.code = code;
    providerError.provider = "Gemini";
    providerError.requestId = requestId;
    throw providerError;
  }

  const summary = String(response?.output_text || "").trim();
  if (!summary) {
    const providerError = new Error(
      "Gemini returned a successful response without output_text",
    );
    providerError.provider = "Gemini";
    providerError.requestId = requestId;
    throw providerError;
  }

  return {
    summary,
    model: activeModel,
    inputTokens: response.usage?.input_tokens ?? null,
    outputTokens: response.usage?.output_tokens ?? null,
  };
};

module.exports = {
  generateContent,
};
