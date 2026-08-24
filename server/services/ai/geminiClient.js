const { GoogleGenAI } = require("@google/genai");

const apiKey = String(process.env.GEMINI_API_KEY || "").trim();
const model = String(process.env.GEMINI_MODEL || "gemini-3.6-flash").trim();
const REQUEST_TIMEOUT_MS = 55_000;

if (!apiKey) {
  throw new Error("GEMINI_API_KEY environment variable is not configured");
}

const ai = new GoogleGenAI({ apiKey });

const generateContent = async ({ systemInstruction, prompt, requestId }) => {
  let response;
  try {
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
    model,
    inputTokens: response.usage?.input_tokens ?? null,
    outputTokens: response.usage?.output_tokens ?? null,
  };
};

module.exports = {
  generateContent,
  model,
};
