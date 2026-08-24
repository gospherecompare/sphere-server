const crypto = require("crypto");
const gemini = require("./geminiClient");
const { buildProductAiInput } = require("./buildProductAiInput");
const { buildProductSummaryPrompt } = require("./prompts/productSummaryPrompt");

const TEMPERATURE = 0.2;

const createInputHash = (input) =>
  crypto.createHash("sha256").update(JSON.stringify(input)).digest("hex");

const generateProductSummary = async ({
  product,
  smartphone,
  variants,
  prices,
  requestId,
}) => {
  const input = buildProductAiInput({
    product,
    smartphone,
    variants,
    prices,
  });

  const inputHash = createInputHash(input);
  const prompt = buildProductSummaryPrompt(input);

  try {
    console.info("Gemini product summary request started", {
      requestId,
      model: gemini.model,
      promptCharacters: prompt.length,
      inputHash,
    });
    const response = await gemini.generateContent({
      systemInstruction:
        "You are a technology product summarization assistant. Summarize products accurately and concisely based only on the provided data.",
      prompt,
      requestId,
    });

    const summary = response.summary;

    console.info("Gemini product summary request completed", {
      requestId,
      model: response.model,
      hasSummary: Boolean(summary),
    });

    if (!summary) {
      throw new Error("AI provider returned an empty summary");
    }

    return {
      summary,
      inputHash,
      model: response.model,
      temperature: TEMPERATURE,
      inputTokens: response.inputTokens,
      outputTokens: response.outputTokens,
    };
  } catch (error) {
    console.error("Gemini API error:", {
      requestId,
      message: error.message,
      status: error.status || null,
      code: error.code || null,
    });
    const wrappedError = new Error(
      `Failed to generate product summary: ${error.message || "Unknown error"}`,
    );
    wrappedError.status = error.status;
    wrappedError.code = error.code;
    wrappedError.requestId = requestId;
    throw wrappedError;
  }
};

module.exports = {
  generateProductSummary,
  createInputHash,
};
