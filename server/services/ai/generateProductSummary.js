const crypto = require("crypto");
const openai = require("./openaiClient");
const { buildProductAiInput } = require("./buildProductAiInput");
const { buildProductSummaryPrompt } = require("./prompts/productSummaryPrompt");

const MODEL = process.env.OPENAI_AI_MODEL || "gpt-4o-mini";
const TEMPERATURE = 0.2;

const createInputHash = (input) =>
  crypto.createHash("sha256").update(JSON.stringify(input)).digest("hex");

const generateProductSummary = async ({
  product,
  smartphone,
  variants,
  prices,
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
    const response = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        {
          role: "system",
          content:
            "You are a technology product summarization assistant. Summarize products accurately and concisely based only on the provided data.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: TEMPERATURE,
      max_tokens: 250,
    });

    const summary = String(
      response.choices?.[0]?.message?.content || "",
    ).trim();

    if (!summary) {
      throw new Error("AI provider returned an empty summary");
    }

    return {
      summary,
      inputHash,
      model: MODEL,
      temperature: TEMPERATURE,
      inputTokens: response.usage?.prompt_tokens ?? null,
      outputTokens: response.usage?.completion_tokens ?? null,
    };
  } catch (error) {
    console.error("OpenAI API error:", error.message);
    throw new Error(
      `Failed to generate product summary: ${error.message || "Unknown error"}`,
    );
  }
};

module.exports = {
  generateProductSummary,
  createInputHash,
};
