const crypto = require("crypto");
const openai = require("./openaiClient");
const { buildBlogAiInput } = require("./buildBlogAiInput");
const { buildBlogSummaryPrompt } = require("./prompts/blogSummaryPrompt");

const MODEL = process.env.OPENAI_AI_MODEL || "gpt-4o-mini";

const TEMPERATURE = 0.2;

const createInputHash = (input) =>
  crypto.createHash("sha256").update(JSON.stringify(input)).digest("hex");

const generateBlogSummary = async (blog) => {
  const input = buildBlogAiInput(blog);
  const inputHash = createInputHash(input);

  const prompt = buildBlogSummaryPrompt(input);

  try {
    const response = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        {
          role: "system",
          content:
            "You are a technology-news summarization assistant. Summarize articles accurately and concisely without adding outside knowledge.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: TEMPERATURE,
      max_tokens: 220,
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
      `Failed to generate summary: ${error.message || "Unknown error"}`,
    );
  }
};

module.exports = {
  generateBlogSummary,
  createInputHash,
};
