const crypto = require("crypto");
const gemini = require("./geminiClient");
const { buildBlogAiInput } = require("./buildBlogAiInput");
const { buildBlogSummaryPrompt } = require("./prompts/blogSummaryPrompt");

const TEMPERATURE = 0.2;

const createInputHash = (input) =>
  crypto.createHash("sha256").update(JSON.stringify(input)).digest("hex");

const generateBlogSummary = async (blog) => {
  const input = buildBlogAiInput(blog);
  const inputHash = createInputHash(input);

  const prompt = buildBlogSummaryPrompt(input);

  try {
    const response = await gemini.generateContent({
      systemInstruction:
        "You are a technology-news summarization assistant. Summarize articles accurately and concisely without adding outside knowledge.",
      prompt,
    });

    const summary = response.summary;

    if (!summary) {
      throw new Error("AI provider returned an empty summary");
    }

    return {
      summary,
      inputHash,
      inputText: JSON.stringify(input, null, 2),
      promptText: prompt,
      model: response.model,
      temperature: TEMPERATURE,
      inputTokens: response.inputTokens,
      outputTokens: response.outputTokens,
    };
  } catch (error) {
    console.error("Gemini API error:", {
      message: error.message,
      status: error.status || null,
      code: error.code || null,
    });
    const wrappedError = new Error(
      `Failed to generate summary: ${error.message || "Unknown error"}`,
    );
    wrappedError.status = error.status;
    wrappedError.code = error.code;
    throw wrappedError;
  }
};

module.exports = {
  generateBlogSummary,
  createInputHash,
};
