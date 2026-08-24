const OpenAI = require("openai");

const apiKey = process.env.OPENAI_API_KEY;

if (!apiKey) {
  throw new Error("OPENAI_API_KEY environment variable is not configured");
}

const openai = new OpenAI({
  apiKey,
});

module.exports = openai;
