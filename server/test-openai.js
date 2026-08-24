require("dotenv").config();

const { GoogleGenAI } = require("@google/genai");

const ai = new GoogleGenAI({
  apiKey: process.env.GEMINI_API_KEY,
});

async function testGemini() {
  try {
    console.log("Testing Gemini...");

    const response = await ai.interactions.create({
      model: process.env.GEMINI_MODEL || "gemini-3.6-flash",
      input: "Reply with exactly: Gemini connection working",
    });

    console.log(response.output_text);
  } catch (error) {
    console.error("GEMINI ERROR");
    console.error({
      message: error.message,
      status: error.status,
      code: error.code,
    });
  }
}

testGemini();
