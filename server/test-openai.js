require("dotenv").config();

const { GoogleGenAI } = require("@google/genai");
const { db } = require("./db");

async function testGemini() {
  try {
    console.log("Testing Gemini...");
    const result = await db.query(
      `SELECT api_key, model
       FROM gemini_ai_config
       WHERE id = 1
       LIMIT 1`,
    );
    const config = result.rows[0];
    if (!config?.api_key) throw new Error("Gemini API key is not configured in admin settings");

    const ai = new GoogleGenAI({ apiKey: config.api_key });

    const response = await ai.interactions.create({
      model: config.model || "gemini-3.6-flash",
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
