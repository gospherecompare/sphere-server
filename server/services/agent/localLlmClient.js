"use strict";

const DEFAULT_URL = process.env.MOBILEX_LLM_URL || "";
const DEFAULT_MODEL = process.env.MOBILEX_LLM_MODEL || "";

const createLocalLlmClient = ({ baseUrl = DEFAULT_URL, model = DEFAULT_MODEL, fetchImpl = globalThis.fetch, timeoutMs = 8000 } = {}) => {
  if (!baseUrl || !model || typeof fetchImpl !== "function") return null;
  const generate = async ({ system, user }) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(baseUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model,
          messages: [
            { role: "system", content: system },
            { role: "user", content: user },
          ],
          stream: false,
          options: { temperature: 0.2 },
        }),
        signal: controller.signal,
      });
      if (!response.ok) return null;
      const payload = await response.json();
      return payload?.message?.content || payload?.choices?.[0]?.message?.content || null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  };
  return { generate };
};

module.exports = { createLocalLlmClient };
