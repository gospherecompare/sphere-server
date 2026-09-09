"use strict";

const DEFAULT_URL = process.env.MOBILEX_NLP_URL || "http://127.0.0.1:8100";

const createNlpModelClient = ({
  baseUrl = DEFAULT_URL,
  fetchImpl = globalThis.fetch,
  timeoutMs = 350,
} = {}) => {
  if (typeof fetchImpl !== "function")
    throw new Error("A fetch implementation is required");

  const understand = async ({ message, context }) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(
        `${baseUrl.replace(/\/+$/, "")}/v1/understand`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message, context }),
          signal: controller.signal,
        },
      );
      if (!response.ok) return null;
      return response.json();
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  };

  return { understand };
};

module.exports = { createNlpModelClient };
