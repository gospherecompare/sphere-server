"use strict";

const { parseAgentRequest, routeFor } = require("./deterministicParser");
const { createResponseGenerator } = require("./responseGenerator");

const titleFor = (interpretation) => {
  const { domain, intent, entities } = interpretation;
  const subject = entities.brand ? `${entities.brand} ` : "";
  if (intent === "latest_news") return `Latest ${subject}news`;
  if (domain === "news") return `${subject}News`;
  if (intent === "trending")
    return `Trending ${domain === "tv" ? "TVs" : "smartphones"}`;
  if (intent === "latest")
    return `Latest ${domain === "tv" ? "TVs" : "smartphones"}`;
  return `${subject}${domain === "tv" ? "TV" : "smartphone"} results`;
};

const clarification = (interpretation, message) => ({
  ...interpretation,
  responseType: "clarification",
  title: "I need a little more context",
  message,
  items: [],
  actions: [],
});

const createAgentExecutor = ({
  toolExecutor,
  nlpClient = null,
  responseGenerator = createResponseGenerator(),
} = {}) => {
  if (!toolExecutor || typeof toolExecutor.execute !== "function")
    throw new Error("toolExecutor.execute is required");

  const execute = async (input) => {
    let interpretation = parseAgentRequest(input);
    const modelResult = nlpClient ? await nlpClient.understand(input) : null;
    const domainConfidence = Number(modelResult?.domain_confidence || 0);
    const intentConfidence = Number(modelResult?.intent_confidence || 0);
    const modelAgreesWithRules =
      modelResult?.domain === interpretation.domain &&
      modelResult?.intent === interpretation.intent;
    if (
      ["smartphone", "tv", "news"].includes(modelResult?.domain) &&
      (modelAgreesWithRules
        ? domainConfidence >= 0.55
        : domainConfidence >= 0.75) &&
      typeof modelResult?.intent === "string" &&
      (modelAgreesWithRules
        ? intentConfidence >= 0.25
        : intentConfidence >= 0.78)
    ) {
      const route = routeFor(modelResult.domain, modelResult.intent);
      if (route.tool) {
        interpretation = {
          ...interpretation,
          domain: modelResult.domain,
          intent: modelResult.intent,
          tool: route.tool,
          responseType: route.responseType,
          confidence: Math.min(domainConfidence, intentConfidence),
          model: {
            version: modelResult.model_version || "unknown",
            domainConfidence,
            intentConfidence,
            dl: modelResult.dl || null,
            entities: modelResult.entities || {},
          },
        };
      }
    }
    if (
      !interpretation.domain ||
      interpretation.intent === "clarify" ||
      !interpretation.tool
    ) {
      return clarification(
        interpretation,
        "Tell me whether you want to find, compare, or learn about a smartphone, TV, or news article.",
      );
    }

    const result = await toolExecutor.execute(interpretation);
    if (result.needsClarification) {
      return clarification(
        interpretation,
        interpretation.intent === "compare"
          ? "Select two products first, then I can compare them."
          : "Open a product or select one first so I can read its specifications.",
      );
    }

    const generated = await responseGenerator.generate({
      interpretation,
      result,
    });
    return {
      ...interpretation,
      title: generated.title || titleFor(interpretation),
      message: generated.message,
      items: result.items || [],
      comparison: result.comparison || null,
      actions: [
        "view",
        ...(interpretation.intent === "filter" ||
        interpretation.intent === "recommend"
          ? ["compare"]
          : []),
      ],
      source: result.source,
    };
  };

  return { execute };
};

module.exports = { createAgentExecutor };
