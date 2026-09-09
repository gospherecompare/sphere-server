"use strict";

const AGENT_CONTRACT_VERSION = "1.0";
const DOMAINS = Object.freeze(["smartphone", "tv", "news"]);
const INTENTS = Object.freeze([
  "filter",
  "recommend",
  "compare",
  "specification",
  "trending",
  "latest",
  "search",
  "spec_score",
  "latest_news",
  "search_news",
  "category_news",
  "brand_news",
  "product_news",
  "article_request",
  "clarify",
]);
const RESPONSE_TYPES = Object.freeze([
  "text",
  "product_list",
  "product_recommendation",
  "product_comparison",
  "specification",
  "trending_products",
  "latest_products",
  "tv_list",
  "tv_recommendation",
  "news_list",
  "news_article",
  "clarification",
  "error",
]);
const TOOL_NAMES = Object.freeze([
  "filterSmartphones",
  "recommendSmartphones",
  "getSmartphoneDetails",
  "compareSmartphones",
  "getSmartphoneSpecScore",
  "getTrendingSmartphones",
  "getLatestSmartphones",
  "searchSmartphones",
  "filterTVs",
  "recommendTVs",
  "getTVDetails",
  "compareTVs",
  "getTrendingTVs",
  "getLatestTVs",
  "searchTVs",
  "getLatestNews",
  "searchNews",
  "getCategoryNews",
  "getBrandNews",
  "getProductNews",
  "getArticle",
]);

const emptyEntities = () => ({
  brand: null,
  product: null,
  category: null,
  min_price: null,
  max_price: null,
  ram: null,
  color: null,
  storage: null,
  battery: null,
  network: null,
  processor: null,
  camera: null,
  front_camera: null,
  rear_camera: null,
  display: null,
  screen_size: null,
  resolution: null,
  refresh_rate: null,
  additional_features: null,
  use_case: null,
  topic: null,
  date: null,
  article: null,
});

const normalizeContext = (context = {}) => ({
  route: typeof context.route === "string" ? context.route : null,
  domain: DOMAINS.includes(context.domain) ? context.domain : null,
  page: typeof context.page === "string" ? context.page : null,
  filters:
    context.filters && typeof context.filters === "object"
      ? context.filters
      : {},
  selectedProducts: Array.isArray(context.selectedProducts)
    ? context.selectedProducts
    : [],
  currentResults: Array.isArray(context.currentResults)
    ? context.currentResults
    : [],
  comparisonProducts: Array.isArray(context.comparisonProducts)
    ? context.comparisonProducts
    : [],
  sessionId: typeof context.sessionId === "string" ? context.sessionId : null,
});

const createAgentRequest = ({ message, context } = {}) => ({
  message: typeof message === "string" ? message.trim() : "",
  context: normalizeContext(context),
});

const createAgentInterpretation = ({
  request,
  domain = null,
  intent = "clarify",
  entities = emptyEntities(),
  filters = {},
  preferences = {},
  tool = null,
  responseType = "clarification",
  confidence = 0,
} = {}) => ({
  contractVersion: AGENT_CONTRACT_VERSION,
  message: request?.message || "",
  context: normalizeContext(request?.context),
  domain: DOMAINS.includes(domain) ? domain : null,
  intent: INTENTS.includes(intent) ? intent : "clarify",
  entities: { ...emptyEntities(), ...entities },
  filters: { ...filters },
  preferences: { ...preferences },
  tool: TOOL_NAMES.includes(tool) ? tool : null,
  responseType: RESPONSE_TYPES.includes(responseType)
    ? responseType
    : "clarification",
  confidence: Math.max(0, Math.min(1, Number(confidence) || 0)),
});

module.exports = {
  AGENT_CONTRACT_VERSION,
  DOMAINS,
  INTENTS,
  RESPONSE_TYPES,
  TOOL_NAMES,
  createAgentRequest,
  createAgentInterpretation,
  emptyEntities,
  normalizeContext,
};
