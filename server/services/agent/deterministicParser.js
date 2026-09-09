"use strict";

const {
  createAgentInterpretation,
  createAgentRequest,
  emptyEntities,
} = require("./agentContract");

const DOMAIN_PATTERNS = [
  [
    "news",
    /\b(news|article|articles|story|stories|headlines|happened|update|updates)\b/i,
  ],
  [
    "tv",
    /\b(tvs?|televisions?|smart\s*tvs?|inch|inches|4k\s*tvs?|oled|qled)\b/i,
  ],
  [
    "smartphone",
    /\b(phone|phones|mobile|mobiles|smartphone|smartphones|iphone|galaxy|pixel|oneplus)\b/i,
  ],
];
const KNOWN_BRANDS = [
  "Samsung",
  "Apple",
  "OnePlus",
  "Xiaomi",
  "Redmi",
  "Realme",
  "Google",
  "Vivo",
  "Oppo",
  "Motorola",
  "Nothing",
  "Sony",
  "LG",
];

const firstMatch = (text, pattern) => text.match(pattern)?.[1] || null;
const parseNumber = (value) => {
  const number = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(number) ? number : null;
};

const normalizeMemory = (value) => {
  const raw = String(value || "")
    .trim()
    .toUpperCase();
  if (!raw) return null;
  const match = raw.match(/(\d+(?:\.\d+)?)\s*(GB|TB)?/i);
  return match ? `${match[1]}${(match[2] || "GB").toUpperCase()}` : raw;
};

const unitMultiplier = (unit = "") => {
  if (/^(k|thousand|grand)$/i.test(unit)) return 1000;
  if (/^(lakh|lac)$/i.test(unit)) return 100000;
  return 1;
};

const parsePriceNumber = (value, unit = "") => {
  const number = parseNumber(value);
  return number === null ? null : number * unitMultiplier(unit);
};

const parsePriceRange = (text) => {
  const match = text.match(
    /(?:between|from)\s*(?:₹|rs\.?|inr\s*)?\s*(\d+(?:[,.]\d+)?)\s*(k|thousand|grand|lakh|lac)?\s*(?:and|to|-|–)\s*(?:₹|rs\.?|inr\s*)?\s*(\d+(?:[,.]\d+)?)\s*(k|thousand|grand|lakh|lac)?/i,
  );
  if (!match) return null;
  const min = parsePriceNumber(match[1], match[2]);
  const max = parsePriceNumber(match[3], match[4]);
  return min !== null && max !== null ? { min, max } : null;
};

const parsePrice = (text) => {
  const range = parsePriceRange(text);
  if (range) return range;
  const match =
    text.match(
      /(?:under|below|less than|up to|within|around|over|above|more than|starting at)\s*(?:₹|rs\.?|inr\s*)?\s*(\d+(?:[,.]\d+)?)\s*(k|thousand|grand|lakh|lac)?/i,
    ) ||
    text.match(
      /(?:₹|rs\.?|inr\s*)?\s*(\d+(?:[,.]\d+)?)\s*(k|thousand|grand|lakh|lac)/i,
    );
  if (!match) return null;
  return parsePriceNumber(match[1], match[2]);
};

const parseEntities = (text, domain) => {
  const entities = emptyEntities();
  entities.brand =
    KNOWN_BRANDS.find((candidate) =>
      new RegExp(`\\b${candidate}\\b`, "i").test(text),
    ) || null;

  const range = parsePriceRange(text);
  const price = parsePrice(text);
  if (range) {
    entities.min_price = range.min;
    entities.max_price = range.max;
  } else if (/\b(under|below|less than|up to|within|around)\b/i.test(text)) {
    entities.max_price = price;
  } else if (/\b(over|above|more than|starting at)\b/i.test(text)) {
    entities.min_price = price;
  }

  entities.ram = normalizeMemory(
    firstMatch(text, /(\d+(?:\.\d+)?)\s*gb\s*ram/i),
  );
  entities.storage = normalizeMemory(
    firstMatch(
      text,
      /(\d+(?:\.\d+)?)\s*(gb|tb)\s*(?:storage|rom|internal storage)?/i,
    ),
  );
  entities.battery = firstMatch(text, /(\d{3,5})\s*mah/i);
  entities.screen_size = firstMatch(
    text,
    /(\d+(?:\.\d+)?)\s*(?:inch|inches|in)\b/i,
  );
  entities.refresh_rate = firstMatch(text, /(\d+)\s*hz/i);
  entities.resolution = firstMatch(
    text,
    /\b(8k|4k|2k|full hd\+?|fhd\+?|uhd|hd|2160p|1440p|1080p)\b/i,
  );
  entities.processor = firstMatch(
    text,
    /\b(snapdragon(?:\s+[\w+.-]+)+|dimensity(?:\s+[\w+.-]+)+|exynos(?:\s+[\w+.-]+)+|apple\s+a\d+|tensor(?:\s+[\w+.-]+)*)\b/i,
  );
  entities.network = firstMatch(
    text,
    /\b(5g|4g|lte|wi-?fi\s*7|wi-?fi\s*6e?|wifi|bluetooth)\b/i,
  );
  entities.color = firstMatch(
    text,
    /\b(black|white|blue|green|red|purple|silver|gold|gray|grey)\b/i,
  );
  entities.display = firstMatch(
    text,
    /\b(amoled|oled|qled|mini[-\s]?led|ltpo|ips|lcd)\b/i,
  );

  if (/\b(gaming|games|heavy gaming|gamers?)\b/i.test(text))
    entities.use_case = "gaming";
  else if (/\b(camera|photography|photos|photography-first)\b/i.test(text))
    entities.use_case = "camera";
  else if (/\b(movie|movies|cinema|films|streaming)\b/i.test(text))
    entities.use_case = "movies";

  if (domain === "news") {
    entities.product = extractProductHint(text);
    entities.topic = firstMatch(
      text,
      /\b(smartphones?|mobiles?|phones?|tv|television|technology|tech|ai|gaming|laptops?|chips?|software|cybersecurity|wearables?)\b/i,
    );
    if (/\btoday(?:'s)?\b|\btoday\b/i.test(text)) entities.date = "today";
    else if (/\byesterday\b/i.test(text)) entities.date = "yesterday";
    else if (/\bthis\s+week\b/i.test(text)) entities.date = "this_week";
  }
  return entities;
};
const detectDomain = (text, context) => {
  const routeDomain = context.domain || null;
  const hasNewsCue = DOMAIN_PATTERNS[0][1].test(text);
  const hasTvCue = DOMAIN_PATTERNS[1][1].test(text);
  const hasPhoneCue = DOMAIN_PATTERNS[2][1].test(text);
  if (
    hasNewsCue &&
    /\b(news|article|articles|story|stories|headlines)\b/i.test(text)
  )
    return "news";
  if (hasTvCue && !hasPhoneCue) return "tv";
  if (hasPhoneCue && !hasTvCue) return "smartphone";
  if (routeDomain) return routeDomain;
  if (hasNewsCue) return "news";
  if (hasTvCue) return "tv";
  if (hasPhoneCue) return "smartphone";
  return null;
};

const entitiesBrandHint = (text) =>
  KNOWN_BRANDS.some((brand) => new RegExp(`\\b${brand}\\b`, "i").test(text));

const productHint = (text) =>
  /\b(?:iphone|galaxy|pixel|oneplus|redmi|xiaomi|realme|nothing|motorola|vivo|oppo)(?:\s+[a-z0-9.-]+){1,4}\b/i.test(
    text,
  );

const extractProductHint = (text) =>
  text
    .match(
      /\b((?:iphone|galaxy|pixel|oneplus|redmi|xiaomi|realme|nothing|motorola|vivo|oppo)\s+[a-z0-9][a-z0-9 .+\-]{0,30}?)(?=\s+(?:news|article|story|updates?|launch|review)\b|[?!.]|$)/i,
    )?.[1]
    ?.trim() || null;

const detectIntent = (text, domain) => {
  if (/\b(compare|versus|vs\.?|between)\b/i.test(text)) return "compare";
  if (
    /\b(best|recommend|recommendation|good for|which one|mainly for|focused on)\b/i.test(
      text,
    ) ||
    /\b(gaming|photography|movies|cinema)\b/i.test(text)
  )
    return "recommend";
  if (/\b(spec(?:ification)?\s*score|spec score)\b/i.test(text))
    return "spec_score";
  if (
    /\b(spec|specification|battery|camera|display|refresh rate|does it support)\b/i.test(
      text,
    )
  )
    return "specification";
  if (/\b(trending|popular|blowing up|hot right now)\b/i.test(text))
    return "trending";
  if (/\b(latest|newest|new launches|recent)\b/i.test(text))
    return domain === "news" ? "latest_news" : "latest";
  if (
    domain === "news" &&
    /\b(open|read|article|story|stories|summarize|summary|full story|report)\b/i.test(
      text,
    )
  )
    return "article_request";
  if (domain === "news" && /\b(category|topic|section|browse)\b/i.test(text))
    return "category_news";
  if (
    domain === "news" &&
    /\b(news|stories|updates|announcements|happened)\b/i.test(text)
  ) {
    if (/\b(search|find|look up|matching|containing)\b/i.test(text))
      return "search_news";
    if (entitiesBrandHint(text)) return "brand_news";
    if (productHint(text)) return "product_news";
    return "search_news";
  }
  if (domain === "news" && /\b(search|find|show)\b/i.test(text))
    return "search_news";
  if (/\b(search|look up)\b/i.test(text)) return "search";
  if (/\b(search|find|show|under|below|with|only)\b/i.test(text))
    return "filter";
  return "clarify";
};

const chooseTool = (domain, intent) =>
  ({
    smartphone: {
      filter: "filterSmartphones",
      recommend: "recommendSmartphones",
      compare: "compareSmartphones",
      specification: "getSmartphoneDetails",
      spec_score: "getSmartphoneSpecScore",
      trending: "getTrendingSmartphones",
      latest: "getLatestSmartphones",
      search: "searchSmartphones",
    },
    tv: {
      filter: "filterTVs",
      recommend: "recommendTVs",
      compare: "compareTVs",
      specification: "getTVDetails",
      trending: "getTrendingTVs",
      latest: "getLatestTVs",
      search: "searchTVs",
    },
    news: {
      latest_news: "getLatestNews",
      search_news: "searchNews",
      category_news: "getCategoryNews",
      brand_news: "getBrandNews",
      product_news: "getProductNews",
      article_request: "getArticle",
    },
  })[domain]?.[intent] || null;

const chooseResponseType = (domain, intent) => {
  if (intent === "clarify") return "clarification";
  if (intent === "compare") return "product_comparison";
  if (intent === "specification") return "specification";
  if (intent === "spec_score") return "specification";
  if (intent === "trending") return "trending_products";
  if (intent === "latest" || intent === "latest_news")
    return domain === "news" ? "news_list" : "latest_products";
  if (domain === "news") return "news_list";
  if (domain === "tv")
    return intent === "recommend" ? "tv_recommendation" : "tv_list";
  return intent === "recommend" ? "product_recommendation" : "product_list";
};

const routeFor = (domain, intent) => ({
  tool: chooseTool(domain, intent),
  responseType: chooseResponseType(domain, intent),
});

const parseAgentRequest = (input) => {
  const request = createAgentRequest(input);
  const domain = detectDomain(request.message, request.context);
  const intent = detectIntent(request.message, domain);
  const entities = parseEntities(request.message, domain);
  const contextFilters = request.context.filters || {};
  return createAgentInterpretation({
    request,
    domain,
    intent,
    entities,
    filters: {
      brand: entities.brand || contextFilters.brand || null,
      minPrice: entities.min_price ?? contextFilters.minPrice ?? null,
      maxPrice: entities.max_price ?? contextFilters.maxPrice ?? null,
      ram: entities.ram || contextFilters.ram || null,
      storage: entities.storage || contextFilters.storage || null,
      color: entities.color || contextFilters.color || null,
      battery: entities.battery || contextFilters.battery || null,
      processor: entities.processor || contextFilters.processor || null,
      network: entities.network || contextFilters.network || null,
      camera: entities.camera || contextFilters.camera || null,
      rearCamera: entities.rear_camera || contextFilters.rearCamera || null,
      frontCamera: entities.front_camera || contextFilters.frontCamera || null,
      display: entities.display || contextFilters.display || null,
      screenSize: entities.screen_size || contextFilters.screenSize || null,
      resolution: entities.resolution || contextFilters.resolution || null,
      refreshRate: entities.refresh_rate || contextFilters.refreshRate || null,
      additionalFeatures:
        entities.additional_features ||
        contextFilters.additionalFeatures ||
        null,
      topic: entities.topic || contextFilters.topic || null,
      date: entities.date || contextFilters.date || null,
    },
    preferences: {
      useCase: entities.use_case,
    },
    tool: chooseTool(domain, intent),
    responseType: chooseResponseType(domain, intent),
    confidence: domain && intent !== "clarify" ? 0.8 : domain ? 0.55 : 0,
  });
};

module.exports = { parseAgentRequest, routeFor };
