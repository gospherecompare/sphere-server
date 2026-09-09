const test = require("node:test");
const assert = require("node:assert/strict");

const { createAgentExecutor } = require("../services/agent/agentExecutor");
const {
  createAgentToolExecutor,
  extractProductReferences,
} = require("../services/agent/toolExecutor");
const { parseAgentRequest } = require("../services/agent/deterministicParser");
const { createNlpModelClient } = require("../services/agent/nlpModelClient");

const capabilities = {
  listProducts: async (input) => [{ id: 1, input }],
  resolveProducts: async ({ names }) =>
    names.map((name, index) => ({ product_id: index + 10, name })),
  getProduct: async (id) => ({ id, name: "Resolved product" }),
  compareProducts: async (ids) => ({
    devices: ids.map((id) => ({ id })),
    overallWinner: ids[0],
  }),
  searchNews: async (input) => [{ id: 20, input }],
  searchProducts: async (input) => [{ id: 30, input }],
  getSmartphoneSpecScore: async (input) => ({ id: 31, input }),
};

test("routes trending and latest through distinct catalog capabilities", async () => {
  const calls = [];
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({
      capabilities: {
        ...capabilities,
        getTrendingProducts: async (input) => {
          calls.push(["trending", input]);
          return [{ id: 2 }];
        },
        getLatestProducts: async (input) => {
          calls.push(["latest", input]);
          return [{ id: 3 }];
        },
      },
    }),
  });

  await executor.execute({ message: "Trending smartphones", context: {} });
  await executor.execute({ message: "Latest smartphones", context: {} });

  assert.equal(calls[0][0], "trending");
  assert.equal(calls[1][0], "latest");
  assert.equal(calls[0][1].domain, "smartphone");
  assert.equal(calls[1][1].domain, "smartphone");
});

test("emits canonical filters and preferences", () => {
  const result = parseAgentRequest({
    message: "Best Samsung phones under 30k for gaming",
  });
  assert.deepEqual(result.filters, {
    brand: "Samsung",
    minPrice: null,
    maxPrice: 30000,
    ram: null,
    storage: null,
    color: null,
    battery: null,
    processor: null,
    network: null,
    camera: null,
    rearCamera: null,
    frontCamera: null,
    display: null,
    screenSize: null,
    resolution: null,
    refreshRate: null,
    additionalFeatures: null,
    topic: null,
    date: null,
  });
  assert.deepEqual(result.preferences, { useCase: "gaming" });
});

test("preserves active filters for contextual follow-up requests", () => {
  const result = parseAgentRequest({
    message: "Which is best for gaming?",
    context: {
      domain: "smartphone",
      filters: { brand: "Samsung", maxPrice: 30000 },
    },
  });

  assert.equal(result.intent, "recommend");
  assert.equal(result.filters.brand, "Samsung");
  assert.equal(result.filters.maxPrice, 30000);
});

test("extracts free-form comparison product references", () => {
  assert.deepEqual(
    extractProductReferences("Compare iPhone 16 and Galaxy S25"),
    ["iPhone 16", "Galaxy S25"],
  );
});

test("executes catalog capabilities without an internal HTTP client", async () => {
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({ capabilities }),
  });
  const response = await executor.execute({
    message: "Compare iPhone 16 and Galaxy S25",
    context: {},
  });
  assert.equal(response.responseType, "product_comparison");
  assert.deepEqual(response.items, [{ id: 10 }, { id: 11 }]);
});

test("resolves a specification product from the message", async () => {
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({ capabilities }),
  });
  const response = await executor.execute({
    message: "What is the battery of Galaxy S25?",
    context: {},
  });
  assert.equal(response.responseType, "specification");
  assert.equal(response.items[0].id, 10);
});

test("passes targeted news parameters to the capability service", async () => {
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({ capabilities }),
  });
  const response = await executor.execute({
    message: "Latest Samsung smartphone news today",
    context: {},
  });
  assert.equal(response.responseType, "news_list");
  assert.deepEqual(response.items[0].input, {
    brand: "Samsung",
    topic: "smartphone",
    date: "today",
    query: null,
    limit: 20,
  });
});

test("uses the trained model only when both predictions clear confidence gates", async () => {
  const executor = createAgentExecutor({
    nlpClient: {
      understand: async () => ({
        model_version: "tfidf-logreg-v1",
        domain: "tv",
        intent: "recommend",
        domain_confidence: 0.91,
        intent_confidence: 0.89,
      }),
    },
    toolExecutor: {
      execute: async (interpretation) => ({
        items: [{ domain: interpretation.domain }],
        source: "test",
      }),
    },
  });
  const response = await executor.execute({
    message: "something unclear",
    context: {},
  });
  assert.equal(response.domain, "tv");
  assert.equal(response.intent, "recommend");
  assert.equal(response.model.version, "tfidf-logreg-v1");
});

test("accepts a lower-confidence model confirmation when it agrees with rules", async () => {
  const executor = createAgentExecutor({
    nlpClient: {
      understand: async () => ({
        model_version: "tfidf-logreg-v1",
        domain: "smartphone",
        intent: "recommend",
        domain_confidence: 0.64,
        intent_confidence: 0.3,
      }),
    },
    toolExecutor: {
      execute: async (interpretation) => ({
        items: [{ domain: interpretation.domain }],
        source: "test",
      }),
    },
  });
  const response = await executor.execute({
    message: "Best Samsung phone for gaming",
    context: {},
  });
  assert.equal(response.model.version, "tfidf-logreg-v1");
  assert.equal(response.domain, "smartphone");
});

test("falls back when the NLP service is unavailable", async () => {
  const client = createNlpModelClient({
    baseUrl: "http://unavailable.test",
    timeoutMs: 10,
    fetchImpl: async () => {
      throw new Error("offline");
    },
  });
  assert.equal(
    await client.understand({ message: "Show phones", context: {} }),
    null,
  );
});

test("routes recommendation through the recommendation capability", async () => {
  const calls = [];
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({
      capabilities: {
        ...capabilities,
        recommendProducts: async (input) => {
          calls.push(input);
          return [{ id: 7 }];
        },
      },
    }),
  });
  const response = await executor.execute({
    message: "Best Samsung phones under 30k for gaming",
    context: {},
  });
  assert.equal(response.responseType, "product_recommendation");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].domain, "smartphone");
  assert.equal(calls[0].preferences.useCase, "gaming");
});

test("supports latest TV as a first-class capability", async () => {
  const calls = [];
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({
      capabilities: {
        ...capabilities,
        getLatestProducts: async (input) => {
          calls.push(input);
          return [{ id: 8 }];
        },
      },
    }),
  });
  const response = await executor.execute({
    message: "Latest TVs",
    context: {},
  });
  assert.equal(response.responseType, "latest_products");
  assert.equal(calls[0].domain, "tv");
});

test("distinguishes News brand and product intents", () => {
  const brand = parseAgentRequest({ message: "Show Samsung news" });
  const product = parseAgentRequest({ message: "Show news about Galaxy S25" });
  const article = parseAgentRequest({
    message: "Read this news article",
    context: { domain: "news" },
  });
  assert.equal(brand.intent, "brand_news");
  assert.equal(product.intent, "product_news");
  assert.equal(article.intent, "article_request");
});

test("passes a resolved product constraint to product news", async () => {
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({ capabilities }),
  });
  const response = await executor.execute({
    message: "Show news about Galaxy S25",
    context: {},
  });
  assert.equal(response.tool, "getProductNews");
  assert.equal(response.items[0].input.product, "Galaxy S25");
});

test("executes declared product search and spec-score tools", async () => {
  const executor = createAgentExecutor({
    toolExecutor: createAgentToolExecutor({ capabilities }),
  });
  const search = await executor.execute({
    message: "Search for Galaxy S25 phones",
    context: {},
  });
  const score = await executor.execute({
    message: "What is the spec score of Galaxy S25?",
    context: {},
  });
  assert.equal(search.tool, "searchSmartphones");
  assert.equal(search.items[0].input.query, "Search for Galaxy S25 phones");
  assert.equal(score.tool, "getSmartphoneSpecScore");
  assert.equal(score.items[0].input.names[0], "Galaxy S25");
});

test("extracts expanded canonical filters", () => {
  const result = parseAgentRequest({
    message:
      "Show blue Samsung phones between 20k and 30k with 12GB RAM, AMOLED, 5G and 120Hz",
  });
  assert.equal(result.filters.minPrice, 20000);
  assert.equal(result.filters.maxPrice, 30000);
  assert.equal(result.filters.color, "blue");
  assert.equal(result.filters.ram, "12GB");
  assert.equal(result.filters.network, "5G");
  assert.equal(result.filters.display, "AMOLED");
  assert.equal(result.filters.refreshRate, "120");
});

test("recognizes plural TV routes and latest intent", () => {
  const result = parseAgentRequest({ message: "Latest TVs" });
  assert.equal(result.domain, "tv");
  assert.equal(result.intent, "latest");
  assert.equal(result.tool, "getLatestTVs");
});

test("routes ambiguous product launches using explicit product domain", () => {
  const result = parseAgentRequest({
    message: "Show the latest TV launches",
    context: { domain: "tv" },
  });
  assert.equal(result.domain, "tv");
  assert.equal(result.tool, "getLatestTVs");
});
