const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const express = require("express");

const { createBase64JsonMiddleware } = require("../base64Response");
const {
  createApiJsonCompressionMiddleware,
} = require("../responseCompression");

const startTestServer = async (base64Enabled) => {
  const app = express();
  const isPublicApi = (req) =>
    req.path.startsWith("/api/public/") || req.path.startsWith("/api/gateway/");
  const base64Middleware = createBase64JsonMiddleware({
    enabled: base64Enabled,
    shouldEncode: isPublicApi,
  });
  const compressionMiddleware = createApiJsonCompressionMiddleware({
    threshold: Number.MAX_SAFE_INTEGER,
  });

  app.use((req, res, next) => {
    if (!isPublicApi(req)) return next();
    compressionMiddleware(req, res, next);
  });
  app.use((req, res, next) => {
    if (!isPublicApi(req)) return next();
    base64Middleware(req, res, next);
  });
  app.get("/api/public/example", (_req, res) => {
    res.json({ name: "Redmi 17 5G", offer: "₹23,999" });
  });
  app.get("/api/admin/example", (_req, res) => {
    res.json({ message: "admin response" });
  });

  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    origin: `http://127.0.0.1:${server.address().port}`,
  };
};

test("encodes public responses but leaves admin responses as JSON", async () => {
  const { server, origin } = await startTestServer(true);
  try {
    const publicResponse = await fetch(`${origin}/api/public/example`, {
      headers: { "accept-encoding": "identity" },
    });
    const publicPayload = await publicResponse.json();
    assert.equal(publicPayload.v, 1);
    assert.deepEqual(
      JSON.parse(Buffer.from(publicPayload.d, "base64").toString("utf8")),
      { name: "Redmi 17 5G", offer: "₹23,999" },
    );

    const adminResponse = await fetch(`${origin}/api/admin/example`, {
      headers: { "accept-encoding": "identity" },
    });
    assert.deepEqual(await adminResponse.json(), { message: "admin response" });
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});

test("returns normal public JSON when Base64 is disabled", async () => {
  const { server, origin } = await startTestServer(false);
  try {
    const response = await fetch(`${origin}/api/public/example`, {
      headers: { "accept-encoding": "identity" },
    });
    assert.deepEqual(await response.json(), {
      name: "Redmi 17 5G",
      offer: "₹23,999",
    });
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});
