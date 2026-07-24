const test = require("node:test");
const assert = require("node:assert/strict");
const zlib = require("node:zlib");

const {
  createApiJsonCompressionMiddleware,
  resolveAcceptedEncoding,
  sendNegotiatedJson,
} = require("../responseCompression");

const createRequest = (headers = {}, method = "GET") => ({
  headers,
  method,
});

const createResponse = () => {
  const headers = new Map();
  let resolveEnded;
  const ended = new Promise((resolve) => {
    resolveEnded = resolve;
  });

  return {
    body: Buffer.alloc(0),
    ended,
    headers,
    headersSent: false,
    statusCode: 200,
    set(name, value) {
      headers.set(String(name).toLowerCase(), String(value));
      return this;
    },
    vary(name) {
      const current = headers.get("vary");
      const values = new Set(
        String(current || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
      );
      values.add(name);
      headers.set("vary", Array.from(values).join(", "));
      return this;
    },
    removeHeader(name) {
      headers.delete(String(name).toLowerCase());
      return this;
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    end(body) {
      this.body = body ? Buffer.from(body) : Buffer.alloc(0);
      this.headersSent = true;
      resolveEnded();
      return this;
    },
    json(value) {
      this.set("Content-Type", "application/json; charset=utf-8");
      this.end(JSON.stringify(value));
      return this;
    },
  };
};

const payload = {
  name: "Example Phone",
  display: {
    size: "6.7 inch",
    features: Array.from({ length: 100 }, () => "AMOLED 120Hz"),
  },
  camera: {
    features: Array.from({ length: 100 }, () => "50 MP optical stabilization"),
  },
};

test("prefers Brotli when the client accepts Brotli and gzip equally", () => {
  assert.equal(resolveAcceptedEncoding("gzip, deflate, br"), "br");
});

test("sends and round-trips a Brotli-compressed JSON response", async () => {
  const request = createRequest({ "accept-encoding": "gzip, br" });
  const response = createResponse();

  await sendNegotiatedJson(request, response, payload, {
    cacheControl: "public, max-age=3600",
    threshold: 256,
  });

  assert.equal(response.headers.get("content-encoding"), "br");
  assert.equal(response.headers.get("vary"), "Accept-Encoding");
  assert.equal(
    response.headers.get("cache-control"),
    "public, max-age=3600",
  );
  assert.deepEqual(
    JSON.parse(zlib.brotliDecompressSync(response.body).toString()),
    payload,
  );
});

test("falls back to gzip when Brotli is unavailable", async () => {
  const request = createRequest({ "accept-encoding": "gzip" });
  const response = createResponse();

  await sendNegotiatedJson(request, response, payload, { threshold: 256 });

  assert.equal(response.headers.get("content-encoding"), "gzip");
  assert.deepEqual(
    JSON.parse(zlib.gunzipSync(response.body).toString()),
    payload,
  );
});

test("sends identity JSON when no supported compression is accepted", async () => {
  const request = createRequest({ "accept-encoding": "identity" });
  const response = createResponse();

  await sendNegotiatedJson(request, response, payload, { threshold: 256 });

  assert.equal(response.headers.has("content-encoding"), false);
  assert.deepEqual(JSON.parse(response.body.toString()), payload);
});

test("returns 304 for a matching ETag without a response body", async () => {
  const firstResponse = createResponse();
  await sendNegotiatedJson(createRequest(), firstResponse, payload);

  const response = createResponse();
  await sendNegotiatedJson(
    createRequest({ "if-none-match": firstResponse.headers.get("etag") }),
    response,
    payload,
  );

  assert.equal(response.statusCode, 304);
  assert.equal(response.body.length, 0);
  assert.equal(response.headers.has("content-encoding"), false);
  assert.equal(response.headers.has("content-length"), false);
});

test("global API middleware compresses an ordinary res.json response", async () => {
  const request = createRequest({ "accept-encoding": "br, gzip" });
  const response = createResponse();
  const middleware = createApiJsonCompressionMiddleware({ threshold: 256 });
  let nextError = null;

  middleware(request, response, (error) => {
    nextError = error || null;
  });
  const returnedResponse = response.json(payload);
  await response.ended;

  assert.equal(nextError, null);
  assert.equal(returnedResponse, response);
  assert.equal(response.headers.get("content-encoding"), "br");
  assert.deepEqual(
    JSON.parse(zlib.brotliDecompressSync(response.body).toString()),
    payload,
  );
});

test("global API middleware leaves small JSON responses uncompressed", async () => {
  const request = createRequest({ "accept-encoding": "br, gzip" });
  const response = createResponse();
  const middleware = createApiJsonCompressionMiddleware({ threshold: 1024 });

  middleware(request, response, () => {});
  response.json({ ok: true });
  await response.ended;

  assert.equal(response.headers.has("content-encoding"), false);
  assert.deepEqual(JSON.parse(response.body.toString()), { ok: true });
});
