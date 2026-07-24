const crypto = require("node:crypto");
const zlib = require("node:zlib");
const { promisify } = require("node:util");

const brotliCompressAsync = promisify(zlib.brotliCompress);
const gzipAsync = promisify(zlib.gzip);

const BROTLI_MIN_RESPONSE_BYTES = Math.max(
  256,
  Number(process.env.BROTLI_MIN_RESPONSE_BYTES) || 1024,
);
const STABLE_SPEC_CACHE_CONTROL =
  process.env.STABLE_SPEC_CACHE_CONTROL ||
  "public, max-age=3600, s-maxage=86400, stale-while-revalidate=604800, stale-if-error=604800";
const SEMI_STATIC_CACHE_CONTROL =
  process.env.SEMI_STATIC_CACHE_CONTROL ||
  "public, max-age=60, s-maxage=300, stale-while-revalidate=600";

const parseAcceptedEncodings = (headerValue) => {
  const accepted = new Map();
  for (const entry of String(headerValue || "").split(",")) {
    const [rawName, ...parameters] = entry.trim().toLowerCase().split(";");
    if (!rawName) continue;

    let quality = 1;
    for (const parameter of parameters) {
      const match = parameter.trim().match(/^q=([0-9.]+)$/);
      if (!match) continue;
      const parsed = Number(match[1]);
      quality = Number.isFinite(parsed) ? Math.max(0, Math.min(1, parsed)) : 0;
    }
    accepted.set(rawName, quality);
  }
  return accepted;
};

const resolveAcceptedEncoding = (headerValue) => {
  const accepted = parseAcceptedEncodings(headerValue);
  if (!accepted.size) return "identity";

  const wildcardQuality = accepted.get("*") ?? 0;
  const brotliQuality = accepted.get("br") ?? wildcardQuality;
  const gzipQuality = accepted.get("gzip") ?? wildcardQuality;

  if (brotliQuality > 0 && brotliQuality >= gzipQuality) return "br";
  if (gzipQuality > 0) return "gzip";
  return "identity";
};

const requestMatchesEtag = (req, etag) =>
  String(req.headers["if-none-match"] || "")
    .split(",")
    .map((value) => value.trim())
    .some((value) => value === "*" || value === etag);

const sendNegotiatedJson = async (
  req,
  res,
  payload,
  {
    brotliQuality = 6,
    cacheControl = "",
    threshold = BROTLI_MIN_RESPONSE_BYTES,
  } = {},
) => {
  const source = Buffer.from(JSON.stringify(payload));
  const quality = Math.max(0, Math.min(11, Number(brotliQuality) || 6));
  let encoding =
    source.length >= threshold
      ? resolveAcceptedEncoding(req.headers["accept-encoding"])
      : "identity";
  let body = source;

  try {
    if (encoding === "br") {
      body = await brotliCompressAsync(source, {
        params: {
          [zlib.constants.BROTLI_PARAM_MODE]:
            zlib.constants.BROTLI_MODE_TEXT,
          [zlib.constants.BROTLI_PARAM_QUALITY]: quality,
          [zlib.constants.BROTLI_PARAM_SIZE_HINT]: source.length,
        },
      });
    } else if (encoding === "gzip") {
      body = await gzipAsync(source, {
        level: zlib.constants.Z_DEFAULT_COMPRESSION,
      });
    }
  } catch (error) {
    console.warn("Response compression failed; sending identity JSON:", error);
    encoding = "identity";
    body = source;
  }

  const etag = `W/"${crypto
    .createHash("sha256")
    .update(source)
    .digest("base64url")}"`;

  res.set("Content-Type", "application/json; charset=utf-8");
  res.set("ETag", etag);
  res.vary("Accept-Encoding");
  if (cacheControl) res.set("Cache-Control", cacheControl);

  if (requestMatchesEtag(req, etag)) {
    res.removeHeader("Content-Encoding");
    res.removeHeader("Content-Length");
    return res.status(304).end();
  }

  if (encoding !== "identity") {
    res.set("Content-Encoding", encoding);
  }
  res.set("Content-Length", String(body.length));

  if (req.method === "HEAD") return res.end();
  return res.end(body);
};

const createApiJsonCompressionMiddleware =
  ({
    brotliQuality = 6,
    threshold = BROTLI_MIN_RESPONSE_BYTES,
  } = {}) =>
  (req, res, next) => {
    const originalJson = res.json.bind(res);
    let responseStarted = false;

    res.json = function compressedJson(payload) {
      if (responseStarted || res.headersSent) {
        return originalJson(payload);
      }
      responseStarted = true;

      void sendNegotiatedJson(req, res, payload, {
        brotliQuality,
        threshold,
      }).catch((error) => {
        res.json = originalJson;
        if (!res.headersSent) {
          next(error);
          return;
        }
        if (typeof res.destroy === "function" && !res.destroyed) {
          res.destroy(error);
        }
      });

      return res;
    };

    next();
  };

module.exports = {
  BROTLI_MIN_RESPONSE_BYTES,
  SEMI_STATIC_CACHE_CONTROL,
  STABLE_SPEC_CACHE_CONTROL,
  createApiJsonCompressionMiddleware,
  parseAcceptedEncodings,
  resolveAcceptedEncoding,
  sendNegotiatedJson,
};
