// index _fixed.js
require("dotenv").config();
const path = require("path");
const express = require("express");
const cors = require("cors");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const crypto = require("crypto");
const {
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse,
} = require("@simplewebauthn/server");
const ExcelJS = require("exceljs");
const rateLimit = require("express-rate-limit");
const { client, db } = require("./db");
const multer = require("multer");
const {
  sendRegistrationMail,
  sendAdminOrganizationPinOtpEmail,
  sendCareerApplicationEmail,
  sendCareerAssignmentEmail,
  sendCareerInterviewEmail,
  sendCareerHrEmail,
  sendCareerOfferEmail,
} = require("./utils/mailer");
const { authenticateCustomer, authenticate } = require("./middleware/auth");
const {
  recomputeProductDynamicScoreSmartphones,
  recomputeProductDynamicScoreLaptops,
  recomputeProductDynamicScoreTVs,
} = require("./utils/hookScore");
const { recomputeProductTrendingScores } = require("./utils/trendingScore");
const {
  normalizeCompareScoreConfig,
  buildCompareRanking,
  weightsToPercent,
} = require("./utils/compareScoring");
const {
  computeTvRawSpecScoreV2,
} = require("./utils/tvSpecScore");
const {
  computeLaptopRawSpecScoreV2,
} = require("./utils/laptopSpecScore");
const {
  recomputeSmartphoneCompetitorAnalysis,
} = require("./utils/competitorAnalysis");
const {
  normalizeText: normalizeComparePageText,
  toFiniteNumber: toComparePageFiniteNumber,
  toSlug: toComparePageSlug,
  joinNamesWithoutCommas,
  resolveSmartphoneSegmentLabel,
  buildComparePageSlug,
  buildComparePageTitle,
  buildComparePageDescription,
  buildAutomaticComparePageTitle,
  buildAutomaticComparePageDescription,
} = require("./utils/comparePageSeo");
const {
  cleanText,
  cleanToken,
  getSearchPopularityDevices,
  normalizeProductType,
  normalizeSearchQuery,
  resolveSearchInterestProduct,
} = require("./utils/searchPopularity");
const { ensureProperHtmlEncoding } = require("./utils/htmlDecoder");
const {
  normalizeRole,
  RBAC_MODULES,
  ROLE_PRESETS,
  getPermissionMatrix,
  getAllPermissionCodes,
  getRolePreset,
  getDefaultPermissionsForRole,
  permissionMatches,
  hasPermissionSet,
  hasAnyPermissionSet,
  hasAllPermissionsSet,
  getModulePermissionCode,
  getModuleLabel,
  getModuleActions,
  isActionSupported,
  normalizePermissionToken,
} = require("./utils/rbac");
const {
  NEWS_PUSH_TOPIC,
  isFirebaseAdminConfigured,
  sendPublishedNewsPush,
  subscribeTokenToTopic,
  unsubscribeTokenFromTopic,
} = require("./utils/newsPush");
const helmet = require("helmet");
const xss = require("xss-clean");
const { clean: xssClean } = require("xss-clean/lib/xss");
const compression = require("compression");

const SECRET = process.env.JWT_SECRET || "smartarena_secret_key_25";
const PORT = process.env.PORT || 5000;

const app = express();

app.set("trust proxy", 1);

const ALLOWED_ORIGINS = new Set([
  "http://localhost:3000",
  "http://localhost:5173",
  "https://main.d8c9hzzm0g9ux.amplifyapp.com",
  "https://www.tryhook.shop",
  "https://tryhook.shop",
]);

app.use(
  cors({
    origin(origin, callback) {
      // Allow non-browser clients (no Origin header) and explicitly allow known web origins.
      if (!origin) return callback(null, true);
      const normalizedOrigin = String(origin).replace(/\/$/, "");
      if (ALLOWED_ORIGINS.has(normalizedOrigin)) return callback(null, true);
      return callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

// Security middlewares - Enhanced with explicit HSTS
app.disable("x-powered-by");

// Enable gzip compression for all responses (reduces file size by 70-80%)

app.use(
  helmet({
    hsts: {
      maxAge: 31536000, // 1 year in seconds
      includeSubDomains: true,
      preload: true,
    },
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'", "fonts.googleapis.com"],
        scriptSrc: [
          "'self'",
          "www.googletagmanager.com",
          "pagead2.googlesyndication.com",
        ],
        fontSrc: ["'self'", "fonts.gstatic.com"],
        imgSrc: ["'self'", "data:", "https:"],
        connectSrc: ["'self'", "https:"],
      },
    },
  }),
);
// Limit JSON body size to mitigate large payload abuse
app.use(express.json({ limit: "10kb" }));
// Parse URL-encoded bodies (for form submissions)
app.use(express.urlencoded({ extended: true }));

// ===== URL CANONICALIZATION MIDDLEWARE =====
// Enforce canonical URL structure: https://tryhook.shop (non-www)
// Only do essential redirects to avoid redirect chains
app.use((req, res, next) => {
  // Only redirect www to non-www if explicitly requested
  // Skip for API endpoints and direct https traffic
  if (req.hostname === "www.tryhook.shop") {
    const newUrl = `https://tryhook.shop${req.originalUrl}`;
    return res.redirect(301, newUrl);
  }

  // Let React Router handle trailing slashes and path normalization
  // Avoid redirect chains
  next();
});

const API_ALIAS_REWRITE_RULES = [
  { alias: /^\/api\/gateway\/catalog\/handset$/i, target: "/api/smartphones" },
  {
    alias: /^\/api\/gateway\/catalog\/network-grid$/i,
    target: "/api/networking",
  },
  { alias: /^\/api\/gateway\/catalog\/compute$/i, target: "/api/laptops" },
  { alias: /^\/api\/gateway\/catalog\/vision$/i, target: "/api/tvs" },
  { alias: /^\/api\/gateway\/meta\/label$/i, target: "/api/brand" },
  { alias: /^\/api\/gateway\/meta\/group$/i, target: "/api/category" },
  {
    alias: /^\/api\/gateway\/channel\/stores$/i,
    target: "/api/public/online-stores",
  },
  {
    alias: /^\/api\/gateway\/insight\/features$/i,
    target: "/api/public/popular-features",
  },
  {
    alias: /^\/api\/gateway\/pulse\/handset$/i,
    target: "/api/public/trending/smartphones",
  },
  {
    alias: /^\/api\/gateway\/pulse\/compute$/i,
    target: "/api/public/trending/laptops",
  },
  {
    alias: /^\/api\/gateway\/pulse\/vision$/i,
    target: "/api/public/trending/tvs",
  },
  {
    alias: /^\/api\/gateway\/pulse\/network-grid$/i,
    target: "/api/public/trending/networking",
  },
  {
    alias: /^\/api\/gateway\/pulse\/duel$/i,
    target: "/api/public/trending/most-compared",
  },
  {
    alias: /^\/api\/gateway\/pulse\/all$/i,
    target: "/api/public/trending/all",
  },
  {
    alias: /^\/api\/gateway\/release\/handset$/i,
    target: "/api/public/new/smartphones",
  },
  {
    alias: /^\/api\/gateway\/release\/compute$/i,
    target: "/api/public/new/laptops",
  },
  {
    alias: /^\/api\/gateway\/release\/vision$/i,
    target: "/api/public/new/tvs",
  },
  {
    alias: /^\/api\/gateway\/release\/network-grid$/i,
    target: "/api/public/new/networking",
  },
  {
    alias: /^\/api\/gateway\/event\/feature$/i,
    target: "/api/public/feature-click",
  },
  { alias: /^\/api\/gateway\/compare\/log$/i, target: "/api/public/compare" },
  {
    alias: /^\/api\/gateway\/compare\/score$/i,
    target: "/api/public/compare/scores",
  },
  {
    alias: /^\/api\/gateway\/compare\/resolve$/i,
    target: "/api/public/compare/resolve",
  },
  { alias: /^\/api\/gateway\/query\/finder$/i, target: "/api/search" },
  { alias: /^\/api\/gateway\/journal\/posts$/i, target: "/api/public/blogs" },
  {
    alias: /^\/api\/gateway\/journal\/posts\/([^/]+)$/i,
    target: (m) => `/api/public/blogs/${m[1]}`,
  },
  {
    alias: /^\/api\/gateway\/node\/([^/]+)$/i,
    target: (m) => `/api/public/product/${m[1]}`,
  },
  {
    alias: /^\/api\/gateway\/node\/([^/]+)\/hit$/i,
    target: (m) => `/api/public/product/${m[1]}/view`,
  },
  {
    alias: /^\/api\/gateway\/node\/([^/]+)\/discovery$/i,
    target: (m) => `/api/public/product/${m[1]}/discovery`,
  },
  {
    alias: /^\/api\/gateway\/node\/([^/]+)\/peers$/i,
    target: (m) => `/api/public/product/${m[1]}/competitors`,
  },
  {
    alias: /^\/api\/gateway\/node\/([^/]+)\/reviews$/i,
    target: (m) => `/api/public/products/${m[1]}/ratings`,
  },
];

app.use((req, _res, next) => {
  try {
    const pathName = req.path || "";
    for (const rule of API_ALIAS_REWRITE_RULES) {
      const match = pathName.match(rule.alias);
      if (!match) continue;

      const rewritten =
        typeof rule.target === "function" ? rule.target(match) : rule.target;
      if (!rewritten) break;

      const qIndex = req.url.indexOf("?");
      const query = qIndex >= 0 ? req.url.slice(qIndex) : "";
      req.url = `${rewritten}${query}`;
      break;
    }
  } catch {
    // ignore alias rewrite failures
  }
  next();
});
// Apply XSS sanitization after body and query parsers. Use the underlying
// `clean` function and avoid reassigning `req.query` if it's getter-only.
app.use(function xssSafe(req, res, next) {
  try {
    if (req.body) req.body = xssClean(req.body);
  } catch (err) {
    // ignore malformed body sanitization errors
  }

  if (req.query && typeof req.query === "object") {
    try {
      // try to replace whole query object
      req.query = xssClean(req.query);
    } catch (err) {
      // if replacing fails (getter-only), sanitize in-place
      try {
        const cleaned = xssClean(req.query);
        for (const k of Object.keys(cleaned)) {
          try {
            req.query[k] = cleaned[k];
          } catch (e) {
            /* skip */
          }
        }
      } catch (e) {
        // give up silently
      }
    }
  }

  try {
    if (req.params) req.params = xssClean(req.params);
  } catch (err) {
    // ignore
  }

  next();
});

// ===== STATIC FILE SERVING FOR REACT SPA =====
// Serve built React app from client/dist directory
const distPath = path.join(__dirname, "../client/dist");
const HASHED_STATIC_ASSET_PATTERN = /-[A-Za-z0-9_-]{8,}\.[^./\\]+$/i;

const applyNoCacheHtmlHeaders = (res) => {
  res.set("Cache-Control", "no-cache, no-store, must-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
};

const isHtmlFilePath = (filePath = "") =>
  path.extname(String(filePath || "")).toLowerCase() === ".html";

const isHashedStaticAsset = (filePath = "") =>
  HASHED_STATIC_ASSET_PATTERN.test(path.basename(String(filePath || "")));

const isDirectFileRequest = (requestPath = "") =>
  Boolean(path.extname(String(requestPath || "")));

app.use(
  express.static(distPath, {
    // Set proper cache control headers for static assets
    setHeaders: (res, filePath) => {
      if (isHtmlFilePath(filePath)) {
        // HTML must be revalidated so it does not point at stale hashed bundles.
        applyNoCacheHtmlHeaders(res);
      }
      // Cache static assets with hashes for 1 year (they won't change)
      else if (isHashedStaticAsset(filePath)) {
        res.set("Cache-Control", "public, max-age=31536000, immutable");
      }
      // Cache other non-hashed static files for 1 hour to allow updates.
      else {
        res.set("Cache-Control", "public, max-age=3600, must-revalidate");
      }
      // Ensure proper MIME types for JavaScript modules
      if (filePath.endsWith(".js") || filePath.endsWith(".mjs")) {
        res.set("Content-Type", "application/javascript; charset=utf-8");
      }
    },
  }),
);

// Global rate limiting is not enabled, but targeted auth limits are applied below.

// important for preflight

const upload = multer({ storage: multer.memoryStorage() });

/* -----------------------
  Utilities
------------------------*/
function formatDateForExcel(input) {
  if (!input) return "";
  const d = new Date(input);
  if (isNaN(d)) return "";
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  return `${day} ${month} ${year}`;
}

const LAUNCH_STATUS_VALUES = new Set([
  "rumored",
  "announced",
  "upcoming",
  "released",
  "available",
  "preorder",
]);
function normalizeLaunchStatusOverride(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return null;
  if (/rumou?r/.test(raw)) return "rumored";
  if (/announce/.test(raw)) return "announced";
  if (/(pre[-\s]?order|pre[-\s]?book|prebooking|presale)/i.test(raw))
    return "upcoming";
  if (/(upcoming|coming soon|expected|launching soon)/i.test(raw))
    return "upcoming";
  if (/(available|on sale|in stock)/i.test(raw)) return "available";
  if (/(released|launched|out now)/i.test(raw)) return "released";
  if (!LAUNCH_STATUS_VALUES.has(raw)) return null;
  return raw === "preorder" ? "upcoming" : raw;
}

const resolveSmartphoneLaunchStage = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!device) return null;
  const explicitStatus = normalizeLaunchStatusOverride(
    device.launch_status_override ||
      device.launchStatusOverride ||
      device.launch_status ||
      device.launchStatus,
  );
  const saleStartDirect = normalizeDateOnlyInput(
    device.sale_start_date ??
      device.saleStartDate ??
      device.sale_date ??
      device.saleDate ??
      null,
  );
  const saleStart =
    saleStartDirect ||
    getEarliestSaleStartDateFromVariants(device.variants || []);
  const launchDate = normalizeDateOnlyInput(
    device.launch_date || device.launchDate || null,
  );

  if (explicitStatus === "rumored" || explicitStatus === "announced") {
    return explicitStatus;
  }

  if (explicitStatus === "released") {
    return "released";
  }

  if (explicitStatus === "available") {
    if (saleStart) {
      if (todayIndia && saleStart > todayIndia) return "upcoming";
      return "available";
    }
    return "available";
  }

  if (explicitStatus === "upcoming") {
    if (saleStart) {
      return todayIndia && saleStart > todayIndia ? "upcoming" : "released";
    }
    if (launchDate) {
      return todayIndia && launchDate > todayIndia ? "upcoming" : "released";
    }
    return "released";
  }

  const statusHint = normalizeLaunchStatusOverride(
    device.status || device.availability || device.badge || device.status_text,
  );
  if (statusHint) {
    if (statusHint === "released") return "released";
    if (statusHint === "available") {
      if (saleStart) {
        if (todayIndia && saleStart > todayIndia) return "upcoming";
        return "available";
      }
      return "available";
    }
    if (statusHint === "upcoming") {
      if (saleStart) {
        return todayIndia && saleStart > todayIndia ? "upcoming" : "released";
      }
      if (launchDate) {
        return todayIndia && launchDate > todayIndia ? "upcoming" : "released";
      }
      return "released";
    }
    return statusHint;
  }

  if (saleStart) {
    if (todayIndia && saleStart <= todayIndia) return "available";
    return "upcoming";
  }

  if (launchDate) {
    if (todayIndia && launchDate > todayIndia) return "upcoming";
    return "released";
  }

  return "released";
};

const SMARTPHONE_COMPARE_LIMIT_DEFAULT = 4;
const SMARTPHONE_COMPETITOR_LIMIT_DEFAULT = 5;

const parseMarketPriceValue = (value) => {
  if (value == null || value === "") return null;
  const normalized = Number(String(value).replace(/[^0-9.]/g, ""));
  return Number.isFinite(normalized) && normalized > 0 ? normalized : null;
};

const readArrayValue = (value) => {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const hasStoreMarketSignal = (store) => {
  if (!store || typeof store !== "object") return false;
  return Boolean(
    parseMarketPriceValue(
      store.price ??
        store.current_price ??
        store.sale_price ??
        store.offer_price ??
        store.mrp,
    ) ||
    store.url ||
    store.store ||
    store.store_name ||
    store.storeName ||
    store.display_store_name ||
    store.sale_start_date ||
    store.saleStartDate ||
    store.sale_date ||
    store.saleDate ||
    store.available_from ||
    store.availableFrom,
  );
};

const hasSpecScoreMarketSignal = (item) => {
  if (!item || typeof item !== "object") return false;

  if (
    normalizeDateOnlyInput(
      item.sale_start_date ??
        item.saleStartDate ??
        item.sale_date ??
        item.saleDate ??
        null,
    )
  ) {
    return true;
  }

  if (
    parseMarketPriceValue(
      item.price ??
        item.current_price ??
        item.launch_price ??
        item.starting_price ??
        item.price_in_india ??
        item.expected_price,
    )
  ) {
    return true;
  }

  const directStores = readArrayValue(item.store_prices ?? item.storePrices);
  if (directStores.some(hasStoreMarketSignal)) return true;

  const variants = readArrayValue(item.variants);
  return variants.some((variant) => {
    if (!variant || typeof variant !== "object") return false;
    if (
      normalizeDateOnlyInput(
        variant.sale_start_date ??
          variant.saleStartDate ??
          variant.sale_date ??
          variant.saleDate ??
          null,
      )
    ) {
      return true;
    }
    if (
      parseMarketPriceValue(
        variant.price ??
          variant.current_price ??
          variant.launch_price ??
          variant.starting_price ??
          variant.expected_price,
      )
    ) {
      return true;
    }
    return readArrayValue(variant.store_prices ?? variant.storePrices).some(
      hasStoreMarketSignal,
    );
  });
};

const resolveSmartphoneLaunchPolicy = (launchStage, item = null) => {
  const stage = normalizeLaunchStatusOverride(launchStage) || "released";
  const hasMarketSignal = hasSpecScoreMarketSignal(item);
  const base = {
    allow_compare: true,
    allow_competitors: true,
    compare_limit: SMARTPHONE_COMPARE_LIMIT_DEFAULT,
    competitor_limit: SMARTPHONE_COMPETITOR_LIMIT_DEFAULT,
    allow_spec_score: true,
  };

  if (stage === "rumored") {
    return {
      ...base,
      allow_compare: false,
      allow_competitors: false,
      compare_limit: 0,
      competitor_limit: 0,
      allow_spec_score: false,
    };
  }

  if (stage === "announced") {
    return {
      ...base,
      compare_limit: 2,
      competitor_limit: 2,
      allow_spec_score: hasMarketSignal,
    };
  }

  if (stage === "upcoming") {
    return {
      ...base,
      allow_spec_score: hasMarketSignal,
    };
  }

  return base;
};

const applySmartphoneLaunchPolicy = (item, launchStage) => {
  if (!item) return item;
  const policy = resolveSmartphoneLaunchPolicy(launchStage, item);
  item.allow_compare = policy.allow_compare;
  item.allowCompare = policy.allow_compare;
  item.allow_competitors = policy.allow_competitors;
  item.allowCompetitors = policy.allow_competitors;
  item.compare_limit = policy.compare_limit;
  item.compareLimit = policy.compare_limit;
  item.competitor_limit = policy.competitor_limit;
  item.competitorLimit = policy.competitor_limit;
  item.allow_spec_score = policy.allow_spec_score;
  item.allowSpecScore = policy.allow_spec_score;
  return item;
};

function parseDateForImport(val) {
  if (!val && val !== 0) return null;
  if (val instanceof Date) {
    if (isNaN(val)) return null;
    return val.toISOString();
  }
  const s = String(val).trim();
  if (!s) return null;
  // Accept YYYY-MM-DD or ISO or common formats
  // Try ISO first
  const iso = new Date(s);
  if (!isNaN(iso)) return iso.toISOString();
  // fallback to dd/mm/yyyy or dd-mm-yyyy
  const parts = s.match(/^(\d{1,2})[\s\/\-](\d{1,2})[\s\/\-](\d{4})$/);
  if (parts) {
    const day = Number(parts[1]);
    const month = Number(parts[2]) - 1;
    const year = Number(parts[3]);
    const d = new Date(Date.UTC(year, month, day));
    if (!isNaN(d)) return d.toISOString();
  }
  return null;
}

function normalizeBodyKeys(obj) {
  const out = {};
  if (!obj || typeof obj !== "object") return out;
  for (const k of Object.keys(obj)) {
    const nk = k.toLowerCase().replace(/[^a-z0-9]/g, "");
    out[nk] = obj[k];
  }
  return out;
}

const LAPTOP_REQUIRED_SECTION_KEYS = [
  "basic_info_json",
  "build_design_json",
  "display_json",
  "performance_json",
  "memory_json",
  "storage_json",
  "battery_json",
  "connectivity_json",
  "ports_json",
  "multimedia_json",
  "software_json",
  "security_json",
  "physical_json",
  "camera_json",
  "warranty_json",
  "environmental_json",
  "in_the_box_json",
  "import_details_json",
];

const LAPTOP_SCORE_SECTION_KEYS = new Set([
  "build_design_json",
  "display_json",
  "performance_json",
  "memory_json",
  "storage_json",
  "battery_json",
  "connectivity_json",
  "ports_json",
  "multimedia_json",
  "software_json",
  "security_json",
  "physical_json",
  "camera_json",
  "warranty_json",
  "environmental_json",
]);

const LAPTOP_CANONICAL_SECTION_TO_JSON = {
  basic_info: "basic_info_json",
  performance: "performance_json",
  display: "display_json",
  memory: "memory_json",
  storage: "storage_json",
  battery: "battery_json",
  multimedia: "multimedia_json",
  ports: "ports_json",
  camera: "camera_json",
  security: "security_json",
  physical: "physical_json",
  software: "software_json",
};

const toPlainObject = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
};

const INDIA_TIME_ZONE = "Asia/Kolkata";

const getIndiaDateOnly = (value = new Date()) => {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;

  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: INDIA_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const tokens = {};
  for (const part of parts) {
    if (part.type !== "literal") tokens[part.type] = part.value;
  }

  if (!tokens.year || !tokens.month || !tokens.day) return null;
  return `${tokens.year}-${tokens.month}-${tokens.day}`;
};

const normalizeDateOnlyInput = (value) => {
  if (value === undefined || value === null || value === "") return null;

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;

    const isoMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (isoMatch) {
      return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
    }

    const dmyMatch = trimmed.match(
      /^(\d{1,2})[\s\/\-](\d{1,2})[\s\/\-](\d{4})$/,
    );
    if (dmyMatch) {
      const day = String(Number(dmyMatch[1])).padStart(2, "0");
      const month = String(Number(dmyMatch[2])).padStart(2, "0");
      const year = dmyMatch[3];
      return `${year}-${month}-${day}`;
    }
  }

  const parsed = parseDateForImport(value);
  return parsed ? String(parsed).slice(0, 10) : null;
};

const toDateOnlyUtcMillis = (value) => {
  const normalized = normalizeDateOnlyInput(value);
  if (!normalized) return null;

  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;

  return Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
};

const diffDateOnlyDays = (fromValue, toValue) => {
  const fromUtc = toDateOnlyUtcMillis(fromValue);
  const toUtc = toDateOnlyUtcMillis(toValue);
  if (!Number.isFinite(fromUtc) || !Number.isFinite(toUtc)) return null;
  return Math.round((toUtc - fromUtc) / 86400000);
};

const toOfferPriceNumber = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const numeric =
    typeof value === "number"
      ? value
      : Number(String(value).replace(/[^0-9.]/g, ""));
  return Number.isFinite(numeric) ? numeric : null;
};

const decorateStorePriceAvailability = (
  storePrice,
  todayIndia = getIndiaDateOnly(),
) => {
  const item = toPlainObject(storePrice);
  const saleStartDate = normalizeDateOnlyInput(
    item.sale_start_date ??
      item.sale_date ??
      item.saleStartDate ??
      item.saleDate ??
      null,
  );
  const isPrebooking = Boolean(
    saleStartDate && todayIndia && saleStartDate > todayIndia,
  );

  return {
    ...item,
    sale_start_date: saleStartDate,
    availability_status: isPrebooking ? "prebooking" : "live",
    is_prebooking: isPrebooking,
    is_live: !isPrebooking,
    cta_label: isPrebooking ? "Coming Soon" : "Buy Now",
  };
};

const decorateStorePriceList = (storePrices, todayIndia = getIndiaDateOnly()) =>
  (Array.isArray(storePrices) ? storePrices : []).map((storePrice) =>
    decorateStorePriceAvailability(storePrice, todayIndia),
  );

const getEarliestSaleStartDateFromVariants = (variants) => {
  const dates = [];
  for (const variant of Array.isArray(variants) ? variants : []) {
    const variantObj = toPlainObject(variant);
    const direct = normalizeDateOnlyInput(
      variantObj.sale_start_date ??
        variantObj.saleStartDate ??
        variantObj.sale_date ??
        variantObj.saleDate ??
        null,
    );
    if (direct) dates.push(direct);

    const storePrices = Array.isArray(variantObj.store_prices)
      ? variantObj.store_prices
      : Array.isArray(variantObj.storePrices)
        ? variantObj.storePrices
        : [];
    for (const store of storePrices) {
      const storeDate = normalizeDateOnlyInput(
        store?.sale_start_date ??
          store?.saleStartDate ??
          store?.sale_date ??
          store?.saleDate ??
          store?.available_from ??
          store?.availableFrom ??
          null,
      );
      if (storeDate) dates.push(storeDate);
    }
  }

  if (!dates.length) return null;
  dates.sort();
  return dates[0];
};

const resolveEffectiveSmartphonePrice = (variants, fallbackPrice = null) => {
  const livePrices = [];
  const prebookingPrices = [];
  const basePrices = [];

  for (const variant of Array.isArray(variants) ? variants : []) {
    const variantObj = toPlainObject(variant);
    const basePrice = toOfferPriceNumber(
      variantObj.base_price ?? variantObj.price ?? null,
    );
    if (basePrice !== null) basePrices.push(basePrice);

    const storePrices = decorateStorePriceList(
      variantObj.store_prices ?? variantObj.storePrices ?? [],
    );
    for (const store of storePrices) {
      const price = toOfferPriceNumber(store.price);
      if (price === null) continue;
      if (store.is_prebooking) prebookingPrices.push(price);
      else livePrices.push(price);
    }
  }

  if (livePrices.length) return Math.min(...livePrices);
  if (prebookingPrices.length) return Math.min(...prebookingPrices);
  if (basePrices.length) return Math.min(...basePrices);

  return toOfferPriceNumber(fallbackPrice);
};

const hasOwn = (obj, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

const INTERNAL_SCORE_METADATA_KEYS = new Set([
  "field_profile",
  "fieldprofile",
  "spec_score_source",
  "specscoresource",
  "overall_score_source",
  "overallscoresource",
  "spec_score_v2_source",
  "specscorev2source",
  "overall_score_v2_source",
  "overallscorev2source",
  "spec_score_v2_raw",
  "specscorev2raw",
  "camera_score_v2_raw",
  "camerascorev2raw",
  "spec_score_price",
  "specscoreprice",
  "spec_score_price_band",
  "specscorepriceband",
  "spec_score_feature_coverage",
  "specscorefeaturecoverage",
  "hook_rank_score",
  "hookrankscore",
]);

const isScoreLikeKey = (key) => {
  const normalized = String(key || "").toLowerCase();
  if (normalized === "score") return true;
  if (normalized === "allow_spec_score") return false;
  if (INTERNAL_SCORE_METADATA_KEYS.has(normalized)) return true;

  const isScoreMetadata =
    normalized.includes("spec_score") ||
    normalized.includes("overall_score") ||
    normalized.includes("camera_score");

  if (!isScoreMetadata) return false;

  if (
    normalized.endsWith("_source") ||
    normalized.endsWith("_raw") ||
    normalized.endsWith("_price") ||
    normalized.includes("price_band") ||
    normalized.includes("feature_coverage")
  ) {
    return true;
  }

  return false;
};

const toArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const normalizeNullableText = (value) => {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  return value;
};

const stripScoreKey = (value) => {
  const obj = toPlainObject(value);
  const cleaned = {};
  for (const [key, val] of Object.entries(obj)) {
    if (isScoreLikeKey(key)) continue;
    cleaned[key] = val;
  }
  return cleaned;
};

const mergeSectionObject = (base, incoming, ensureScore = false) => {
  const merged = { ...toPlainObject(base), ...toPlainObject(incoming) };
  if (ensureScore && !hasOwn(merged, "score")) merged.score = null;
  return merged;
};

const ensureSectionShape = (value, ensureScore = false) => {
  const normalized = toPlainObject(value);
  if (ensureScore && !hasOwn(normalized, "score")) normalized.score = null;
  return normalized;
};

const getObjectValue = (payload, directKey, sectionKey, existingValue = {}) => {
  if (hasOwn(payload, directKey)) return toPlainObject(payload[directKey]);
  if (hasOwn(payload, sectionKey)) return stripScoreKey(payload[sectionKey]);
  return toPlainObject(existingValue);
};

const buildLegacyLaptopFromPayload = (payload, existingLaptopRow = {}) => {
  const existingRow = existingLaptopRow || {};
  const multimediaJson = toPlainObject(payload.multimedia_json);

  const features = hasOwn(payload, "features")
    ? toArray(payload.features).filter(Boolean)
    : hasOwn(multimediaJson, "features")
      ? toArray(multimediaJson.features).filter(Boolean)
      : toArray(existingRow.features).filter(Boolean);

  return {
    cpu: getObjectValue(payload, "cpu", "performance_json", existingRow.cpu),
    display: getObjectValue(
      payload,
      "display",
      "display_json",
      existingRow.display,
    ),
    memory: getObjectValue(
      payload,
      "memory",
      "memory_json",
      existingRow.memory,
    ),
    storage: getObjectValue(
      payload,
      "storage",
      "storage_json",
      existingRow.storage,
    ),
    battery: getObjectValue(
      payload,
      "battery",
      "battery_json",
      existingRow.battery,
    ),
    connectivity: getObjectValue(
      payload,
      "connectivity",
      "connectivity_json",
      existingRow.connectivity,
    ),
    physical: getObjectValue(
      payload,
      "physical",
      "physical_json",
      existingRow.physical,
    ),
    software: getObjectValue(
      payload,
      "software",
      "software_json",
      existingRow.software,
    ),
    warranty: getObjectValue(
      payload,
      "warranty",
      "warranty_json",
      existingRow.warranty,
    ),
    features,
  };
};

const normalizeLaptopPayload = (inputLaptop = {}, existingLaptopRow = {}) => {
  const payload = toPlainObject(inputLaptop);
  const existingRow = existingLaptopRow || {};
  const existingMeta = toPlainObject(existingRow.meta);
  const existingSections = toPlainObject(existingRow.spec_sections);
  const canonicalBasicInfo = toPlainObject(payload.basic_info);
  const basicInfoJson = mergeSectionObject(
    toPlainObject(payload.basic_info_json),
    stripScoreKey(canonicalBasicInfo),
  );
  const canonicalMetadata = toPlainObject(payload.metadata);
  const canonicalBuildDesign = toPlainObject(canonicalMetadata.build_design);
  const buildDesignJson = mergeSectionObject(
    toPlainObject(payload.build_design_json),
    stripScoreKey(canonicalBuildDesign),
  );
  const legacyPayload = {
    ...payload,
    cpu: hasOwn(payload, "cpu")
      ? payload.cpu
      : hasOwn(payload, "performance")
        ? payload.performance
        : undefined,
  };
  const legacy = buildLegacyLaptopFromPayload(legacyPayload, existingRow);

  const category = normalizeNullableText(
    hasOwn(payload, "category")
      ? payload.category
      : hasOwn(basicInfoJson, "category")
        ? basicInfoJson.category
        : existingMeta.category,
  );

  const brand = normalizeNullableText(
    hasOwn(payload, "brand")
      ? payload.brand
      : hasOwn(basicInfoJson, "brand")
        ? basicInfoJson.brand
        : hasOwn(basicInfoJson, "brand_name")
          ? basicInfoJson.brand_name
          : existingMeta.brand,
  );

  const model = normalizeNullableText(
    hasOwn(payload, "model")
      ? payload.model
      : hasOwn(basicInfoJson, "model")
        ? basicInfoJson.model
        : existingMeta.model,
  );

  const launchDate = normalizeNullableText(
    hasOwn(payload, "launch_date")
      ? payload.launch_date
      : hasOwn(basicInfoJson, "launch_date")
        ? basicInfoJson.launch_date
        : existingMeta.launch_date,
  );

  const colors = hasOwn(payload, "colors")
    ? toArray(payload.colors)
    : hasOwn(buildDesignJson, "colors")
      ? toArray(buildDesignJson.colors)
      : hasOwn(basicInfoJson, "colors")
        ? toArray(basicInfoJson.colors)
        : toArray(existingMeta.colors);

  const sections = { ...existingSections };

  for (const [key, value] of Object.entries(payload)) {
    if (key.endsWith("_json")) {
      const normalizedValue =
        value && typeof value === "object" && !Array.isArray(value)
          ? stripScoreKey(value)
          : value;
      if (Array.isArray(normalizedValue)) {
        sections[key] = normalizedValue;
        continue;
      }
      if (normalizedValue && typeof normalizedValue === "object") {
        sections[key] = {
          ...toPlainObject(sections[key]),
          ...toPlainObject(normalizedValue),
        };
        continue;
      }
      if (normalizedValue !== undefined) {
        sections[key] = normalizedValue;
      }
      continue;
    }

    const mappedSectionKey = LAPTOP_CANONICAL_SECTION_TO_JSON[key];
    if (mappedSectionKey) {
      const normalizedValue =
        value && typeof value === "object" && !Array.isArray(value)
          ? stripScoreKey(value)
          : value;
      if (Array.isArray(normalizedValue)) {
        sections[mappedSectionKey] = normalizedValue;
        continue;
      }
      if (normalizedValue && typeof normalizedValue === "object") {
        sections[mappedSectionKey] = {
          ...toPlainObject(sections[mappedSectionKey]),
          ...toPlainObject(normalizedValue),
        };
        continue;
      }
      if (normalizedValue !== undefined) {
        sections[mappedSectionKey] = normalizedValue;
      }
      continue;
    }

    if (key === "metadata") continue;
  }

  sections.build_design_json = mergeSectionObject(
    sections.build_design_json,
    stripScoreKey(canonicalBuildDesign),
    true,
  );
  sections.connectivity_json = mergeSectionObject(
    sections.connectivity_json,
    stripScoreKey(canonicalMetadata.connectivity),
    true,
  );
  sections.warranty_json = mergeSectionObject(
    sections.warranty_json,
    stripScoreKey(canonicalMetadata.warranty),
    true,
  );
  sections.environmental_json = mergeSectionObject(
    sections.environmental_json,
    stripScoreKey(canonicalMetadata.environmental),
    true,
  );
  sections.in_the_box_json = mergeSectionObject(
    sections.in_the_box_json,
    stripScoreKey(canonicalMetadata.in_the_box),
  );
  sections.import_details_json = mergeSectionObject(
    sections.import_details_json,
    stripScoreKey(canonicalMetadata.import_details),
  );
  sections.dynamic_json = mergeSectionObject(
    sections.dynamic_json,
    stripScoreKey(canonicalMetadata.dynamic),
  );

  if (Array.isArray(canonicalMetadata.images)) {
    sections.images_json = canonicalMetadata.images;
  }
  if (Array.isArray(canonicalMetadata.variants)) {
    sections.variants_json = canonicalMetadata.variants;
  }

  sections.basic_info_json = mergeSectionObject(sections.basic_info_json, {
    category,
    brand,
    model,
    launch_date: launchDate,
  });
  sections.build_design_json = mergeSectionObject(
    sections.build_design_json,
    { colors },
    true,
  );
  sections.performance_json = mergeSectionObject(
    sections.performance_json,
    legacy.cpu,
    true,
  );
  sections.display_json = mergeSectionObject(
    sections.display_json,
    legacy.display,
    true,
  );
  sections.memory_json = mergeSectionObject(
    sections.memory_json,
    legacy.memory,
    true,
  );
  sections.storage_json = mergeSectionObject(
    sections.storage_json,
    legacy.storage,
    true,
  );
  sections.battery_json = mergeSectionObject(
    sections.battery_json,
    legacy.battery,
    true,
  );
  sections.connectivity_json = mergeSectionObject(
    sections.connectivity_json,
    legacy.connectivity,
    true,
  );
  sections.software_json = mergeSectionObject(
    sections.software_json,
    legacy.software,
    true,
  );
  sections.physical_json = mergeSectionObject(
    sections.physical_json,
    legacy.physical,
    true,
  );
  sections.warranty_json = mergeSectionObject(
    sections.warranty_json,
    legacy.warranty,
    true,
  );
  sections.ports_json = mergeSectionObject(
    sections.ports_json,
    getObjectValue(payload, "ports", "ports_json"),
    true,
  );
  sections.multimedia_json = mergeSectionObject(
    sections.multimedia_json,
    getObjectValue(payload, "multimedia", "multimedia_json"),
    true,
  );
  sections.security_json = mergeSectionObject(
    sections.security_json,
    getObjectValue(payload, "security", "security_json"),
    true,
  );
  sections.camera_json = mergeSectionObject(
    sections.camera_json,
    getObjectValue(payload, "camera", "camera_json"),
    true,
  );

  if (legacy.features.length) {
    sections.multimedia_json = mergeSectionObject(
      sections.multimedia_json,
      { features: legacy.features },
      true,
    );
  }

  const reservedTopLevelKeys = new Set([
    "category",
    "brand",
    "model",
    "launch_date",
    "colors",
    "cpu",
    "display",
    "memory",
    "storage",
    "battery",
    "connectivity",
    "physical",
    "software",
    "features",
    "warranty",
    "ports",
    "multimedia",
    "security",
    "camera",
    "basic_info",
    "performance",
    "metadata",
    "meta",
  ]);

  const dynamicTopLevel = {};
  for (const [key, value] of Object.entries(payload)) {
    if (reservedTopLevelKeys.has(key) || key.endsWith("_json")) continue;
    dynamicTopLevel[key] = value;
  }

  if (Object.keys(dynamicTopLevel).length) {
    sections.dynamic_json = mergeSectionObject(
      sections.dynamic_json,
      dynamicTopLevel,
    );
  }

  for (const sectionKey of LAPTOP_REQUIRED_SECTION_KEYS) {
    sections[sectionKey] = ensureSectionShape(
      sections[sectionKey],
      LAPTOP_SCORE_SECTION_KEYS.has(sectionKey),
    );
  }

  const meta = {
    ...existingMeta,
    category,
    brand,
    model,
    launch_date: launchDate,
    colors,
    spec_schema_version: 2,
  };

  return {
    legacy,
    meta,
    spec_sections: sections,
  };
};

const ensureScoreOnJsonSections = (sectionsValue) => {
  const sectionsObj = toPlainObject(sectionsValue);
  const normalized = {};

  for (const [key, value] of Object.entries(sectionsObj)) {
    if (
      key.endsWith("_json") &&
      value &&
      typeof value === "object" &&
      !Array.isArray(value)
    ) {
      const sectionObj = toPlainObject(value);
      normalized[key] = hasOwn(sectionObj, "score")
        ? sectionObj
        : { score: null, ...sectionObj };
      continue;
    }
    normalized[key] = value;
  }

  return normalized;
};

const normalizeLaptopSectionsForResponse = (sectionsValue, rowValue) => {
  const rowObj = toPlainObject(rowValue);
  const sectionsObj = ensureScoreOnJsonSections(sectionsValue);
  const normalized = { ...sectionsObj };

  if (Array.isArray(rowObj.images)) {
    normalized.images_json = rowObj.images;
  } else if (!Array.isArray(normalized.images_json)) {
    normalized.images_json = [];
  }

  if (Array.isArray(rowObj.variants)) {
    normalized.variants_json = rowObj.variants;
  }

  return ensureScoreOnJsonSections(normalized);
};

const stripScoreRecursively = (value) => {
  if (value instanceof Date) return value;
  if (Array.isArray(value)) {
    return value.map((item) => stripScoreRecursively(item));
  }
  if (!value || typeof value !== "object") return value;

  const cleaned = {};
  for (const [key, val] of Object.entries(value)) {
    if (isScoreLikeKey(key)) continue;
    cleaned[key] = stripScoreRecursively(val);
  }
  return cleaned;
};

const removeSectionKeyCollisions = (
  rowValue,
  sectionsValue,
  extraOmitKeys = [],
) => {
  const rowObj = toPlainObject(rowValue);
  const sectionsObj = toPlainObject(sectionsValue);
  const sectionKeys = new Set(Object.keys(sectionsObj));
  const omitKeys = new Set(["spec_sections", ...extraOmitKeys]);
  const cleaned = {};

  for (const [key, value] of Object.entries(rowObj)) {
    if (omitKeys.has(key)) continue;
    if (sectionKeys.has(key)) continue;
    cleaned[key] = value;
  }

  return cleaned;
};

const enrichLaptopResponse = (row) => {
  const safeRow = row || {};
  const sections = normalizeLaptopSectionsForResponse(
    safeRow.spec_sections,
    safeRow,
  );
  const base = removeSectionKeyCollisions(safeRow, sections, [
    "meta",
    "rating",
    "images",
    "variants",
    "brand",
    "model",
    "colors",
    "category",
    "launch_date",
    "spec_schema_version",
  ]);
  return stripScoreRecursively({
    ...base,
    spec_sections: sections,
  });
};

const toCanonicalLaptopProductResponse = (row) => {
  const safeRow = row || {};
  const sections = normalizeLaptopSectionsForResponse(
    safeRow.spec_sections,
    safeRow,
  );
  const meta = toPlainObject(safeRow.meta);
  const basicInfoSection = toPlainObject(sections.basic_info_json);
  const dynamicSection = toPlainObject(sections.dynamic_json);
  const {
    title: _basicTitle,
    brand: _basicBrand,
    product_name: _basicProductName,
    brand_name: _basicBrandName,
    ...basicInfoExtras
  } = basicInfoSection;

  const ensureScored = (value) => {
    const obj = toPlainObject(value);
    return hasOwn(obj, "score") ? obj : { score: null, ...obj };
  };

  const toArraySafe = (value) => (Array.isArray(value) ? value : []);

  const buildInfo = ensureScored({
    ...basicInfoExtras,
    product_name:
      basicInfoSection.product_name ||
      basicInfoSection.title ||
      dynamicSection.product_name ||
      safeRow.name ||
      null,
    brand_name:
      basicInfoSection.brand_name ||
      basicInfoSection.brand ||
      dynamicSection.brand_name ||
      safeRow.brand_name ||
      null,
    category: basicInfoSection.category || meta.category || null,
    model: basicInfoSection.model || meta.model || null,
    launch_date: basicInfoSection.launch_date || meta.launch_date || null,
    colors: toArraySafe(
      basicInfoSection.colors && Array.isArray(basicInfoSection.colors)
        ? basicInfoSection.colors
        : meta.colors,
    ),
    product_type:
      basicInfoSection.product_type ||
      dynamicSection.product_type ||
      safeRow.product_type ||
      "laptop",
  });

  const multimediaSection = ensureScored({
    ...toPlainObject(sections.multimedia_json),
    features: toArraySafe(
      toPlainObject(sections.multimedia_json).features || safeRow.features,
    ),
  });

  const dynamicMeta = {};
  for (const [key, value] of Object.entries(dynamicSection)) {
    if (["product_name", "brand_name", "product_type"].includes(key)) continue;
    dynamicMeta[key] = value;
  }

  return stripScoreRecursively({
    product_id: safeRow.product_id ?? null,
    basic_info: buildInfo,
    performance: ensureScored(
      Object.keys(toPlainObject(sections.performance_json)).length
        ? sections.performance_json
        : safeRow.cpu,
    ),
    display: ensureScored(
      Object.keys(toPlainObject(sections.display_json)).length
        ? sections.display_json
        : safeRow.display,
    ),
    memory: ensureScored(
      Object.keys(toPlainObject(sections.memory_json)).length
        ? sections.memory_json
        : safeRow.memory,
    ),
    storage: ensureScored(
      Object.keys(toPlainObject(sections.storage_json)).length
        ? sections.storage_json
        : safeRow.storage,
    ),
    battery: ensureScored(
      Object.keys(toPlainObject(sections.battery_json)).length
        ? sections.battery_json
        : safeRow.battery,
    ),
    multimedia: multimediaSection,
    ports: ensureScored(sections.ports_json),
    camera: ensureScored(sections.camera_json),
    security: ensureScored(sections.security_json),
    physical: ensureScored(
      Object.keys(toPlainObject(sections.physical_json)).length
        ? sections.physical_json
        : safeRow.physical,
    ),
    software: ensureScored(
      Object.keys(toPlainObject(sections.software_json)).length
        ? sections.software_json
        : safeRow.software,
    ),
    metadata: ensureScored({
      spec_schema_version: meta.spec_schema_version ?? 2,
      created_at: safeRow.created_at || null,
      build_design: ensureScored(sections.build_design_json),
      connectivity: ensureScored(
        Object.keys(toPlainObject(sections.connectivity_json)).length
          ? sections.connectivity_json
          : safeRow.connectivity,
      ),
      warranty: ensureScored(
        Object.keys(toPlainObject(sections.warranty_json)).length
          ? sections.warranty_json
          : safeRow.warranty,
      ),
      environmental: ensureScored(sections.environmental_json),
      in_the_box: ensureScored(sections.in_the_box_json),
      import_details: ensureScored(sections.import_details_json),
      images: toArraySafe(sections.images_json),
      variants: toArraySafe(sections.variants_json),
      dynamic: ensureScored(dynamicMeta),
    }),
  });
};

const DEFAULT_COMPARE_SCORING_CONFIG = normalizeCompareScoreConfig({});
const DEVICE_PROFILE_TYPES = ["smartphone", "laptop", "tv"];
const DEFAULT_DEVICE_FIELD_PROFILES = {
  smartphone: {
    mandatory: {
      name: ["name", "product_name", "model"],
      brand: ["brand_name", "brand"],
      processor: ["performance.processor", "processor", "specs.processor"],
      battery: [
        "battery.capacity",
        "battery.battery_capacity",
        "battery.battery_capacity_mah",
        "battery",
      ],
      display: ["display.size", "display.display_size", "display"],
      camera: [
        "camera.rear_camera.main_camera.resolution",
        "camera.rear_camera.main.resolution",
        "camera.main_camera_megapixels",
        "camera.main_camera",
      ],
      price: [
        "variants[].base_price",
        "variants[].store_prices[].price",
        "price",
      ],
      image: ["images[]", "image"],
    },
    display: {
      processor: ["performance.processor", "processor", "specs.processor"],
      ram: ["performance.ram", "variants[].ram", "specs.ram"],
      storage: ["performance.storage", "variants[].storage", "specs.storage"],
      battery: [
        "battery.capacity",
        "battery.battery_capacity",
        "battery.battery_capacity_mah",
      ],
      main_camera: [
        "camera.rear_camera.main_camera.resolution",
        "camera.rear_camera.main.resolution",
        "camera.main_camera_megapixels",
        "camera.main_camera",
      ],
      display_size: ["display.size", "display.display_size", "specs.display"],
      refresh_rate: ["display.refresh_rate", "display.refreshRate"],
      os: [
        "performance.operating_system",
        "performance.operatingSystem",
        "performance.os",
      ],
      network: [
        "connectivity.network_type",
        "network.network_type",
        "network.5g_support",
      ],
    },
  },
  laptop: {
    mandatory: {
      name: ["name", "product_name", "basic_info.product_name", "model"],
      brand: ["brand_name", "brand", "basic_info.brand_name"],
      processor: [
        "performance.processor",
        "cpu.processor",
        "specifications.processor",
      ],
      ram: ["memory.ram", "variants[].ram", "specifications.ram"],
      storage: [
        "storage.capacity",
        "variants[].storage",
        "specifications.storage",
      ],
      display: [
        "display.size",
        "display.display_size",
        "specifications.display",
      ],
      battery: ["battery.capacity", "specifications.battery"],
      price: [
        "variants[].base_price",
        "variants[].store_prices[].price",
        "price",
      ],
      image: ["images[]", "image"],
    },
    display: {
      processor: [
        "performance.processor",
        "cpu.processor",
        "specifications.processor",
      ],
      ram: ["memory.ram", "variants[].ram", "specifications.ram"],
      storage: [
        "storage.capacity",
        "variants[].storage",
        "specifications.storage",
      ],
      display_size: [
        "display.size",
        "display.display_size",
        "specifications.display_size",
      ],
      resolution: ["display.resolution", "specifications.resolution"],
      battery: ["battery.capacity", "specifications.battery"],
      os: [
        "software.operating_system",
        "software.os",
        "specifications.operating_system",
      ],
      graphics: [
        "performance.gpu",
        "specifications.graphics",
        "graphics.model",
      ],
      weight: ["physical.weight", "specifications.weight"],
    },
  },
  tv: {
    mandatory: {
      name: ["name", "product_name", "basic_info_json.title", "model"],
      brand: ["brand_name", "brand", "basic_info_json.brand_name"],
      screen_size: [
        "key_specs_json.screen_size",
        "display_json.screen_size",
        "specs.screenSize",
      ],
      resolution: [
        "key_specs_json.resolution",
        "display_json.resolution",
        "specs.resolution",
      ],
      os: [
        "key_specs_json.operating_system",
        "smart_tv_json.operating_system",
        "specs.operatingSystem",
      ],
      refresh_rate: [
        "key_specs_json.refresh_rate",
        "display_json.refresh_rate",
        "specs.refreshRate",
      ],
      price: [
        "variants[].base_price",
        "variants[].store_prices[].price",
        "price",
      ],
      image: ["images[]", "image"],
    },
    display: {
      screen_size: [
        "key_specs_json.screen_size",
        "display_json.screen_size",
        "specs.screenSize",
      ],
      resolution: [
        "key_specs_json.resolution",
        "display_json.resolution",
        "specs.resolution",
      ],
      refresh_rate: [
        "key_specs_json.refresh_rate",
        "display_json.refresh_rate",
        "specs.refreshRate",
      ],
      panel_type: [
        "key_specs_json.panel_type",
        "display_json.panel_type",
        "specs.displayType",
      ],
      os: [
        "key_specs_json.operating_system",
        "smart_tv_json.operating_system",
        "specs.operatingSystem",
      ],
      audio_output: [
        "key_specs_json.audio_output",
        "audio_json.output_power",
        "specs.audioOutput",
      ],
      energy_rating: [
        "power_json.energy_rating",
        "power_json.energy_star_rating",
        "specs.energyRating",
      ],
      smart_features: [
        "smart_tv_json.supported_apps",
        "smart_tv_json.voice_assistant",
        "key_specs_json.ai_features",
      ],
    },
  },
};

const normalizeFieldPathList = (value, fallback = []) => {
  const list = Array.isArray(value) ? value : value ? [value] : [];
  const cleaned = list
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .slice(0, 30);
  return cleaned.length ? cleaned : fallback;
};

const normalizeFieldMap = (value, fallback = {}) => {
  const source =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const output = {};
  const keys = new Set([
    ...Object.keys(fallback || {}),
    ...Object.keys(source || {}),
  ]);

  keys.forEach((key) => {
    output[key] = normalizeFieldPathList(source[key], fallback[key] || []);
  });

  return output;
};

const normalizeSingleDeviceProfile = (value, fallback) => {
  const source =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  return {
    mandatory: normalizeFieldMap(source.mandatory, fallback.mandatory),
    display: normalizeFieldMap(source.display, fallback.display),
  };
};

const normalizeDeviceFieldProfilesConfig = (value) => {
  const source =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const out = {};

  DEVICE_PROFILE_TYPES.forEach((type) => {
    out[type] = normalizeSingleDeviceProfile(
      source[type],
      DEFAULT_DEVICE_FIELD_PROFILES[type],
    );
  });

  return out;
};

const toDeviceFieldProfilesResponse = (config) => ({
  profiles: normalizeDeviceFieldProfilesConfig(config?.profiles),
  updated_at: config?.updated_at || null,
});

const toCompareScoringAdminResponse = (config) => ({
  weights: weightsToPercent(
    config.weights || DEFAULT_COMPARE_SCORING_CONFIG.weights,
  ),
  chipset_rules: Array.isArray(config.chipsetRules)
    ? config.chipsetRules
    : DEFAULT_COMPARE_SCORING_CONFIG.chipsetRules,
  updated_at: config.updated_at || null,
});

async function readCompareScoringConfig() {
  const result = await db.query(
    `SELECT weights, chipset_rules, updated_at
     FROM compare_scoring_config
     WHERE id = 1
     LIMIT 1`,
  );

  if (!result.rows.length) {
    return { ...DEFAULT_COMPARE_SCORING_CONFIG, updated_at: null };
  }

  const row = result.rows[0];
  const normalized = normalizeCompareScoreConfig({
    weights: row.weights,
    chipset_rules: row.chipset_rules,
  });

  return {
    ...normalized,
    updated_at: row.updated_at || null,
  };
}

async function readDeviceFieldProfilesConfig() {
  const result = await db.query(
    `SELECT profiles, updated_at
     FROM device_field_profiles_config
     WHERE id = 1
     LIMIT 1`,
  );

  if (!result.rows.length) {
    return {
      profiles: normalizeDeviceFieldProfilesConfig(
        DEFAULT_DEVICE_FIELD_PROFILES,
      ),
      updated_at: null,
    };
  }

  const row = result.rows[0];
  return {
    profiles: normalizeDeviceFieldProfilesConfig(row.profiles),
    updated_at: row.updated_at || null,
  };
}

const normalizeComparePageSlugInput = (value) => {
  const raw = String(value || "")
    .replace(/^\/+compare\/+/i, "")
    .replace(/^compare\/+/i, "")
    .replace(/^\/+|\/+$/g, "")
    .trim();
  if (!raw) return "";
  const base = raw.replace(/-comparison$/i, "");
  const slugBase = toComparePageSlug(base);
  return slugBase ? `${slugBase}-comparison` : "";
};

const normalizeComparePageLabel = (value, maxLength = 160) =>
  normalizeComparePageText(value).slice(0, Math.max(0, maxLength));

const normalizeComparePageStatus = (value) =>
  String(value || "")
    .trim()
    .toLowerCase() === "draft"
    ? "draft"
    : "published";

const MANUAL_COMPARE_PAGE_SOURCE = "manual";
const AUTOMATIC_COMPARE_PAGE_SOURCE = "automatic";

const normalizeComparePageSource = (value) =>
  String(value || "").trim().toLowerCase() === AUTOMATIC_COMPARE_PAGE_SOURCE
    ? AUTOMATIC_COMPARE_PAGE_SOURCE
    : MANUAL_COMPARE_PAGE_SOURCE;

const buildComparePageKey = (productIds = []) =>
  Array.from(
    new Set(
      (Array.isArray(productIds) ? productIds : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  )
    .sort((left, right) => left - right)
    .join(":");

const buildProductPairKey = (leftId, rightId) => {
  const left = Number(leftId);
  const right = Number(rightId);
  if (
    !Number.isInteger(left) ||
    !Number.isInteger(right) ||
    left <= 0 ||
    right <= 0 ||
    left === right
  ) {
    return "";
  }
  return left < right ? `${left}:${right}` : `${right}:${left}`;
};

const collectComparePageTextFragments = (value, bucket = []) => {
  if (value == null) return bucket;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed) bucket.push(trimmed);
    return bucket;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    bucket.push(String(value));
    return bucket;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectComparePageTextFragments(item, bucket);
    return bucket;
  }
  if (typeof value === "object") {
    for (const nested of Object.values(value)) {
      collectComparePageTextFragments(nested, bucket);
    }
  }
  return bucket;
};

const collectComparePageNumbers = (value, bucket = []) => {
  if (value == null) return bucket;
  if (typeof value === "number" && Number.isFinite(value)) {
    bucket.push(value);
    return bucket;
  }
  if (typeof value === "string") {
    const matches = value.match(/-?\d+(?:\.\d+)?/g);
    if (matches) {
      for (const item of matches) {
        const parsed = Number(item);
        if (Number.isFinite(parsed)) bucket.push(parsed);
      }
    }
    return bucket;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectComparePageNumbers(item, bucket);
    return bucket;
  }
  if (typeof value === "object") {
    for (const nested of Object.values(value)) {
      collectComparePageNumbers(nested, bucket);
    }
  }
  return bucket;
};

const readLargestComparePageNumber = (value, { min = null, max = null } = {}) => {
  const numbers = collectComparePageNumbers(value, []).filter((item) => {
    if (!Number.isFinite(item)) return false;
    if (min != null && item < min) return false;
    if (max != null && item > max) return false;
    return true;
  });
  if (!numbers.length) return null;
  return Math.max(...numbers);
};

const getDaysSinceDateOnly = (value, today = getIndiaDateOnly()) => {
  const date = normalizeDateOnlyInput(value);
  const normalizedToday = normalizeDateOnlyInput(today);
  if (!date || !normalizedToday) return null;
  return diffDateOnlyDays(date, normalizedToday);
};

const deriveSmartphoneTypeLabelFromProduct = (product = {}) => {
  if (!product || typeof product !== "object") return "";

  const fullText = normalizeComparePageText(
    collectComparePageTextFragments(
      [
        product.name,
        product.category,
        product.model,
        product.brand_name,
        product.display,
        product.performance,
        product.camera,
        product.battery,
        product.network,
        product.connectivity,
        product.build_design,
      ],
      [],
    ).join(" "),
  ).toLowerCase();

  const bestPrice = toComparePageFiniteNumber(product.best_price);
  const segmentLabel = resolveSmartphoneSegmentLabel(bestPrice);
  const batteryMah = readLargestComparePageNumber(product.battery, {
    min: 2500,
    max: 10000,
  });
  const chargingWatt = readLargestComparePageNumber(product.battery, {
    min: 18,
    max: 200,
  });
  const displaySize = readLargestComparePageNumber(product.display, {
    min: 4.5,
    max: 8,
  });
  const refreshRate = readLargestComparePageNumber(product.display, {
    min: 80,
    max: 240,
  });
  const cameraMp = readLargestComparePageNumber(product.camera, {
    min: 8,
    max: 250,
  });
  const daysSinceLaunch = getDaysSinceDateOnly(
    product.sale_start_date || product.launch_date || null,
  );

  if (/\bflip\b/.test(fullText)) return "Flip Foldable";
  if (/\bfold|foldable\b/.test(fullText)) return "Book Foldable";
  if (/\brugged|mil[-\s]?std|armor\b/.test(fullText)) return "Rugged";
  if (
    /\bgaming\b|\brog\b|black shark|legion/.test(fullText) ||
    (refreshRate != null && refreshRate >= 144) ||
    ((segmentLabel === "Flagship" || segmentLabel === "Ultra Flagship") &&
      chargingWatt != null &&
      chargingWatt >= 100)
  ) {
    return "Gaming";
  }
  if (/\bselfie\b/.test(fullText)) return "Selfie";
  if (
    /\bperiscope\b|\btelephoto\b|\bzoom\b/.test(fullText) &&
    ["Premium", "Flagship", "Ultra Flagship"].includes(segmentLabel)
  ) {
    return "Camera Flagship";
  }
  if (batteryMah != null && batteryMah >= 6000) return "Battery Focused";
  if (chargingWatt != null && chargingWatt >= 90) return "Fast Charging";
  if (
    displaySize != null &&
    displaySize <= 6.2 &&
    !/\bplus\b|\bmax\b|\bultra\b/.test(fullText)
  ) {
    return "Compact";
  }
  if (/\bclean android\b|\bstock android\b|\bandroid one\b/.test(fullText)) {
    return "Clean Android";
  }
  if (/\bslim\b|\bthin\b|\bdesign\b/.test(fullText)) return "Slim Design";
  if (/\bai\b/.test(fullText)) return "AI";
  if (cameraMp != null && cameraMp >= 50) {
    return ["Premium", "Flagship", "Ultra Flagship"].includes(segmentLabel)
      ? "Camera Flagship"
      : "Camera";
  }
  if (bestPrice != null && bestPrice <= 25000 && /\b5g\b/.test(fullText)) {
    return "Value 5G";
  }
  if (daysSinceLaunch != null && daysSinceLaunch >= 0 && daysSinceLaunch <= 75) {
    return "New Launch";
  }
  return "";
};

const getComparePageRoutePath = (slug = "") =>
  `/compare/${String(slug || "").trim()}`;

const normalizePublishedComparePageRow = (row) => {
  const items = Array.isArray(row?.items)
    ? row.items
        .map((item) => ({
          product_id: Number(item?.product_id) || null,
          product_name: item?.product_name || item?.name || "",
          product_type: item?.product_type || "smartphone",
          brand_name: item?.brand_name || "",
          position: Number(item?.position) || null,
        }))
        .filter(
          (item) => Number.isInteger(item.product_id) && item.product_id > 0,
        )
        .sort(
          (left, right) =>
            Number(left.position || 0) - Number(right.position || 0),
        )
    : [];

  return {
    id: Number(row?.id) || null,
    slug: row?.slug || "",
    compare_key: row?.compare_key || "",
    route_path: row?.slug ? getComparePageRoutePath(row.slug) : "/compare",
    entity_type: row?.entity_type || "smartphone",
    primary_product_id: Number(row?.primary_product_id) || null,
    primary_product_name: row?.primary_product_name || "",
    segment_label: row?.segment_label || "",
    smartphone_type_label: row?.smartphone_type_label || "",
    title: row?.title || "",
    meta_description: row?.meta_description || "",
    status: row?.status || "published",
    source: normalizeComparePageSource(row?.source),
    generation_reason: row?.generation_reason || "",
    system_score: toComparePageFiniteNumber(row?.system_score) ?? 0,
    manual_compare_count: Number(row?.manual_compare_count) || 0,
    last_compared_at: row?.last_compared_at || null,
    generated_at: row?.generated_at || null,
    published_at: row?.published_at || null,
    created_at: row?.created_at || null,
    updated_at: row?.updated_at || null,
    items,
  };
};

async function readPublishedComparePages({
  id = null,
  slug = "",
  publishedOnly = false,
  limit = 100,
} = {}) {
  const params = [];
  const where = [`cp.entity_type = 'smartphone'`];

  if (publishedOnly) {
    where.push(`cp.status = 'published'`);
  }

  const normalizedId = Number(id);
  if (Number.isInteger(normalizedId) && normalizedId > 0) {
    params.push(normalizedId);
    where.push(`cp.id = $${params.length}`);
  }

  const normalizedSlug = normalizeComparePageSlugInput(slug);
  if (normalizedSlug) {
    params.push(normalizedSlug);
    where.push(`cp.slug = $${params.length}`);
  }

  params.push(Math.min(200, Math.max(1, Number(limit) || 100)));

  const result = await db.query(
    `
    SELECT
      cp.id,
      cp.slug,
      cp.compare_key,
      cp.entity_type,
      cp.primary_product_id,
      cp.segment_label,
      cp.smartphone_type_label,
      cp.title,
      cp.meta_description,
      cp.status,
      cp.source,
      cp.generation_reason,
      cp.system_score,
      cp.manual_compare_count,
      cp.last_compared_at,
      cp.generated_at,
      cp.published_at,
      cp.created_at,
      cp.updated_at,
      primary_product.name AS primary_product_name,
      COALESCE(
        json_agg(
          jsonb_build_object(
            'product_id', item_product.id,
            'product_name', item_product.name,
            'product_type', item_product.product_type,
            'brand_name', item_brand.name,
            'position', cpi.position
          )
          ORDER BY cpi.position
        ) FILTER (WHERE cpi.id IS NOT NULL),
        '[]'::json
      ) AS items
    FROM published_compare_pages cp
    INNER JOIN products primary_product
      ON primary_product.id = cp.primary_product_id
    LEFT JOIN published_compare_page_items cpi
      ON cpi.compare_page_id = cp.id
    LEFT JOIN products item_product
      ON item_product.id = cpi.product_id
    LEFT JOIN brands item_brand
      ON item_brand.id = item_product.brand_id
    WHERE ${where.join(" AND ")}
    GROUP BY cp.id, primary_product.name
    ORDER BY cp.updated_at DESC, cp.id DESC
    LIMIT $${params.length}
    `,
    params,
  );

  return (result.rows || []).map(normalizePublishedComparePageRow);
}

async function readSmartphoneComparePageProductSummaries(productIds = []) {
  const ids = Array.from(
    new Set(
      (Array.isArray(productIds) ? productIds : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );

  if (!ids.length) return [];

  const result = await db.query(
    `
    SELECT
      p.id AS product_id,
      p.name,
      p.product_type,
      b.name AS brand_name,
      COALESCE(
        (
          SELECT MIN(vsp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices vsp
            ON vsp.variant_id = pv.id
          WHERE pv.product_id = p.id
            AND vsp.price IS NOT NULL
        ),
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p.id
            AND pv.base_price IS NOT NULL
        )
      ) AS best_price
    FROM products p
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
     AND pub.is_published = true
    LEFT JOIN brands b
      ON b.id = p.brand_id
    WHERE p.product_type = 'smartphone'
      AND p.id = ANY($1::int[])
    `,
    [ids],
  );

  return (result.rows || []).map((row) => ({
    product_id: Number(row?.product_id) || null,
    name: row?.name || "",
    product_type: row?.product_type || "smartphone",
    brand_name: row?.brand_name || "",
    best_price: toComparePageFiniteNumber(row?.best_price),
  }));
}

async function readSmartphoneCompareGenerationRows(productIds = []) {
  const ids = Array.from(
    new Set(
      (Array.isArray(productIds) ? productIds : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );

  const params = [];
  let where = `
      p.product_type = 'smartphone'
      AND pub.is_published = true
    `;

  if (ids.length) {
    params.push(ids);
    where += ` AND p.id = ANY($${params.length}::int[])`;
  }

  const result = await db.query(
    `
    SELECT
      p.id AS product_id,
      p.name,
      p.product_type,
      COALESCE(b.name, NULLIF(TRIM(s.brand), ''), '') AS brand_name,
      NULLIF(TRIM(s.category), '') AS category,
      NULLIF(TRIM(s.model), '') AS model,
      s.launch_date,
      s.launch_status_override,
      s.display,
      s.performance,
      s.camera,
      s.battery,
      s.network,
      s.connectivity,
      s.build_design,
      store_stats.sale_start_date,
      COALESCE(store_stats.min_store_price, base_stats.min_base_price) AS best_price,
      COALESCE(compare_stats.compare_count_30d, 0) AS manual_compare_count,
      compare_stats.last_compared_at
    FROM products p
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
    INNER JOIN smartphones s
      ON s.product_id = p.id
    LEFT JOIN brands b
      ON b.id = p.brand_id
    LEFT JOIN LATERAL (
      SELECT
        MIN(vsp.price)::numeric AS min_store_price,
        MIN(vsp.sale_start_date) AS sale_start_date
      FROM product_variants pv
      INNER JOIN variant_store_prices vsp
        ON vsp.variant_id = pv.id
      WHERE pv.product_id = p.id
        AND vsp.price IS NOT NULL
    ) store_stats ON true
    LEFT JOIN LATERAL (
      SELECT MIN(pv.base_price)::numeric AS min_base_price
      FROM product_variants pv
      WHERE pv.product_id = p.id
        AND pv.base_price IS NOT NULL
    ) base_stats ON true
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*)::int AS compare_count_30d,
        MAX(compared_at) AS last_compared_at
      FROM (
        SELECT compared_at
        FROM product_comparisons
        WHERE product_id = p.id
          AND compared_at >= now() - INTERVAL '30 days'
        UNION ALL
        SELECT compared_at
        FROM product_comparisons
        WHERE compared_with = p.id
          AND compared_at >= now() - INTERVAL '30 days'
      ) comparisons
    ) compare_stats ON true
    WHERE ${where}
    ORDER BY COALESCE(s.launch_date, p.created_at::date) DESC, p.id DESC
    `,
    params,
  );

  return (result.rows || []).map((row) => ({
    product_id: Number(row?.product_id) || null,
    name: row?.name || "",
    product_type: row?.product_type || "smartphone",
    brand_name: row?.brand_name || "",
    category: row?.category || "",
    model: row?.model || "",
    launch_date: row?.launch_date || null,
    launch_status_override: row?.launch_status_override || null,
    display: row?.display || {},
    performance: row?.performance || {},
    camera: row?.camera || {},
    battery: row?.battery || {},
    network: row?.network || {},
    connectivity: row?.connectivity || {},
    build_design: row?.build_design || {},
    sale_start_date: row?.sale_start_date || null,
    best_price: toComparePageFiniteNumber(row?.best_price),
    manual_compare_count: Number(row?.manual_compare_count) || 0,
    last_compared_at: row?.last_compared_at || null,
  }));
}

async function readSmartphonePairCompareSignals() {
  const result = await db.query(
    `
    SELECT
      LEAST(pc.product_id, pc.compared_with) AS left_id,
      GREATEST(pc.product_id, pc.compared_with) AS right_id,
      COUNT(pc.id)::int AS compare_count,
      MAX(pc.compared_at) AS last_compared_at
    FROM product_comparisons pc
    INNER JOIN products p1
      ON p1.id = pc.product_id
     AND p1.product_type = 'smartphone'
    INNER JOIN products p2
      ON p2.id = pc.compared_with
     AND p2.product_type = 'smartphone'
    INNER JOIN product_publish pub1
      ON pub1.product_id = p1.id
     AND pub1.is_published = true
    INNER JOIN product_publish pub2
      ON pub2.product_id = p2.id
     AND pub2.is_published = true
    WHERE pc.compared_at >= now() - INTERVAL '30 days'
    GROUP BY 1, 2
    `,
  );

  const signalMap = new Map();
  for (const row of result.rows || []) {
    const key = buildProductPairKey(row?.left_id, row?.right_id);
    if (!key) continue;
    signalMap.set(key, {
      compare_count: Number(row?.compare_count) || 0,
      last_compared_at: row?.last_compared_at || null,
    });
  }
  return signalMap;
}

async function readSmartphoneCompetitorSuggestionsMap(
  productIds = [],
  limitPerProduct = 4,
) {
  const ids = Array.from(
    new Set(
      (Array.isArray(productIds) ? productIds : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );
  if (!ids.length) return new Map();

  const result = await db.query(
    `
    SELECT
      ca.product_id,
      ca.competitor_id,
      ca.competition_score,
      ca.reason
    FROM competitor_analysis ca
    INNER JOIN products p
      ON p.id = ca.competitor_id
     AND p.product_type = 'smartphone'
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
     AND pub.is_published = true
    WHERE ca.product_id = ANY($1::int[])
    ORDER BY ca.product_id ASC, ca.competition_score DESC, ca.competitor_id ASC
    `,
    [ids],
  );

  const byProductId = new Map();
  for (const row of result.rows || []) {
    const productId = Number(row?.product_id);
    const competitorId = Number(row?.competitor_id);
    if (!Number.isInteger(productId) || !Number.isInteger(competitorId)) continue;
    const bucket = byProductId.get(productId) || [];
    if (bucket.length < Math.max(1, Number(limitPerProduct) || 4)) {
      bucket.push({
        product_id: competitorId,
        competition_score: toComparePageFiniteNumber(row?.competition_score) ?? 0,
        reason: normalizeComparePageLabel(row?.reason, 200),
      });
      byProductId.set(productId, bucket);
    }
  }

  return byProductId;
}

async function findExistingComparePageByKey(compareKey, { excludePageId = null } = {}) {
  const normalizedKey = String(compareKey || "").trim();
  if (!normalizedKey) return null;

  const params = [normalizedKey];
  const excludeId = Number(excludePageId);
  let where = `cp.compare_key = $1`;
  if (Number.isInteger(excludeId) && excludeId > 0) {
    params.push(excludeId);
    where += ` AND cp.id <> $${params.length}`;
  }

  const result = await db.query(
    `
    SELECT cp.id
    FROM published_compare_pages cp
    WHERE ${where}
    ORDER BY cp.updated_at DESC, cp.id DESC
    LIMIT 1
    `,
    params,
  );

  const existingId = Number(result.rows[0]?.id);
  if (!Number.isInteger(existingId) || existingId <= 0) return null;

  const pages = await readPublishedComparePages({ id: existingId, limit: 1 });
  return pages[0] || null;
}

async function readSmartphoneCompareSuggestions(productId, limit = 2) {
  const normalizedId = Number(productId);
  const normalizedLimit = Math.min(3, Math.max(1, Number(limit) || 2));
  if (!Number.isInteger(normalizedId) || normalizedId <= 0) {
    return { primary_product: null, suggestions: [] };
  }

  const primaryRows = await readSmartphoneComparePageProductSummaries([
    normalizedId,
  ]);
  const primaryProduct = primaryRows[0] || null;
  if (!primaryProduct) {
    return { primary_product: null, suggestions: [] };
  }

  const fetchSuggestions = async () => {
    const result = await db.query(
      `
      SELECT
        ca.competitor_id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        ca.competition_score,
        ca.reason,
        COALESCE(
          (
            SELECT MIN(vsp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices vsp
              ON vsp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND vsp.price IS NOT NULL
          ),
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p.id
              AND pv.base_price IS NOT NULL
          )
        ) AS best_price
      FROM competitor_analysis ca
      INNER JOIN products p
        ON p.id = ca.competitor_id
       AND p.product_type = 'smartphone'
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE ca.product_id = $1
      ORDER BY ca.competition_score DESC, ca.competitor_id ASC
      LIMIT $2
      `,
      [normalizedId, normalizedLimit],
    );

    return (result.rows || []).map((row) => ({
      product_id: Number(row?.product_id) || null,
      name: row?.name || "",
      product_type: row?.product_type || "smartphone",
      brand_name: row?.brand_name || "",
      best_price: toComparePageFiniteNumber(row?.best_price),
      competition_score: toComparePageFiniteNumber(row?.competition_score),
      reason: row?.reason || "",
    }));
  };

  let suggestions = await fetchSuggestions();
  if (suggestions.length === 0) {
    try {
      await recomputeSmartphoneCompetitorAnalysis(db, {
        limit: normalizedLimit,
        productIds: [normalizedId],
      });
    } catch (err) {
      console.error("Compare page suggestion recompute failed:", err.message);
    }
    suggestions = await fetchSuggestions();
  }

  const suggestedProductIds = [
    normalizedId,
    ...suggestions
      .map((item) => Number(item?.product_id))
      .filter((value) => Number.isInteger(value) && value > 0),
  ].slice(0, 1 + normalizedLimit);
  const compareKey = buildComparePageKey(suggestedProductIds);
  const existingPage = compareKey
    ? await findExistingComparePageByKey(compareKey)
    : null;

  return {
    primary_product: primaryProduct,
    suggestions,
    compare_key: compareKey,
    existing_page: existingPage,
  };
}

const buildAutomaticCompareGenerationReason = ({
  stage = "",
  compareCount = 0,
  competitorReasons = [],
} = {}) => {
  const parts = [];
  if (stage === "upcoming" || stage === "announced") {
    parts.push("latest launch signal");
  }
  if (compareCount > 0) {
    parts.push(`${compareCount} manual compare${compareCount === 1 ? "" : "s"} in the last 30 days`);
  }
  const cleanReasons = (Array.isArray(competitorReasons) ? competitorReasons : [])
    .map((value) => normalizeComparePageLabel(value, 120))
    .filter(Boolean)
    .slice(0, 2);
  if (cleanReasons.length) {
    parts.push(cleanReasons.join(" | "));
  }
  if (!parts.length) {
    return "Auto generated from smartphone competitor analysis";
  }
  return `Auto generated from smartphone competitor analysis and ${parts.join(" with ")}`;
};

async function syncAutomaticSmartphoneComparePages({
  userId = null,
  recomputeIfMissing = true,
} = {}) {
  const allProducts = await readSmartphoneCompareGenerationRows();
  if (!allProducts.length) {
    return {
      ok: true,
      total_products: 0,
      created: 0,
      updated: 0,
      reused_manual: 0,
      drafted_stale: 0,
      generated: 0,
    };
  }

  const eligibleProducts = allProducts.filter((product) => {
    const stage = resolveSmartphoneLaunchStage(product);
    const policy = resolveSmartphoneLaunchPolicy(stage, product);
    return policy.allow_compare !== false;
  });

  if (!eligibleProducts.length) {
    return {
      ok: true,
      total_products: allProducts.length,
      created: 0,
      updated: 0,
      reused_manual: 0,
      drafted_stale: 0,
      generated: 0,
    };
  }

  let suggestionsByProductId = await readSmartphoneCompetitorSuggestionsMap(
    eligibleProducts.map((product) => product.product_id),
    5,
  );

  if (recomputeIfMissing && suggestionsByProductId.size === 0) {
    try {
      await recomputeSmartphoneCompetitorAnalysis(db, { limit: 5 });
      suggestionsByProductId = await readSmartphoneCompetitorSuggestionsMap(
        eligibleProducts.map((product) => product.product_id),
        5,
      );
    } catch (err) {
      console.error("Automatic compare page recompute fallback failed:", err);
    }
  }

  const productById = new Map(
    allProducts.map((product) => [Number(product.product_id), product]),
  );
  const pairSignals = await readSmartphonePairCompareSignals();
  const candidateByCompareKey = new Map();
  const todayIndia = getIndiaDateOnly();

  for (const primaryProduct of eligibleProducts) {
    const primaryId = Number(primaryProduct.product_id);
    if (!Number.isInteger(primaryId) || primaryId <= 0) continue;

    const stage = resolveSmartphoneLaunchStage(primaryProduct, todayIndia);
    const policy = resolveSmartphoneLaunchPolicy(stage, primaryProduct);
    const additionalLimit = Math.max(
      1,
      Math.min(2, Math.max(1, Number(policy.compare_limit || 3) - 1)),
    );

    const rankedCompetitors = (suggestionsByProductId.get(primaryId) || [])
      .map((candidate) => ({
        ...candidate,
        product: productById.get(Number(candidate.product_id)) || null,
      }))
      .filter((candidate) => candidate.product)
      .slice(0, 5);

    if (!rankedCompetitors.length) continue;

    const selected = rankedCompetitors.slice(0, additionalLimit);
    if (!selected.length) continue;

    const orderedProducts = [primaryProduct, ...selected.map((entry) => entry.product)].slice(
      0,
      1 + additionalLimit,
    );
    const orderedProductIds = orderedProducts
      .map((entry) => Number(entry?.product_id))
      .filter((value) => Number.isInteger(value) && value > 0);
    if (orderedProductIds.length < 2) continue;

    const compareKey = buildComparePageKey(orderedProductIds);
    if (!compareKey) continue;

    const compareCount = orderedProductIds.reduce((sum, productId, index) => {
      let running = sum;
      for (let cursor = index + 1; cursor < orderedProductIds.length; cursor += 1) {
        const signal = pairSignals.get(
          buildProductPairKey(productId, orderedProductIds[cursor]),
        );
        running += Number(signal?.compare_count) || 0;
      }
      return running;
    }, 0);

    const lastComparedAt = orderedProductIds.reduce((latest, productId, index) => {
      let nextLatest = latest;
      for (let cursor = index + 1; cursor < orderedProductIds.length; cursor += 1) {
        const signal = pairSignals.get(
          buildProductPairKey(productId, orderedProductIds[cursor]),
        );
        const candidateTime = signal?.last_compared_at
          ? new Date(signal.last_compared_at).getTime()
          : 0;
        const latestTime = nextLatest ? new Date(nextLatest).getTime() : 0;
        if (candidateTime > latestTime) {
          nextLatest = signal.last_compared_at;
        }
      }
      return nextLatest;
    }, null);

    const averagePrice =
      orderedProducts
        .map((item) => toComparePageFiniteNumber(item?.best_price))
        .filter((value) => value != null)
        .reduce((sum, value, _index, array) => sum + value / array.length, 0) || null;
    const segmentLabel =
      resolveSmartphoneSegmentLabel(
        toComparePageFiniteNumber(primaryProduct.best_price) ?? averagePrice,
      ) || "";
    const smartphoneTypeLabel = deriveSmartphoneTypeLabelFromProduct(primaryProduct);
    const names = orderedProducts.map((item) => item?.name).filter(Boolean);
    const systemScore = selected.reduce(
      (sum, item) => sum + (toComparePageFiniteNumber(item?.competition_score) ?? 0),
      0,
    );
    const daysSinceLaunch = getDaysSinceDateOnly(
      primaryProduct.sale_start_date || primaryProduct.launch_date,
      todayIndia,
    );
    const freshnessBoost =
      daysSinceLaunch != null && daysSinceLaunch >= 0 && daysSinceLaunch <= 75 ? 40 : 0;
    const priorityScore = systemScore * 100 + compareCount * 10 + freshnessBoost;

    const candidate = {
      compareKey,
      productIds: orderedProductIds,
      primaryProductId: primaryId,
      segmentLabel,
      smartphoneTypeLabel,
      title: buildAutomaticComparePageTitle({
        names,
        segmentLabel,
        smartphoneTypeLabel,
        price:
          toComparePageFiniteNumber(primaryProduct.best_price) ?? averagePrice,
      }),
      metaDescription: buildAutomaticComparePageDescription({
        names,
        segmentLabel,
        smartphoneTypeLabel,
        price:
          toComparePageFiniteNumber(primaryProduct.best_price) ?? averagePrice,
        updatedAt: new Date(),
      }),
      source: AUTOMATIC_COMPARE_PAGE_SOURCE,
      systemScore,
      manualCompareCount: compareCount,
      lastComparedAt,
      generatedAt: new Date(),
      generationReason: buildAutomaticCompareGenerationReason({
        stage,
        compareCount,
        competitorReasons: selected.map((item) => item.reason),
      }),
      priorityScore,
    };

    const existing = candidateByCompareKey.get(compareKey);
    if (!existing || candidate.priorityScore > existing.priorityScore) {
      candidateByCompareKey.set(compareKey, candidate);
    }
  }

  let created = 0;
  let updated = 0;
  let reusedManual = 0;
  const activeKeys = new Set(candidateByCompareKey.keys());

  for (const candidate of candidateByCompareKey.values()) {
    const existing = await findExistingComparePageByKey(candidate.compareKey);
    if (existing?.source === MANUAL_COMPARE_PAGE_SOURCE) {
      reusedManual += 1;
      continue;
    }

    const saved = await savePublishedComparePage({
      pageId: existing?.id ?? null,
      payload: {
        product_ids: candidate.productIds,
        primary_product_id: candidate.primaryProductId,
        segment_label: candidate.segmentLabel,
        smartphone_type_label: candidate.smartphoneTypeLabel,
        title: candidate.title,
        meta_description: candidate.metaDescription,
        status: existing?.status || "published",
        source: AUTOMATIC_COMPARE_PAGE_SOURCE,
        compare_key: candidate.compareKey,
        generation_reason: candidate.generationReason,
        system_score: candidate.systemScore,
        manual_compare_count: candidate.manualCompareCount,
        last_compared_at: candidate.lastComparedAt,
        generated_at: candidate.generatedAt,
      },
      userId,
      bypassDuplicateCheck: true,
    });

    if (saved?.id && existing?.id) {
      updated += 1;
    } else if (saved?.id) {
      created += 1;
    }
  }

  let draftedStale = 0;
  if (activeKeys.size > 0) {
    const automaticPages = await readPublishedComparePages({ limit: 400 });
    const staleAutomaticIds = automaticPages
      .filter(
        (page) =>
          page.source === AUTOMATIC_COMPARE_PAGE_SOURCE &&
          page.compare_key &&
          !activeKeys.has(page.compare_key),
      )
      .map((page) => Number(page.id))
      .filter((value) => Number.isInteger(value) && value > 0);

    if (staleAutomaticIds.length) {
      const result = await db.query(
        `
        UPDATE published_compare_pages
        SET
          status = 'draft',
          updated_by = $2,
          updated_at = now()
        WHERE id = ANY($1::int[])
          AND status <> 'draft'
        `,
        [staleAutomaticIds, userId],
      );
      draftedStale = Number(result.rowCount) || 0;
    }
  }

  return {
    ok: true,
    total_products: allProducts.length,
    generated: candidateByCompareKey.size,
    created,
    updated,
    reused_manual: reusedManual,
    drafted_stale: draftedStale,
  };
}

async function savePublishedComparePage({
  pageId = null,
  payload = {},
  userId = null,
  bypassDuplicateCheck = false,
}) {
  const rawProductIds = Array.isArray(payload.product_ids)
    ? payload.product_ids
    : Array.isArray(payload.productIds)
      ? payload.productIds
      : Array.isArray(payload.items)
        ? payload.items.map(
            (item) => item?.product_id ?? item?.productId ?? item?.id,
          )
        : [];

  const requestedProductIds = Array.from(
    new Set(
      rawProductIds
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  ).slice(0, 3);

  if (requestedProductIds.length < 2) {
    const error = new Error(
      "Select at least 2 smartphones for a compare page.",
    );
    error.statusCode = 400;
    throw error;
  }

  const productRows =
    await readSmartphoneComparePageProductSummaries(requestedProductIds);
  if (productRows.length !== requestedProductIds.length) {
    const error = new Error(
      "All compare page products must be published smartphones.",
    );
    error.statusCode = 400;
    throw error;
  }

  const productMap = new Map(productRows.map((row) => [row.product_id, row]));
  const requestedPrimaryProductId = Number(
    payload.primary_product_id ??
      payload.primaryProductId ??
      requestedProductIds[0],
  );
  const primaryProductId = productMap.has(requestedPrimaryProductId)
    ? requestedPrimaryProductId
    : requestedProductIds[0];

  const orderedProductIds = [
    primaryProductId,
    ...requestedProductIds.filter(
      (productId) => productId !== primaryProductId,
    ),
  ].slice(0, 3);

  const orderedProducts = orderedProductIds
    .map((productId) => productMap.get(productId))
    .filter(Boolean);
  const compareKey =
    String(payload.compare_key || "").trim() || buildComparePageKey(orderedProductIds);

  const priceCandidates = orderedProducts
    .map((item) => item?.best_price)
    .filter((value) => value != null);
  const averagePrice =
    priceCandidates.length > 0
      ? priceCandidates.reduce((sum, value) => sum + value, 0) /
        priceCandidates.length
      : null;

  const segmentLabel =
    normalizeComparePageLabel(
      payload.segment_label ?? payload.segmentLabel,
      80,
    ) ||
    resolveSmartphoneSegmentLabel(
      productMap.get(primaryProductId)?.best_price ?? averagePrice,
    );
  const smartphoneTypeLabel = normalizeComparePageLabel(
    payload.smartphone_type_label ?? payload.smartphoneTypeLabel,
    80,
  );
  const names = orderedProducts.map((product) => product.name).filter(Boolean);

  const slug =
    normalizeComparePageSlugInput(payload.slug) || buildComparePageSlug(names);
  if (!slug) {
    const error = new Error("Unable to generate compare page slug.");
    error.statusCode = 400;
    throw error;
  }

  const status = normalizeComparePageStatus(payload.status);
  const source = normalizeComparePageSource(payload.source);
  const title =
    normalizeComparePageLabel(payload.title, 220) ||
    buildComparePageTitle({
      names,
      segmentLabel,
      smartphoneTypeLabel,
    });
  const metaDescription =
    normalizeComparePageLabel(
      payload.meta_description ?? payload.metaDescription,
      320,
    ) ||
    buildComparePageDescription({
      names,
      segmentLabel,
      smartphoneTypeLabel,
      updatedAt: new Date(),
    });
  const generationReason = normalizeComparePageLabel(
    payload.generation_reason ?? payload.generationReason,
    320,
  );
  const systemScore = toComparePageFiniteNumber(
    payload.system_score ?? payload.systemScore,
  );
  const manualCompareCount = Math.max(
    0,
    Number(payload.manual_compare_count ?? payload.manualCompareCount) || 0,
  );
  const generatedAt = payload.generated_at ?? payload.generatedAt ?? null;
  const lastComparedAt =
    payload.last_compared_at ?? payload.lastComparedAt ?? null;

  const normalizedPageId = Number(pageId);
  if (!bypassDuplicateCheck && compareKey) {
    const existingPage = await findExistingComparePageByKey(compareKey, {
      excludePageId: normalizedPageId,
    });
    if (existingPage) {
      const error = new Error(
        existingPage.source === AUTOMATIC_COMPARE_PAGE_SOURCE
          ? "Automatic compare already exists for these smartphones."
          : "Compare page already exists for these smartphones.",
      );
      error.statusCode = 409;
      error.existingPage = existingPage;
      throw error;
    }
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    let savedPageId = normalizedPageId;
    if (Number.isInteger(savedPageId) && savedPageId > 0) {
      const result = await client.query(
        `
        UPDATE published_compare_pages
        SET
          slug = $1,
          compare_key = $2,
          entity_type = 'smartphone',
          primary_product_id = $3,
          segment_label = $4,
          smartphone_type_label = $5,
          title = $6,
          meta_description = $7,
          status = $8,
          source = $9,
          generation_reason = $10,
          system_score = $11,
          manual_compare_count = $12,
          last_compared_at = $13,
          generated_at = COALESCE($14::timestamp, generated_at, now()),
          updated_by = $15,
          updated_at = now(),
          published_at = CASE
            WHEN $8 = 'published' AND published_at IS NULL THEN now()
            WHEN $8 <> 'published' THEN NULL
            ELSE published_at
          END
        WHERE id = $16
        RETURNING id
        `,
        [
          slug,
          compareKey || null,
          primaryProductId,
          segmentLabel || null,
          smartphoneTypeLabel || null,
          title,
          metaDescription,
          status,
          source,
          generationReason || null,
          systemScore ?? 0,
          manualCompareCount,
          lastComparedAt || null,
          generatedAt || null,
          userId,
          savedPageId,
        ],
      );

      if (!result.rows.length) {
        const error = new Error("Compare page not found.");
        error.statusCode = 404;
        throw error;
      }
    } else {
      const result = await client.query(
        `
        INSERT INTO published_compare_pages (
          slug,
          compare_key,
          entity_type,
          primary_product_id,
          segment_label,
          smartphone_type_label,
          title,
          meta_description,
          status,
          source,
          generation_reason,
          system_score,
          manual_compare_count,
          last_compared_at,
          created_by,
          updated_by,
          generated_at,
          published_at,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          $2,
          'smartphone',
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11,
          $12,
          $13,
          $14,
          $14,
          COALESCE($15::timestamp, now()),
          CASE WHEN $8 = 'published' THEN now() ELSE NULL END,
          now(),
          now()
        )
        RETURNING id
        `,
        [
          slug,
          compareKey || null,
          primaryProductId,
          segmentLabel || null,
          smartphoneTypeLabel || null,
          title,
          metaDescription,
          status,
          source,
          generationReason || null,
          systemScore ?? 0,
          manualCompareCount,
          lastComparedAt || null,
          userId,
          generatedAt || null,
        ],
      );
      savedPageId = Number(result.rows[0]?.id) || null;
    }

    await client.query(
      `
      UPDATE published_compare_pages
      SET compare_key = COALESCE(compare_key, $2)
      WHERE id = $1
      `,
      [savedPageId, compareKey || null],
    );

    await client.query(
      `DELETE FROM published_compare_page_items WHERE compare_page_id = $1`,
      [savedPageId],
    );

    for (let index = 0; index < orderedProductIds.length; index += 1) {
      await client.query(
        `
        INSERT INTO published_compare_page_items (
          compare_page_id,
          product_id,
          position,
          created_at
        )
        VALUES ($1, $2, $3, now())
        `,
        [savedPageId, orderedProductIds[index], index + 1],
      );
    }

    await client.query("COMMIT");
    const pages = await readPublishedComparePages({
      id: savedPageId,
      limit: 1,
    });
    return pages[0] || null;
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_rollbackErr) {
      // ignore rollback errors
    }
    throw err;
  } finally {
    client.release();
  }
}

const parseJsonLikeValue = (value) => {
  if (typeof value !== "string") return value;
  const text = value.trim();
  if (!text) return value;
  if (
    (text.startsWith("{") && text.endsWith("}")) ||
    (text.startsWith("[") && text.endsWith("]"))
  ) {
    try {
      return JSON.parse(text);
    } catch (_err) {
      return value;
    }
  }
  return value;
};

const profileHasValue = (value) => {
  const normalized = parseJsonLikeValue(value);
  if (normalized === null || normalized === undefined) return false;
  if (typeof normalized === "string") return normalized.trim().length > 0;
  if (typeof normalized === "number") return Number.isFinite(normalized);
  if (typeof normalized === "boolean") return true;
  if (Array.isArray(normalized))
    return normalized.some((item) => profileHasValue(item));
  if (normalized && typeof normalized === "object")
    return Object.values(normalized).some((item) => profileHasValue(item));
  return true;
};

const toProfileDisplayValue = (value) => {
  const normalized = parseJsonLikeValue(value);
  if (normalized === null || normalized === undefined) return null;
  if (Array.isArray(normalized)) {
    const flattened = normalized
      .map((item) => toProfileDisplayValue(item))
      .filter((item) => profileHasValue(item));
    return flattened.length ? flattened.join(", ") : null;
  }
  if (normalized && typeof normalized === "object") {
    const entries = Object.entries(normalized)
      .map(([key, val]) => {
        const rendered = toProfileDisplayValue(val);
        return profileHasValue(rendered) ? `${key}: ${rendered}` : null;
      })
      .filter(Boolean);
    return entries.length ? entries.join(" | ") : null;
  }
  return String(normalized).trim();
};

const collectProfilePathValues = (source, path) => {
  if (!path || !source) return [];
  const segments = String(path)
    .split(".")
    .map((segment) => segment.trim())
    .filter(Boolean);
  if (!segments.length) return [];

  const walk = (current, index) => {
    const value = parseJsonLikeValue(current);
    if (index >= segments.length) return [value];
    if (value === null || value === undefined) return [];

    const segment = segments[index];

    if (segment === "*") {
      if (Array.isArray(value)) {
        return value.flatMap((item) => walk(item, index + 1));
      }
      if (value && typeof value === "object") {
        return Object.values(value).flatMap((item) => walk(item, index + 1));
      }
      return [];
    }

    if (segment.endsWith("[]")) {
      const key = segment.slice(0, -2);
      const target = key ? parseJsonLikeValue(value?.[key]) : value;
      if (!Array.isArray(target)) return [];
      return target.flatMap((item) => walk(item, index + 1));
    }

    if (/^\d+$/.test(segment)) {
      const idx = Number(segment);
      if (!Array.isArray(value) || idx >= value.length) return [];
      return walk(value[idx], index + 1);
    }

    return walk(value?.[segment], index + 1);
  };

  return walk(source, 0).filter((item) => profileHasValue(item));
};

const resolveProfileValueByPaths = (source, paths = []) => {
  for (const path of paths || []) {
    const values = collectProfilePathValues(source, path);
    if (!values.length) continue;
    const first = values.find((item) => profileHasValue(item));
    if (profileHasValue(first)) return first;
  }
  return null;
};

const normalizeProfileDeviceType = (type) => {
  const normalized = String(type || "")
    .trim()
    .toLowerCase();
  if (["smartphone", "smartphones", "mobile", "mobiles"].includes(normalized)) {
    return "smartphone";
  }
  if (["laptop", "laptops", "notebook", "notebooks"].includes(normalized)) {
    return "laptop";
  }
  if (
    [
      "tv",
      "tvs",
      "television",
      "televisions",
      "home-appliance",
      "home_appliance",
      "homeappliance",
      "appliance",
      "appliances",
    ].includes(normalized)
  ) {
    return "tv";
  }
  return "smartphone";
};

const resolveDeviceFieldProfileScore = (type, device, profiles) => {
  const normalizedProfiles = normalizeDeviceFieldProfilesConfig(profiles);
  const normalizedType = normalizeProfileDeviceType(
    type || device?.product_type,
  );
  const profile =
    normalizedProfiles[normalizedType] || normalizedProfiles.smartphone;

  const mandatoryValues = {};
  const displayValues = {};
  const missingMandatory = [];

  Object.entries(profile.mandatory || {}).forEach(([key, paths]) => {
    const resolved = resolveProfileValueByPaths(device, paths);
    mandatoryValues[key] = resolved;
    if (!profileHasValue(resolved)) missingMandatory.push(key);
  });

  Object.entries(profile.display || {}).forEach(([key, paths]) => {
    displayValues[key] = resolveProfileValueByPaths(device, paths);
  });

  const mandatoryTotal = Object.keys(profile.mandatory || {}).length;
  const mandatoryAvailable = mandatoryTotal - missingMandatory.length;
  const displayTotal = Object.keys(profile.display || {}).length;
  const displayAvailable = Object.values(displayValues).filter((value) =>
    profileHasValue(value),
  ).length;

  const mandatoryCoverage =
    mandatoryTotal > 0 ? (mandatoryAvailable / mandatoryTotal) * 100 : 0;
  const displayCoverage =
    displayTotal > 0 ? (displayAvailable / displayTotal) * 100 : 0;

  const score = Number(
    Math.max(
      0,
      Math.min(100, mandatoryCoverage * 0.75 + displayCoverage * 0.25),
    ).toFixed(1),
  );

  return {
    type: normalizedType,
    mandatory_values: mandatoryValues,
    display_values: displayValues,
    mandatory_display: Object.fromEntries(
      Object.entries(mandatoryValues).map(([key, value]) => [
        key,
        toProfileDisplayValue(value),
      ]),
    ),
    display_display: Object.fromEntries(
      Object.entries(displayValues).map(([key, value]) => [
        key,
        toProfileDisplayValue(value),
      ]),
    ),
    missing_mandatory: missingMandatory,
    mandatory_coverage: Number(mandatoryCoverage.toFixed(1)),
    display_coverage: Number(displayCoverage.toFixed(1)),
    section_scores: {
      core: Number(mandatoryCoverage.toFixed(1)),
      display: Number(displayCoverage.toFixed(1)),
    },
    score,
  };
};

const toFiniteScore100 = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(100, n));
};

const mapScoreToDisplayBand = (
  score,
  minTarget = 80,
  maxTarget = 98,
  precision = 1,
) => {
  const normalized = toFiniteScore100(score);
  if (normalized == null) return null;

  const minValue = Number(minTarget);
  const maxValue = Number(maxTarget);
  if (
    !Number.isFinite(minValue) ||
    !Number.isFinite(maxValue) ||
    maxValue <= minValue
  ) {
    return normalized;
  }

  const mapped = minValue + (normalized / 100) * (maxValue - minValue);
  return Number(mapped.toFixed(precision));
};

const buildSpecScoreSource = (type, row) => {
  const item = toPlainObject(row);
  const normalizedType = normalizeProfileDeviceType(type || item.product_type);

  if (normalizedType === "laptop") {
    const basicInfo = toPlainObject(
      parseJsonLikeValue(item.basic_info || item.basicInfo),
    );
    const metadata = toPlainObject(
      parseJsonLikeValue(item.metadata || item.meta),
    );
    const specSections = toPlainObject(parseJsonLikeValue(item.spec_sections));
    const variants = Array.isArray(item.variants)
      ? item.variants
      : Array.isArray(metadata.variants)
        ? metadata.variants
        : Array.isArray(specSections.variants_json)
          ? specSections.variants_json
          : [];
    const images = Array.isArray(item.images)
      ? item.images
      : Array.isArray(metadata.images)
        ? metadata.images
        : Array.isArray(specSections.images_json)
          ? specSections.images_json
          : [];

    return {
      ...item,
      name: item.name || basicInfo.product_name || basicInfo.title || null,
      brand_name:
        item.brand_name || basicInfo.brand_name || basicInfo.brand || null,
      model: item.model || basicInfo.model || null,
      performance: toPlainObject(item.performance || item.cpu),
      display: toPlainObject(item.display),
      memory: toPlainObject(item.memory),
      storage: toPlainObject(item.storage),
      battery: toPlainObject(item.battery),
      software: toPlainObject(item.software),
      physical: toPlainObject(item.physical),
      connectivity: toPlainObject(item.connectivity || metadata.connectivity),
      ports: toPlainObject(item.ports),
      multimedia: toPlainObject(item.multimedia),
      security: toPlainObject(item.security),
      camera: toPlainObject(item.camera),
      warranty: toPlainObject(item.warranty || metadata.warranty),
      variants,
      images,
    };
  }

  if (normalizedType === "tv") {
    const variants = Array.isArray(item.variants)
      ? item.variants
      : Array.isArray(item.variants_json)
        ? item.variants_json
        : [];
    const images = Array.isArray(item.images)
      ? item.images
      : Array.isArray(item.images_json)
        ? item.images_json
        : [];
    return {
      ...item,
      name: item.name || item.product_name || null,
      brand_name: item.brand_name || item.brand || null,
      variants,
      images,
    };
  }

  return {
    ...item,
    variants: Array.isArray(item.variants)
      ? item.variants
      : Array.isArray(item.variants_json)
        ? item.variants_json
        : [],
    images: Array.isArray(item.images)
      ? item.images
      : Array.isArray(item.images_json)
        ? item.images_json
        : [],
  };
};

const toFiniteNumberOrNull = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const extractLargestNumber = (value) => {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const matches = String(value).match(/(\d+(?:\.\d+)?)/g);
  if (!matches || !matches.length) return null;
  const nums = matches.map((m) => Number(m)).filter((n) => Number.isFinite(n));
  if (!nums.length) return null;
  return Math.max(...nums);
};

const clampScore01 = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
};

const safePowNormalize = (value, min, max, power = 1.25) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const lo = Number(min);
  const hi = Number(max);
  if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi <= lo) return null;
  const ratio = clampScore01((n - lo) / (hi - lo));
  return Math.pow(ratio, power) * 100;
};

const safeLogNormalize = (value, min, max, power = 1.1) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const lo = Number(min);
  const hi = Number(max);
  if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi <= lo) return null;
  const shifted = Math.max(lo, n);
  const top = Math.log1p(hi - lo);
  if (!Number.isFinite(top) || top <= 0) return null;
  const ratio = clampScore01(Math.log1p(shifted - lo) / top);
  return Math.pow(ratio, power) * 100;
};

const readSmartphonePrice = (source) => {
  const directCandidates = [
    source?.price,
    source?.base_price,
    source?.starting_price,
    source?.min_store_price,
    source?.min_base_price,
  ];
  for (const candidate of directCandidates) {
    const parsed = toFiniteNumberOrNull(candidate);
    if (parsed != null && parsed > 0) return parsed;
  }

  const variants = Array.isArray(source?.variants) ? source.variants : [];
  const prices = [];
  for (const variant of variants) {
    const basePrice = toFiniteNumberOrNull(
      variant?.base_price ?? variant?.price ?? variant?.amount,
    );
    if (basePrice != null && basePrice > 0) prices.push(basePrice);

    const stores = Array.isArray(variant?.store_prices)
      ? variant.store_prices
      : [];
    for (const store of stores) {
      const storePrice = toFiniteNumberOrNull(store?.price ?? store?.amount);
      if (storePrice != null && storePrice > 0) prices.push(storePrice);
    }
  }

  if (!prices.length) return null;
  return Math.min(...prices);
};

const readSmartphoneBatteryMah = (source) => {
  const battery = toPlainObject(source?.battery);
  const rawCandidates = [
    battery?.battery_capacity_mah,
    battery?.battery_capacity,
    battery?.capacity_mah,
    battery?.capacity,
    battery?.battery,
    source?.battery_capacity_mah,
    source?.battery_capacity,
  ];
  for (const candidate of rawCandidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 1000) return num;
  }
  return null;
};

const readSmartphoneChargingWatt = (source) => {
  const battery = toPlainObject(source?.battery);
  const rawCandidates = [
    battery?.fast_charging,
    battery?.charging_speed,
    battery?.charging,
    source?.fast_charging,
  ];
  for (const candidate of rawCandidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 5) return num;
  }
  return null;
};

const readSmartphoneRefreshRateHz = (source) => {
  const display = toPlainObject(source?.display);
  const rawCandidates = [
    display?.refresh_rate,
    display?.refreshRate,
    display?.hz,
  ];
  for (const candidate of rawCandidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 30) return num;
  }
  return null;
};

const readSmartphoneMainCameraMp = (source) => {
  const camera = toPlainObject(source?.camera);
  const rear = toPlainObject(camera?.rear_camera);
  const main = toPlainObject(
    rear?.main_camera || rear?.main || rear?.primary || camera?.main || {},
  );

  const rawCandidates = [
    camera?.main_camera_megapixels,
    camera?.main_camera,
    main?.resolution,
    main?.megapixels,
    rear?.main_camera_megapixels,
  ];

  for (const candidate of rawCandidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 2) return num;
  }
  return null;
};

const readSmartphoneFrontCameraMp = (source) => {
  const camera = toPlainObject(source?.camera);
  const front = toPlainObject(camera?.front_camera);
  const rawCandidates = [
    camera?.front_camera_megapixels,
    camera?.selfie_camera,
    front?.resolution,
    front?.megapixels,
    front?.main_camera_megapixels,
    camera?.front_camera,
  ];

  for (const candidate of rawCandidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 2) return num;
  }
  return null;
};

const countSmartphoneRearCameraLenses = (source) => {
  const camera = toPlainObject(source?.camera);
  const rear = camera?.rear_camera;

  if (rear && typeof rear === "object" && !Array.isArray(rear)) {
    const count = Object.values(rear).filter((value) =>
      profileHasValue(value),
    ).length;
    if (count > 0) return count;
  }

  if (Array.isArray(rear)) {
    const count = rear.filter((value) => profileHasValue(value)).length;
    if (count > 0) return count;
  }

  const fallbackKeys = [
    "main_camera",
    "main",
    "primary",
    "ultra_wide",
    "ultra_wide_camera",
    "telephoto",
    "periscope",
    "macro",
    "depth",
  ];
  const fallbackCount = fallbackKeys.filter((key) =>
    profileHasValue(camera?.[key]),
  ).length;
  if (fallbackCount > 0) return fallbackCount;

  const rearText = String(rear || camera?.main_camera || camera?.main || "");
  const mpMentions = rearText.match(/(\d+(?:\.\d+)?)\s*mp/gi);
  if (mpMentions?.length) return mpMentions.length;

  return null;
};

const computeSmartphoneCameraRawScore = (source) => {
  const mainMp = readSmartphoneMainCameraMp(source);
  const frontMp = readSmartphoneFrontCameraMp(source);
  const lensCount = countSmartphoneRearCameraLenses(source);

  const rearScore = safeLogNormalize(mainMp, 8, 200, 0.95);
  const lensScore = safePowNormalize(lensCount, 1, 4, 0.72);
  const frontScore = safeLogNormalize(frontMp, 5, 60, 1.0);

  const weightedParts = [
    { score: rearScore, weight: 0.62 },
    { score: lensScore, weight: 0.23 },
    { score: frontScore, weight: 0.15 },
  ].filter((part) => Number.isFinite(part.score));

  if (!weightedParts.length) return null;

  const totalWeight = weightedParts.reduce((sum, part) => sum + part.weight, 0);
  if (!Number.isFinite(totalWeight) || totalWeight <= 0) return null;

  const baseScore =
    weightedParts.reduce((sum, part) => sum + part.score * part.weight, 0) /
    totalWeight;
  const lensBoost = Number.isFinite(lensCount)
    ? Math.min(8, Math.max(0, lensCount - 1) * 2.5)
    : 0;
  const boosted = Math.min(100, baseScore + 6 + lensBoost);

  return toFiniteScore100(Number(boosted.toFixed(1)));
};

const readSmartphoneRamGb = (source) => {
  const perf = toPlainObject(source?.performance);
  const candidates = [perf?.ram, source?.ram];
  for (const candidate of candidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 1) return num;
  }

  const variants = Array.isArray(source?.variants) ? source.variants : [];
  const values = variants
    .map((variant) => extractLargestNumber(variant?.ram))
    .filter((n) => n != null && n >= 1);
  if (!values.length) return null;
  return Math.max(...values);
};

const readSmartphoneStorageGb = (source) => {
  const perf = toPlainObject(source?.performance);
  const candidates = [perf?.storage, source?.storage];
  for (const candidate of candidates) {
    const num = extractLargestNumber(candidate);
    if (num != null && num >= 8) return num;
  }

  const variants = Array.isArray(source?.variants) ? source.variants : [];
  const values = variants
    .map((variant) => extractLargestNumber(variant?.storage))
    .filter((n) => n != null && n >= 8);
  if (!values.length) return null;
  return Math.max(...values);
};

const readSmartphoneProcessorTier = (source) => {
  const text = String(
    source?.performance?.processor ||
      source?.processor ||
      source?.performance_json?.processor ||
      "",
  )
    .trim()
    .toLowerCase();
  if (!text) return null;

  const rules = [
    {
      pattern:
        /(snapdragon\s*8|dimensity\s*9|apple\s*a1[67-9]|apple\s*a2[0-9]|tensor\s*g[45]|exynos\s*24)/i,
      score: 98,
    },
    {
      pattern:
        /(snapdragon\s*7|dimensity\s*8|apple\s*a1[4-6]|tensor\s*g[23]|exynos\s*14)/i,
      score: 84,
    },
    {
      pattern:
        /(snapdragon\s*[46]|dimensity\s*[67]|helio\s*g9|apple\s*a1[12])/i,
      score: 68,
    },
    { pattern: /(helio|unisoc|t6|g8)/i, score: 52 },
  ];

  for (const rule of rules) {
    if (rule.pattern.test(text)) return rule.score;
  }

  return 60;
};

const getSmartphonePriceBand = (price) => {
  const value = Number(price);
  if (!Number.isFinite(value) || value <= 0) return "unknown";
  if (value <= 10000) return "under_10000";
  if (value <= 15000) return "under_15000";
  if (value <= 20000) return "under_20000";
  if (value <= 25000) return "under_25000";
  if (value <= 30000) return "under_30000";
  if (value <= 40000) return "under_40000";
  if (value <= 50000) return "under_50000";
  return "above_50000";
};

const getSmartphonePriceCeiling = (price) => {
  const value = Number(price);
  if (!Number.isFinite(value) || value <= 0) return 92;
  if (value <= 12000) return 84;
  if (value <= 18000) return 88;
  if (value <= 25000) return 91;
  if (value <= 35000) return 94;
  if (value <= 50000) return 97;
  return 100;
};

const toSpecTier = (score) => {
  const value = Number(score);
  if (!Number.isFinite(value)) return "Unrated";
  if (value >= 90) return "Elite";
  if (value >= 80) return "Premium";
  if (value >= 70) return "Upper Mid";
  if (value >= 60) return "Mid";
  return "Entry";
};

const computeSmartphoneRawSpecScoreV2 = (source, fieldProfile) => {
  const batteryMah = readSmartphoneBatteryMah(source);
  const chargingW = readSmartphoneChargingWatt(source);
  const refreshHz = readSmartphoneRefreshRateHz(source);
  const cameraMp = readSmartphoneMainCameraMp(source);
  const cameraQuality = computeSmartphoneCameraRawScore(source);
  const ramGb = readSmartphoneRamGb(source);
  const storageGb = readSmartphoneStorageGb(source);
  const processorTier = readSmartphoneProcessorTier(source);
  const price = readSmartphonePrice(source);

  const featureScores = {
    processor: toFiniteScore100(processorTier),
    display: safePowNormalize(refreshHz, 60, 165, 1.18),
    camera:
      toFiniteScore100(cameraQuality) ??
      safeLogNormalize(cameraMp, 12, 200, 1.25),
    battery: safeLogNormalize(batteryMah, 3000, 7500, 1.32),
    charging: safeLogNormalize(chargingW, 10, 150, 1.2),
    ram: safeLogNormalize(ramGb, 4, 24, 1.25),
    storage: safeLogNormalize(storageGb, 64, 1024, 1.18),
  };

  const weights = {
    processor: 0.24,
    display: 0.16,
    camera: 0.3,
    battery: 0.14,
    charging: 0.06,
    ram: 0.06,
    storage: 0.04,
  };

  let weightedTotal = 0;
  let weightTotal = 0;
  Object.entries(weights).forEach(([key, weight]) => {
    const score = featureScores[key];
    if (!Number.isFinite(score)) return;
    weightedTotal += score * weight;
    weightTotal += weight;
  });

  const profileScore = toFiniteScore100(fieldProfile?.score);
  let rawScore = null;
  let sourceKey = "model_v2_feature_raw";

  if (weightTotal > 0) {
    rawScore = weightedTotal / weightTotal;
  } else if (profileScore != null) {
    rawScore = profileScore;
    sourceKey = "model_v2_profile_fallback";
  }

  if (rawScore == null) {
    return {
      rawScore: null,
      source: "model_v2_unavailable",
      price,
      priceBand: getSmartphonePriceBand(price),
      featureCoverage: 0,
    };
  }

  const coverageRatio = clampScore01(weightTotal);
  const completenessMultiplier = 0.8 + 0.2 * coverageRatio;
  let adjusted = rawScore * completenessMultiplier;

  if (price != null) {
    adjusted = Math.min(adjusted, getSmartphonePriceCeiling(price));
  }

  const finalRawScore = Number(Math.max(0, Math.min(100, adjusted)).toFixed(1));

  return {
    rawScore: finalRawScore,
    source: sourceKey,
    price,
    priceBand: getSmartphonePriceBand(price),
    featureCoverage: Number((coverageRatio * 100).toFixed(1)),
  };
};

const computePercentileScore = (value, sortedValues) => {
  const n = Array.isArray(sortedValues) ? sortedValues.length : 0;
  if (!n) return null;
  if (n === 1) return 100;

  const target = Number(value);
  if (!Number.isFinite(target)) return null;

  let countLessOrEqual = 0;
  for (const item of sortedValues) {
    if (item <= target) countLessOrEqual += 1;
  }

  const rank = Math.max(1, countLessOrEqual);
  const percentile = ((rank - 1) / (n - 1)) * 100;
  return Number(Math.max(0, Math.min(100, percentile)).toFixed(1));
};

const applySpecScoreToRow = (type, row, profiles) => {
  if (!row || typeof row !== "object" || Array.isArray(row)) return row;

  const source = buildSpecScoreSource(type, row);
  const normalizedType = normalizeProfileDeviceType(
    type || source?.product_type,
  );
  const fieldProfile = resolveDeviceFieldProfileScore(type, source, profiles);

  const providedSpecScore = toFiniteScore100(row.spec_score ?? row.specScore);
  const specScore =
    providedSpecScore != null ? providedSpecScore : fieldProfile.score;
  const specScoreSource =
    providedSpecScore != null ? "provided" : "profile_fallback";

  const providedOverallScore = toFiniteScore100(
    row.overall_score ?? row.overallScore,
  );
  const overallScore =
    providedOverallScore != null ? providedOverallScore : specScore;
  const overallScoreSource =
    providedOverallScore != null
      ? "provided"
      : providedSpecScore != null
        ? "derived_from_spec_score"
        : "profile_fallback";

  let specScoreV2 = null;
  let specScoreV2Source = "model_v2_unavailable";
  let overallScoreV2 = null;
  let overallScoreV2Source = "model_v2_unavailable";
  let specScoreV2Raw = null;
  let specScorePrice = null;
  let specScorePriceBand = "unknown";
  let specFeatureCoverage = null;
  let specScoreV2Display8098 = null;
  let overallScoreV2Display8098 = null;
  let cameraScoreV2Raw = null;
  let cameraScoreV2Display8099 = null;
  let specScoreV2Breakdown = null;
  let specScoreV2MatchedFeatures = null;
  let specScoreV2CategoryCoverage = null;
  let specScoreV2Version = null;

  if (normalizedType === "smartphone") {
    const v2 = computeSmartphoneRawSpecScoreV2(source, fieldProfile);
    specScoreV2 = toFiniteScore100(v2.rawScore);
    specScoreV2Raw = specScoreV2;
    specScoreV2Source = v2.source;
    overallScoreV2 = specScoreV2;
    overallScoreV2Source =
      specScoreV2 != null ? "model_v2_raw" : "model_v2_unavailable";
    specScorePrice = toFiniteNumberOrNull(v2.price);
    specScorePriceBand = v2.priceBand || "unknown";
    specFeatureCoverage = toFiniteNumberOrNull(v2.featureCoverage);
    specScoreV2Display8098 = mapScoreToDisplayBand(specScoreV2);
    overallScoreV2Display8098 = specScoreV2Display8098;

    cameraScoreV2Raw = computeSmartphoneCameraRawScore(source);
    cameraScoreV2Display8099 =
      cameraScoreV2Raw != null
        ? mapScoreToDisplayBand(cameraScoreV2Raw, 80, 99)
        : null;
  } else if (normalizedType === "tv") {
    const v2 = computeTvRawSpecScoreV2(source);
    specScoreV2 = toFiniteScore100(v2.rawScore);
    specScoreV2Raw = specScoreV2;
    specScoreV2Source = v2.source;
    overallScoreV2 = specScoreV2;
    overallScoreV2Source =
      specScoreV2 != null ? v2.source : "tv_spec_score_v1_unavailable";
    specScorePrice = toFiniteNumberOrNull(v2.price);
    specScorePriceBand = v2.priceBand || "unknown";
    specFeatureCoverage = toFiniteNumberOrNull(v2.featureCoverage);
    specScoreV2Display8098 = mapScoreToDisplayBand(specScoreV2);
    overallScoreV2Display8098 = specScoreV2Display8098;
    specScoreV2Breakdown = v2.breakdown || null;
    specScoreV2MatchedFeatures = Array.isArray(v2.matchedFeatures)
      ? v2.matchedFeatures
      : null;
    specScoreV2CategoryCoverage = v2.categoryCoverage || null;
    specScoreV2Version = v2.version || null;
  } else if (normalizedType === "laptop") {
    const v2 = computeLaptopRawSpecScoreV2(source);
    specScoreV2 = toFiniteScore100(v2.rawScore);
    specScoreV2Raw = specScoreV2;
    specScoreV2Source = v2.source;
    overallScoreV2 = specScoreV2;
    overallScoreV2Source =
      specScoreV2 != null ? v2.source : "laptop_spec_score_v1_unavailable";
    specScorePrice = toFiniteNumberOrNull(v2.price);
    specScorePriceBand = v2.priceBand || "unknown";
    specFeatureCoverage = toFiniteNumberOrNull(v2.featureCoverage);
    specScoreV2Display8098 = mapScoreToDisplayBand(specScoreV2);
    overallScoreV2Display8098 = specScoreV2Display8098;
    specScoreV2Breakdown = v2.breakdown || null;
    specScoreV2MatchedFeatures = Array.isArray(v2.matchedFeatures)
      ? v2.matchedFeatures
      : null;
    specScoreV2CategoryCoverage = v2.categoryCoverage || null;
    specScoreV2Version = v2.version || null;
  }

  const cameraWithScore =
    cameraScoreV2Display8099 != null
      ? {
          ...toPlainObject(row.camera || source?.camera),
          score: cameraScoreV2Display8099,
        }
      : row.camera;
  const cameraJsonWithScore =
    cameraScoreV2Display8099 != null
      ? {
          ...toPlainObject(row.camera_json || row.camera || source?.camera),
          score: cameraScoreV2Display8099,
        }
      : row.camera_json;
  const usesDedicatedSpecScoreV2 =
    normalizedType === "tv" || normalizedType === "laptop";
  const outputSpecScore = usesDedicatedSpecScoreV2 ? specScoreV2 : specScore;
  const outputSpecScoreSource =
    usesDedicatedSpecScoreV2 ? specScoreV2Source : specScoreSource;
  const outputOverallScore =
    usesDedicatedSpecScoreV2 ? overallScoreV2 : overallScore;
  const outputOverallScoreSource =
    usesDedicatedSpecScoreV2 ? overallScoreV2Source : overallScoreSource;
  const outputSpecScoreDisplay =
    usesDedicatedSpecScoreV2
      ? specScoreV2
      : row.spec_score_display ?? row.specScoreDisplay;
  const outputOverallScoreDisplay =
    usesDedicatedSpecScoreV2
      ? specScoreV2
      : row.overall_score_display ?? row.overallScoreDisplay;
  const outputSpecScoreV2Display =
    usesDedicatedSpecScoreV2
      ? specScoreV2
      : row.spec_score_v2_display ?? row.specScoreV2Display;
  const outputOverallScoreV2Display =
    usesDedicatedSpecScoreV2
      ? specScoreV2
      : row.overall_score_v2_display ?? row.overallScoreV2Display;

  return {
    ...row,
    camera: cameraWithScore,
    camera_json: cameraJsonWithScore,
    field_profile: fieldProfile,
    spec_score: outputSpecScore,
    spec_score_source: outputSpecScoreSource,
    overall_score: outputOverallScore,
    overall_score_source: outputOverallScoreSource,
    spec_score_display: outputSpecScoreDisplay,
    overall_score_display: outputOverallScoreDisplay,
    spec_score_v2_raw: specScoreV2Raw,
    spec_score_v2: specScoreV2,
    spec_score_v2_source: specScoreV2Source,
    overall_score_v2: overallScoreV2,
    overall_score_v2_source: overallScoreV2Source,
    spec_score_v2_display: outputSpecScoreV2Display,
    overall_score_v2_display: outputOverallScoreV2Display,
    spec_score_v2_display_80_98: specScoreV2Display8098,
    overall_score_v2_display_80_98: overallScoreV2Display8098,
    spec_score_price: specScorePrice,
    spec_score_price_band: specScorePriceBand,
    spec_score_feature_coverage: specFeatureCoverage,
    spec_score_v2_breakdown: specScoreV2Breakdown,
    spec_score_v2_matched_features: specScoreV2MatchedFeatures,
    spec_score_v2_category_coverage: specScoreV2CategoryCoverage,
    spec_score_v2_version: specScoreV2Version,
    camera_score_v2_raw: cameraScoreV2Raw,
    camera_score_v2_display_80_99: cameraScoreV2Display8099,
    spec_tier_v2: toSpecTier(specScoreV2),
  };
};

const applySpecScoreToRows = (type, rows, profiles) => {
  const normalizedType = normalizeProfileDeviceType(type);
  const scoredRows = (rows || []).map((row) =>
    applySpecScoreToRow(type, row, profiles),
  );

  if (normalizedType !== "smartphone" || scoredRows.length === 0) {
    return scoredRows;
  }

  const bandBuckets = new Map();
  scoredRows.forEach((row, index) => {
    const raw = toFiniteScore100(row?.spec_score_v2_raw);
    if (raw == null) return;

    const sourceKey = String(row?.spec_score_v2_source || "").toLowerCase();
    if (sourceKey.includes("fallback") || sourceKey.includes("unavailable"))
      return;

    const band = row?.spec_score_price_band || "unknown";
    if (!bandBuckets.has(band)) bandBuckets.set(band, []);
    bandBuckets.get(band).push({ index, raw });
  });

  const updated = new Map();
  for (const [, bucket] of bandBuckets.entries()) {
    if (!Array.isArray(bucket) || bucket.length < 8) continue;

    const sortedValues = bucket
      .map((item) => item.raw)
      .filter((value) => Number.isFinite(value))
      .sort((a, b) => a - b);

    if (!sortedValues.length) continue;

    bucket.forEach(({ index, raw }) => {
      const percentile = computePercentileScore(raw, sortedValues);
      if (percentile == null) return;

      const blended = Number((percentile * 0.68 + raw * 0.32).toFixed(1));
      const compressed = Number(
        (
          Math.pow(Math.max(0, Math.min(100, blended)) / 100, 1.06) * 100
        ).toFixed(1),
      );
      const display8098 = mapScoreToDisplayBand(compressed);

      updated.set(index, {
        spec_score_v2: compressed,
        overall_score_v2: compressed,
        spec_score_v2_source: "model_v2_segment_percentile",
        overall_score_v2_source: "model_v2_segment_percentile",
        spec_score_v2_display_80_98: display8098,
        overall_score_v2_display_80_98: display8098,
        spec_tier_v2: toSpecTier(compressed),
      });
    });
  }

  return scoredRows.map((row, index) => {
    const patch = updated.get(index);
    return patch ? { ...row, ...patch } : row;
  });
};

const TV_PUBLIC_SCORE_INTERNAL_KEYS = new Set([
  "field_profile",
  "spec_score",
  "spec_score_source",
  "overall_score",
  "overall_score_source",
  "spec_score_display",
  "overall_score_display",
  "spec_score_v2_raw",
  "spec_score_v2_source",
  "overall_score_v2",
  "overall_score_v2_source",
  "spec_score_v2_display",
  "overall_score_v2_display",
  "spec_score_v2_display_80_98",
  "overall_score_v2_display_80_98",
  "spec_score_price",
  "spec_score_price_band",
  "spec_score_feature_coverage",
  "spec_score_v2_breakdown",
  "spec_score_v2_matched_features",
  "spec_score_v2_category_coverage",
  "spec_score_v2_version",
  "camera_score_v2_raw",
  "camera_score_v2_display_80_99",
  "spec_tier_v2",
]);

const TV_CATALOG_INTERNAL_TREND_KEYS = new Set([
  "hook_score",
  "buyer_intent",
  "trend_velocity",
  "freshness",
  "hook_calculated_at",
]);

const omitResponseKeys = (row, omittedKeys) => {
  const cleaned = {};
  for (const [key, value] of Object.entries(row || {})) {
    if (omittedKeys.has(String(key).toLowerCase())) continue;
    cleaned[key] = value;
  }
  return cleaned;
};

const toPublicTvResponseRow = (row) =>
  omitResponseKeys(row, TV_PUBLIC_SCORE_INTERNAL_KEYS);

const toPublicTvCatalogResponseRow = (row) =>
  omitResponseKeys(toPublicTvResponseRow(row), TV_CATALOG_INTERNAL_TREND_KEYS);

const toPublicLaptopResponseRow = (row) =>
  omitResponseKeys(row, TV_PUBLIC_SCORE_INTERNAL_KEYS);

const toPublicLaptopCatalogResponseRow = (row) =>
  omitResponseKeys(
    toPublicLaptopResponseRow(row),
    TV_CATALOG_INTERNAL_TREND_KEYS,
  );

const BLOG_ALLOWED_PRODUCT_TYPES = new Set(["smartphone", "laptop", "tv"]);
const BLOG_ALLOWED_STATUSES = new Set(["draft", "published"]);
const BLOG_ALLOWED_CATEGORIES = new Set([
  "news",
  "mobiles",
  "gadgets",
  "guides",
  "launches",
]);

const parseBlogTags = (value) => {
  if (Array.isArray(value)) {
    return Array.from(
      new Set(value.map((item) => String(item || "").trim()).filter(Boolean)),
    );
  }

  const raw = String(value || "").trim();
  if (!raw) return [];

  return Array.from(
    new Set(
      raw
        .split(/[,;\n]+/)
        .map((item) => String(item || "").trim())
        .filter(Boolean),
    ),
  );
};

const parseBlogBoolean = (value) => {
  if (typeof value === "boolean") return value;
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
};

const parseBlogDate = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return date;
};

const normalizePushToken = (value) => {
  const token = String(value || "").trim();
  return token.length >= 20 ? token : "";
};

const normalizePushTopic = (value) => {
  const topic = String(value || NEWS_PUSH_TOPIC)
    .trim()
    .toLowerCase();
  return topic === NEWS_PUSH_TOPIC ? topic : "";
};

const normalizePushPermission = (value) => {
  const permission = String(value || "")
    .trim()
    .toLowerCase();
  return ["granted", "default", "denied"].includes(permission)
    ? permission
    : null;
};

const toSafeFiniteNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toPositiveInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
};

const toBlogSlug = (value, fallback = "blog") => {
  const base = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  return base || fallback;
};

const normalizeBlogTokenKey = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");

const formatBlogValue = (value) => {
  const rendered = toProfileDisplayValue(value);
  if (!profileHasValue(rendered)) return "";
  return String(rendered).trim();
};

const formatBlogPrice = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "";
  return `₹${Math.round(n).toLocaleString("en-IN")}`;
};

const AFFILIATE_PLACEMENT_STATUSES = new Set([
  "draft",
  "published",
  "unpublished",
]);
const AFFILIATE_PLACEMENT_SCOPE_TYPES = new Set([
  "global",
  "product",
  "blog",
  "brand",
  "category",
]);
const AFFILIATE_SOURCE_TYPES = new Set(["manual", "auto"]);
const AFFILIATE_PAGE_TYPES = new Set([
  "product_list",
  "product_detail",
  "news",
]);
const AFFILIATE_LIST_SLOTS = new Set(["listing_featured", "product_card"]);
const AFFILIATE_DETAIL_SLOTS = new Set(["detail_highlight", "store_panel"]);
const AFFILIATE_NEWS_SLOTS = new Set(["inline_after_intro", "article_end"]);

const toNullableTrimmedText = (value, maxLength = 5000) => {
  if (value === null || value === undefined) return null;
  const cleaned = cleanText(value, maxLength);
  return cleaned ? String(cleaned).trim() : null;
};

const parseAffiliateBooleanInput = (value, fallback = false) => {
  if (value === true || value === false) return value;
  if (value === 1 || value === 0) return Boolean(value);
  if (value === null || value === undefined || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
};

const normalizeAffiliateStatus = (value, fallback = "draft") => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return AFFILIATE_PLACEMENT_STATUSES.has(normalized) ? normalized : fallback;
};

const normalizeAffiliateScopeType = (value, fallback = "global") => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return AFFILIATE_PLACEMENT_SCOPE_TYPES.has(normalized)
    ? normalized
    : fallback;
};

const normalizeAffiliateSourceType = (value, fallback = "manual") => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return AFFILIATE_SOURCE_TYPES.has(normalized) ? normalized : fallback;
};

const normalizeAffiliatePageType = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return AFFILIATE_PAGE_TYPES.has(normalized) ? normalized : "";
};

const normalizeAffiliateSlot = (pageType, value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  if (pageType === "product_list") {
    return AFFILIATE_LIST_SLOTS.has(normalized)
      ? normalized
      : "product_card";
  }
  if (pageType === "product_detail") {
    return AFFILIATE_DETAIL_SLOTS.has(normalized)
      ? normalized
      : "detail_highlight";
  }
  if (pageType === "news") {
    return AFFILIATE_NEWS_SLOTS.has(normalized)
      ? normalized
      : "inline_after_intro";
  }
  return normalized;
};

const normalizeAffiliateComparisonText = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const normalizeAffiliateComparisonUrl = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\/+$/g, "");

const normalizeAffiliateIdList = (value) => {
  const source = Array.isArray(value)
    ? value
    : value === null || value === undefined
      ? []
      : String(value)
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean);

  return Array.from(
    new Set(
      source
        .map((item) => Number(item))
        .filter((item) => Number.isInteger(item) && item > 0),
    ),
  );
};

const normalizeAffiliateSlug = (value, fallback = "affiliate-link") =>
  toBlogSlug(value, fallback).slice(0, 120);

const parseAffiliateDateValue = (value) => {
  if (value === undefined) return undefined;
  if (value === null || value === "") return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "__invalid__";
  return parsed.toISOString();
};

const addDaysToIsoDate = (isoValue, days) => {
  const start = new Date(isoValue);
  if (Number.isNaN(start.getTime())) return null;
  const next = new Date(start.getTime());
  next.setUTCDate(next.getUTCDate() + Math.max(0, Math.floor(days)));
  return next.toISOString();
};

const resolveAffiliateEffectiveUnpublishAt = (placement) => {
  if (!placement) return null;
  const explicitValue = placement.unpublish_at || placement.unpublishAt || null;
  if (explicitValue) return explicitValue;

  const durationDays = Number(placement.duration_days ?? placement.durationDays);
  if (!Number.isFinite(durationDays) || durationDays <= 0) return null;

  const startValue =
    placement.publish_at ||
    placement.publishAt ||
    placement.created_at ||
    placement.createdAt ||
    null;
  if (!startValue) return null;

  return addDaysToIsoDate(startValue, durationDays);
};

const getAffiliatePlacementLifecycleState = (placement, now = new Date()) => {
  const status = normalizeAffiliateStatus(placement?.status, "draft");
  if (status === "draft") return "draft";
  if (status === "unpublished") return "unpublished";

  const publishAtValue = placement?.publish_at || placement?.publishAt || null;
  if (publishAtValue) {
    const publishAt = new Date(publishAtValue);
    if (!Number.isNaN(publishAt.getTime()) && publishAt > now) {
      return "scheduled";
    }
  }

  const effectiveEndValue = resolveAffiliateEffectiveUnpublishAt(placement);
  if (effectiveEndValue) {
    const effectiveEnd = new Date(effectiveEndValue);
    if (!Number.isNaN(effectiveEnd.getTime()) && effectiveEnd < now) {
      return "expired";
    }
  }

  return status === "published" ? "active" : status;
};

const isAffiliatePlacementLive = (placement, now = new Date()) =>
  getAffiliatePlacementLifecycleState(placement, now) === "active";

const isAffiliatePageAllowed = (placement, pageType) => {
  if (!placement || !pageType) return false;
  if (pageType === "product_list")
    return parseAffiliateBooleanInput(
      placement.allow_product_list ?? placement.allowProductList,
      false,
    );
  if (pageType === "product_detail")
    return parseAffiliateBooleanInput(
      placement.allow_product_detail ?? placement.allowProductDetail,
      false,
    );
  if (pageType === "news")
    return parseAffiliateBooleanInput(
      placement.allow_news ?? placement.allowNews,
      false,
    );
  return false;
};

const resolveAffiliateCurrentSlot = (placement, pageType) => {
  if (pageType === "product_list") {
    return normalizeAffiliateSlot(
      pageType,
      placement?.list_slot ?? placement?.listSlot,
    );
  }
  if (pageType === "product_detail") {
    return normalizeAffiliateSlot(
      pageType,
      placement?.detail_slot ?? placement?.detailSlot,
    );
  }
  if (pageType === "news") {
    return normalizeAffiliateSlot(
      pageType,
      placement?.news_slot ?? placement?.newsSlot,
    );
  }
  return "";
};

const normalizeAffiliateDeviceType = (value, userAgent = "") => {
  const explicit = String(value || "")
    .trim()
    .toLowerCase();
  if (["mobile", "tablet", "desktop"].includes(explicit)) return explicit;

  const ua = String(userAgent || "").toLowerCase();
  if (/ipad|tablet|playbook|silk/i.test(ua)) return "tablet";
  if (/mobi|android|iphone|ipod/i.test(ua)) return "mobile";
  return "desktop";
};

const getAffiliateScopeSpecificityWeight = (matchType) => {
  switch (matchType) {
    case "blog":
      return 520;
    case "product":
      return 500;
    case "brand":
      return 360;
    case "category":
      return 320;
    case "global":
      return 180;
    default:
      return 0;
  }
};

const getAffiliateFreshnessWeight = (placement, now = new Date()) => {
  const publishedValue =
    placement?.publish_at ||
    placement?.publishAt ||
    placement?.created_at ||
    placement?.createdAt ||
    null;
  if (!publishedValue) return 0;

  const publishedAt = new Date(publishedValue);
  if (Number.isNaN(publishedAt.getTime())) return 0;

  const ageDays = Math.max(
    0,
    Math.floor((now.getTime() - publishedAt.getTime()) / 86400000),
  );

  if (ageDays <= 7) return 45;
  if (ageDays <= 30) return 30;
  if (ageDays <= 90) return 15;
  return 5;
};

const getAffiliateClickWeight = (placement) => {
  const totalClicks = Number(placement?.total_clicks ?? placement?.totalClicks);
  if (!Number.isFinite(totalClicks) || totalClicks <= 0) return 0;
  return Math.min(60, Math.round(Math.log10(totalClicks + 1) * 25));
};

const getAffiliateSourceWeight = (placement) =>
  normalizeAffiliateSourceType(placement?.source_type, "manual") === "manual"
    ? 60
    : 0;

const buildAffiliatePlacementScore = (placement, matchType, now = new Date()) => {
  const priority = Number(placement?.priority) || 0;
  return (
    priority * 100 +
    getAffiliateSourceWeight(placement) +
    getAffiliateScopeSpecificityWeight(matchType) +
    getAffiliateFreshnessWeight(placement, now) +
    getAffiliateClickWeight(placement) +
    (placement?.price ? 10 : 0)
  );
};

const normalizeAffiliatePlacementInput = (body = {}, { existing = null } = {}) => {
  const nowIso = new Date().toISOString();
  let publishAt = parseAffiliateDateValue(
    body.publish_at ?? body.publishAt ?? existing?.publish_at,
  );
  const unpublishAt = parseAffiliateDateValue(
    body.unpublish_at ?? body.unpublishAt ?? existing?.unpublish_at,
  );

  const status = normalizeAffiliateStatus(body.status ?? existing?.status, "draft");
  const scopeType = normalizeAffiliateScopeType(
    body.scope_type ?? body.scopeType ?? existing?.scope_type,
    "global",
  );
  const durationRaw =
    body.duration_days ?? body.durationDays ?? existing?.duration_days ?? null;
  const durationValue = Number(durationRaw);
  const durationDays =
    Number.isFinite(durationValue) && durationValue > 0
      ? Math.floor(durationValue)
      : null;

  if (status === "published" && (publishAt === undefined || publishAt === null)) {
    publishAt = existing?.publish_at || nowIso;
  }

  const priceRaw = body.price ?? existing?.price ?? null;
  const priceValue = Number(priceRaw);
  const price =
    Number.isFinite(priceValue) && priceValue > 0 ? priceValue : null;

  const payload = {
    name: toNullableTrimmedText(
      body.name ?? existing?.name ?? body.title ?? existing?.title,
      180,
    ),
    slug:
      body.slug === undefined
        ? existing?.slug ?? null
        : toNullableTrimmedText(body.slug, 160),
    title: toNullableTrimmedText(body.title ?? existing?.title, 220),
    description: toNullableTrimmedText(
      body.description ?? existing?.description,
      4000,
    ),
    cta_text: toNullableTrimmedText(
      body.cta_text ?? body.ctaText ?? existing?.cta_text,
      80,
    ),
    cta_subtext: toNullableTrimmedText(
      body.cta_subtext ?? body.ctaSubtext ?? existing?.cta_subtext,
      180,
    ),
    badge_text: toNullableTrimmedText(
      body.badge_text ?? body.badgeText ?? existing?.badge_text,
      40,
    ),
    disclosure_text: toNullableTrimmedText(
      body.disclosure_text ?? body.disclosureText ?? existing?.disclosure_text,
      180,
    ),
    store_name: toNullableTrimmedText(
      body.store_name ?? body.storeName ?? existing?.store_name,
      120,
    ),
    store_logo_url: toNullableTrimmedText(
      body.store_logo_url ?? body.storeLogoUrl ?? existing?.store_logo_url,
      2000,
    ),
    image_url: toNullableTrimmedText(
      body.image_url ?? body.imageUrl ?? existing?.image_url,
      2000,
    ),
    destination_url: toNullableTrimmedText(
      body.destination_url ?? body.destinationUrl ?? existing?.destination_url,
      2000,
    ),
    affiliate_url: toNullableTrimmedText(
      body.affiliate_url ?? body.affiliateUrl ?? existing?.affiliate_url,
      2000,
    ),
    price,
    currency_code:
      toNullableTrimmedText(
        body.currency_code ?? body.currencyCode ?? existing?.currency_code,
        12,
      ) || "INR",
    priority: Math.max(
      0,
      Math.floor(Number(body.priority ?? existing?.priority ?? 0) || 0),
    ),
    status,
    publish_at: publishAt === undefined ? existing?.publish_at ?? null : publishAt,
    unpublish_at:
      unpublishAt === undefined ? existing?.unpublish_at ?? null : unpublishAt,
    duration_days: durationDays,
    allow_product_list: parseAffiliateBooleanInput(
      body.allow_product_list ??
        body.allowProductList ??
        existing?.allow_product_list,
      false,
    ),
    allow_product_detail: parseAffiliateBooleanInput(
      body.allow_product_detail ??
        body.allowProductDetail ??
        existing?.allow_product_detail,
      false,
    ),
    allow_news: parseAffiliateBooleanInput(
      body.allow_news ?? body.allowNews ?? existing?.allow_news,
      false,
    ),
    scope_type: scopeType,
    product_id: toPositiveInt(
      body.product_id ?? body.productId ?? existing?.product_id,
      null,
    ),
    blog_id: toPositiveInt(body.blog_id ?? body.blogId ?? existing?.blog_id, null),
    brand_id: toPositiveInt(
      body.brand_id ?? body.brandId ?? existing?.brand_id,
      null,
    ),
    category_name: toNullableTrimmedText(
      body.category_name ?? body.categoryName ?? existing?.category_name,
      120,
    ),
    list_slot: normalizeAffiliateSlot(
      "product_list",
      body.list_slot ?? body.listSlot ?? existing?.list_slot,
    ),
    detail_slot: normalizeAffiliateSlot(
      "product_detail",
      body.detail_slot ?? body.detailSlot ?? existing?.detail_slot,
    ),
    news_slot: normalizeAffiliateSlot(
      "news",
      body.news_slot ?? body.newsSlot ?? existing?.news_slot,
    ),
  };

  const errors = [];
  if (!payload.name) errors.push("Name is required.");
  if (!payload.destination_url && !payload.affiliate_url) {
    errors.push("Add an affiliate URL or destination URL.");
  }
  if (
    !payload.allow_product_list &&
    !payload.allow_product_detail &&
    !payload.allow_news
  ) {
    errors.push("Select at least one page permission.");
  }
  if (payload.publish_at === "__invalid__") {
    errors.push("Publish date is invalid.");
  }
  if (payload.unpublish_at === "__invalid__") {
    errors.push("Unpublish date is invalid.");
  }
  if (scopeType === "product" && !payload.product_id) {
    errors.push("A product must be selected for product scope.");
  }
  if (scopeType === "blog" && !payload.blog_id) {
    errors.push("A news article must be selected for blog scope.");
  }
  if (scopeType === "brand" && !payload.brand_id) {
    errors.push("A brand must be selected for brand scope.");
  }
  if (scopeType === "category" && !payload.category_name) {
    errors.push("A category is required for category scope.");
  }

  if (payload.publish_at && payload.unpublish_at) {
    const publishDate = new Date(payload.publish_at);
    const unpublishDate = new Date(payload.unpublish_at);
    if (
      !Number.isNaN(publishDate.getTime()) &&
      !Number.isNaN(unpublishDate.getTime()) &&
      unpublishDate < publishDate
    ) {
      errors.push("Unpublish date must be after the publish date.");
    }
  }

  return { payload, errors };
};

const extractLowestVariantPrice = (variants = []) => {
  const numeric = [];
  for (const variant of Array.isArray(variants) ? variants : []) {
    const base = Number(variant?.base_price);
    if (Number.isFinite(base) && base > 0) numeric.push(base);
    const stores = Array.isArray(variant?.store_prices)
      ? variant.store_prices
      : [];
    for (const store of stores) {
      const price = Number(store?.price);
      if (Number.isFinite(price) && price > 0) numeric.push(price);
    }
  }
  if (!numeric.length) return null;
  return Math.min(...numeric);
};

const collectTemplateTokens = (content) => {
  if (!content || typeof content !== "string") return [];
  const tokenRegex = /\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g;
  const tokens = new Set();
  let match = tokenRegex.exec(content);
  while (match) {
    const key = normalizeBlogTokenKey(match[1]);
    if (key) tokens.add(key);
    match = tokenRegex.exec(content);
  }
  return Array.from(tokens);
};

const renderBlogTemplateWithTokens = (
  content,
  tokenMap,
  { preserveUnknown = true } = {},
) => {
  const source = String(content || "");
  const normalizedTokens = toPlainObject(tokenMap);
  return source.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (full, key) => {
    const normalizedKey = normalizeBlogTokenKey(key);
    if (!normalizedKey) return preserveUnknown ? full : "";
    const value = normalizedTokens[normalizedKey];
    if (!profileHasValue(value)) return preserveUnknown ? full : "";
    return String(value);
  });
};

const ensureBlogManagerAccess = async (req, res, action = "view") => {
  const role = normalizeRole(req?.user?.role || "viewer");
  const permissionMap = {
    view: ["content.news.view"],
    create: ["content.news.create"],
    edit: ["content.news.edit"],
    delete: ["content.news.delete"],
    manage: ["content.news.manage", "content.news.edit", "content.news.create"],
  };
  const requested = permissionMap[action] || permissionMap.view;
  const allowed = await hasRoleAnyPermissions(role, requested);
  if (allowed) return true;
  res.status(403).json({ message: "Content access required" });
  return false;
};

const readBlogProductDetailsByType = async (type, productId) => {
  if (type === "smartphone") {
    const result = await db.query(
      "SELECT * FROM smartphones WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    return result.rows[0] || {};
  }

  if (type === "laptop") {
    const result = await db.query(
      "SELECT * FROM laptop WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    return result.rows[0] || {};
  }

  if (type === "tv") {
    const result = await db.query(
      "SELECT * FROM tvs WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    return result.rows[0] || {};
  }

  return {};
};

const readBlogProductVariants = async (productId) => {
  const result = await db.query(
    `
    SELECT
      v.id AS variant_id,
      v.variant_key,
      v.attributes,
      v.base_price,
      COALESCE(
        (
          SELECT json_agg(
            jsonb_build_object(
              'id', sp.id,
              'store_name', sp.store_name,
              'price', sp.price,
              'url', sp.url,
              'offer_text', sp.offer_text,
              'delivery_info', sp.delivery_info
            )
            ORDER BY sp.price ASC NULLS LAST, sp.id ASC
          )
          FROM variant_store_prices sp
          WHERE sp.variant_id = v.id
        ),
        '[]'::json
      ) AS store_prices
    FROM product_variants v
    WHERE v.product_id = $1
    ORDER BY v.id ASC
  `,
    [productId],
  );

  return (result.rows || []).map((row) => {
    const attributes = toPlainObject(parseJsonLikeValue(row.attributes));
    return {
      ...row,
      ...attributes,
      store_prices: Array.isArray(row.store_prices) ? row.store_prices : [],
    };
  });
};

const readBlogProductImages = async (productId) => {
  const result = await db.query(
    `
    SELECT image_url
    FROM product_images
    WHERE product_id = $1
    ORDER BY position ASC NULLS LAST, id ASC
  `,
    [productId],
  );
  return (result.rows || [])
    .map((row) => String(row.image_url || "").trim())
    .filter(Boolean);
};

const collectImageCandidates = (...values) => {
  const candidates = [];
  const seen = new Set();

  const pushCandidate = (value) => {
    const parsed = parseJsonLikeValue(value);
    if (Array.isArray(parsed)) {
      parsed.forEach(pushCandidate);
      return;
    }

    if (parsed && typeof parsed === "object") {
      [
        parsed.image_url,
        parsed.hero_image,
        parsed.cover_image,
        parsed.thumbnail,
        parsed.url,
        parsed.src,
        parsed.image,
      ].forEach(pushCandidate);
      return;
    }

    const text = String(parsed || "").trim();
    if (!text || seen.has(text)) return;
    seen.add(text);
    candidates.push(text);
  };

  values.forEach(pushCandidate);
  return candidates;
};

const fetchBlogProductSnapshot = async (
  productId,
  profiles,
  baseRowOverride = null,
) => {
  const normalizedId = Number(productId);
  if (!Number.isInteger(normalizedId) || normalizedId <= 0) return null;

  let baseRow = baseRowOverride;
  if (!baseRow) {
    const baseResult = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        b.logo AS brand_logo,
        (to_jsonb(b)->>'website') AS brand_website,
        COALESCE(ds.hook_score, 0) AS hook_score,
        COALESCE(ds.buyer_intent, 0) AS buyer_intent,
        COALESCE(ds.trend_velocity, 0) AS trend_velocity,
        COALESCE(ds.freshness, 0) AS freshness,
        COALESCE(ts.views_7d, 0) AS views_7d,
        COALESCE(ts.compares_7d, 0) AS compares_7d,
        COALESCE(pub.is_published, false) AS is_published
      FROM products p
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      LEFT JOIN product_trending_score ts
        ON ts.product_id = p.id
      LEFT JOIN product_publish pub
        ON pub.product_id = p.id
      WHERE p.id = $1
      LIMIT 1
    `,
      [normalizedId],
    );
    baseRow = baseResult.rows[0] || null;
  }

  if (!baseRow) return null;

  const productType = normalizeProfileDeviceType(baseRow.product_type);
  if (!BLOG_ALLOWED_PRODUCT_TYPES.has(productType)) return null;

  const [detailRow, variants, images] = await Promise.all([
    readBlogProductDetailsByType(productType, normalizedId),
    readBlogProductVariants(normalizedId),
    readBlogProductImages(normalizedId),
  ]);
  const heroImage =
    collectImageCandidates(
      detailRow?.hero_image,
      detailRow?.image_url,
      detailRow?.cover_image,
      detailRow?.thumbnail,
      detailRow?.image,
      detailRow?.images,
      images,
    )[0] || null;

  const source = stripScoreRecursively({
    ...toPlainObject(detailRow),
    product_id: normalizedId,
    product_type: productType,
    name: baseRow.name || null,
    brand_name: baseRow.brand_name || null,
    hook_score: baseRow.hook_score ?? null,
    buyer_intent: baseRow.buyer_intent ?? null,
    trend_velocity: baseRow.trend_velocity ?? null,
    freshness: baseRow.freshness ?? null,
    variants,
    images,
  });

  const scored = applySpecScoreToRow(productType, source, profiles);
  const lowestPrice = extractLowestVariantPrice(variants);

  return {
    product_id: normalizedId,
    product_type: productType,
    core: {
      ...baseRow,
      product_type: productType,
      views_7d: toSafeFiniteNumber(baseRow.views_7d, 0),
      compares_7d: toSafeFiniteNumber(baseRow.compares_7d, 0),
      hook_score: toSafeFiniteNumber(baseRow.hook_score, 0),
      buyer_intent: toSafeFiniteNumber(baseRow.buyer_intent, 0),
      trend_velocity: toSafeFiniteNumber(baseRow.trend_velocity, 0),
      freshness: toSafeFiniteNumber(baseRow.freshness, 0),
    },
    scored,
    variants,
    images,
    lowest_price: lowestPrice,
    hero_image: heroImage,
  };
};

const buildBlogTokenMap = (snapshot) => {
  const scored = toPlainObject(snapshot?.scored);
  const productType = normalizeProfileDeviceType(snapshot?.product_type);
  const display = toPlainObject(scored.field_profile?.display_display);
  const mandatory = toPlainObject(scored.field_profile?.mandatory_display);
  const defaultProfile = toPlainObject(
    DEFAULT_DEVICE_FIELD_PROFILES[productType],
  );
  const defaultDisplayPaths = toPlainObject(defaultProfile.display);
  const defaultMandatoryPaths = toPlainObject(defaultProfile.mandatory);
  const tokenMap = {};

  const setToken = (key, value) => {
    const normalizedKey = normalizeBlogTokenKey(key);
    if (!normalizedKey) return;
    const formatted = formatBlogValue(value);
    if (!formatted) return;
    tokenMap[normalizedKey] = formatted;
  };

  const ensureToken = (key, ...candidates) => {
    const normalizedKey = normalizeBlogTokenKey(key);
    if (!normalizedKey || profileHasValue(tokenMap[normalizedKey])) return;

    for (const candidate of candidates) {
      if (!profileHasValue(candidate)) continue;
      setToken(key, candidate);
      if (profileHasValue(tokenMap[normalizedKey])) return;
    }
  };

  const resolveTokenValueByPaths = (...pathGroups) => {
    for (const paths of pathGroups) {
      if (!Array.isArray(paths) || !paths.length) continue;
      const resolved = resolveProfileValueByPaths(scored, paths);
      if (profileHasValue(resolved)) return resolved;
    }
    return null;
  };

  setToken("product_name", scored.name || snapshot?.core?.name);
  setToken(
    "brand",
    scored.brand_name || scored.brand || snapshot?.core?.brand_name,
  );
  setToken("product_type", snapshot?.product_type || scored.product_type);
  setToken("processor", display.processor || mandatory.processor);
  setToken("ram", display.ram || mandatory.ram);
  setToken("storage", display.storage || mandatory.storage);
  setToken(
    "display",
    display.display_size || display.screen_size || mandatory.display,
  );
  setToken("display_size", display.display_size || display.screen_size);
  setToken("resolution", display.resolution);
  setToken("refresh_rate", display.refresh_rate);
  setToken("battery", display.battery || mandatory.battery);
  setToken("main_camera", display.main_camera || mandatory.camera);
  setToken("os", display.os || mandatory.os);
  setToken("network", display.network || mandatory.network);
  setToken("panel_type", display.panel_type);
  setToken("audio_output", display.audio_output);
  setToken("energy_rating", display.energy_rating);
  setToken(
    "spec_score",
    `${toSafeFiniteNumber(scored.spec_score, 0).toFixed(1)}%`,
  );
  setToken("hero_image", snapshot?.hero_image || "");

  const priceText = formatBlogPrice(snapshot?.lowest_price);
  if (priceText) tokenMap.price = priceText;

  const images = Array.isArray(snapshot?.images) ? snapshot.images : [];
  images.slice(0, 6).forEach((url, index) => {
    setToken(`product_image_${index + 1}`, url);
  });

  for (const [key, value] of Object.entries(display)) {
    setToken(key, value);
  }
  for (const [key, value] of Object.entries(mandatory)) {
    setToken(key, value);
  }

  for (const [key, paths] of Object.entries(defaultDisplayPaths)) {
    ensureToken(key, resolveTokenValueByPaths(paths));
  }

  for (const [key, paths] of Object.entries(defaultMandatoryPaths)) {
    ensureToken(key, resolveTokenValueByPaths(paths));
  }

  ensureToken(
    "display",
    tokenMap.display_size,
    tokenMap.screen_size,
    resolveTokenValueByPaths(
      defaultMandatoryPaths.display,
      defaultDisplayPaths.display_size,
    ),
  );
  ensureToken(
    "main_camera",
    tokenMap.camera,
    resolveTokenValueByPaths(
      defaultDisplayPaths.main_camera,
      defaultMandatoryPaths.camera,
    ),
  );
  ensureToken(
    "processor",
    resolveTokenValueByPaths(
      defaultDisplayPaths.processor,
      defaultMandatoryPaths.processor,
    ),
  );
  ensureToken(
    "battery",
    resolveTokenValueByPaths(
      defaultDisplayPaths.battery,
      defaultMandatoryPaths.battery,
    ),
  );

  return tokenMap;
};

const buildBlogSuggestions = (snapshot, tokenMap) => {
  const type = normalizeProfileDeviceType(snapshot?.product_type);
  const templates = [];

  if (type === "smartphone") {
    templates.push(
      "{{product_name}} is powered by {{processor}} and features {{display}}.",
      "With {{battery}} battery and {{main_camera}} main camera, {{product_name}} is built for all-day use.",
      "At {{price}}, {{product_name}} offers a balanced mix of performance and value.",
    );
  } else if (type === "laptop") {
    templates.push(
      "{{product_name}} runs on {{processor}} with {{ram}} RAM and {{storage}} storage.",
      "It comes with {{display}} and is designed for daily productivity workloads.",
      "{{product_name}} is currently listed around {{price}}.",
    );
  } else if (type === "tv") {
    templates.push(
      "{{product_name}} offers {{display_size}} display with {{resolution}} resolution.",
      "With {{refresh_rate}} refresh rate and {{panel_type}} panel, it targets smooth everyday viewing.",
      "{{product_name}} is priced around {{price}} in current listings.",
    );
  }

  return templates.map((template, index) => ({
    id: index + 1,
    template,
    rendered: renderBlogTemplateWithTokens(template, tokenMap, {
      preserveUnknown: true,
    }),
  }));
};

const normalizeBlogProductIds = (...sources) => {
  const seen = new Set();
  const ids = [];

  const pushValue = (value) => {
    const normalized = Number(
      value && typeof value === "object"
        ? value.product_id ?? value.productId ?? value.id
        : value,
    );
    if (!Number.isInteger(normalized) || normalized <= 0 || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    ids.push(normalized);
  };

  sources.forEach((source) => {
    if (Array.isArray(source)) {
      source.forEach(pushValue);
      return;
    }

    if (source && typeof source === "object" && Array.isArray(source.product_ids)) {
      source.product_ids.forEach(pushValue);
      return;
    }

    pushValue(source);
  });

  return ids;
};

const orderBlogProductIds = (productIds = [], primaryProductId = null) => {
  const ordered = normalizeBlogProductIds(productIds);
  const primaryId = Number(primaryProductId);
  if (!Number.isInteger(primaryId) || primaryId <= 0) return ordered;
  if (!ordered.includes(primaryId)) return ordered;
  return [primaryId, ...ordered.filter((value) => value !== primaryId)];
};

const fetchBlogSnapshotsByProductIds = async (productIds = [], profiles = []) => {
  const normalizedIds = normalizeBlogProductIds(productIds);
  const snapshots = [];
  const missingIds = [];

  for (const productId of normalizedIds) {
    const snapshot = await fetchBlogProductSnapshot(productId, profiles);
    if (!snapshot) {
      missingIds.push(productId);
      continue;
    }
    snapshots.push(snapshot);
  }

  return {
    productIds: normalizedIds,
    snapshots,
    missingIds,
  };
};

const buildBlogProductSummary = (snapshot) => ({
  product_id: Number(snapshot?.product_id) || null,
  product_type: normalizeProfileDeviceType(snapshot?.product_type),
  name: String(snapshot?.core?.name || "").trim(),
  brand_name: String(snapshot?.core?.brand_name || "").trim(),
  spec_score: toSafeFiniteNumber(snapshot?.scored?.spec_score, 0),
  price: formatBlogPrice(snapshot?.lowest_price) || null,
  image: snapshot?.hero_image || null,
  images: Array.isArray(snapshot?.images) ? snapshot.images : [],
});

const buildBlogSelectionContext = (
  snapshots = [],
  requestedTokenMap = {},
) => {
  const validSnapshots = Array.isArray(snapshots)
    ? snapshots.filter((snapshot) => {
        const productId = Number(snapshot?.product_id);
        return Number.isInteger(productId) && productId > 0;
      })
    : [];
  const primarySnapshot = validSnapshots[0] || null;
  const mergedTokenMap = primarySnapshot ? buildBlogTokenMap(primarySnapshot) : {};
  const productSummaries = [];

  validSnapshots.forEach((snapshot, index) => {
    const productIndex = index + 1;
    const summary = buildBlogProductSummary(snapshot);
    const scopedTokenMap = buildBlogTokenMap(snapshot);
    const productName = summary.name || `Product ${productIndex}`;
    const productBrand = summary.brand_name || "";

    productSummaries.push(summary);

    mergedTokenMap[`product_${productIndex}_id`] = String(summary.product_id || "");
    mergedTokenMap[`product_${productIndex}_name`] = productName;
    mergedTokenMap[`product_${productIndex}_brand`] = productBrand;
    mergedTokenMap[`product_${productIndex}_type`] = summary.product_type || "";

    Object.entries(scopedTokenMap).forEach(([key, value]) => {
      if (!profileHasValue(value)) return;
      mergedTokenMap[`product_${productIndex}_${key}`] = value;
      if (productIndex === 1) {
        mergedTokenMap[`primary_${key}`] = value;
      }
    });
  });

  const productNames = productSummaries
    .map((product) => String(product?.name || "").trim())
    .filter(Boolean);
  const productBrands = productSummaries
    .map((product) => String(product?.brand_name || "").trim())
    .filter(Boolean);

  if (primarySnapshot) {
    mergedTokenMap.primary_product_id = String(primarySnapshot.product_id || "");
    mergedTokenMap.primary_product_type = normalizeProfileDeviceType(
      primarySnapshot.product_type,
    );
  }

  if (productNames.length) {
    mergedTokenMap.product_count = String(productNames.length);
    mergedTokenMap.product_names = productNames.join(", ");
    mergedTokenMap.primary_product_name = productNames[0];
  }

  if (productBrands.length) {
    mergedTokenMap.product_brands = productBrands.join(", ");
  }

  if (productNames.length > 1) {
    mergedTokenMap.secondary_product_names = productNames.slice(1).join(", ");
  }

  const tokenMap = {
    ...mergedTokenMap,
    ...toPlainObject(requestedTokenMap),
  };
  const tokenKeys = Object.keys(tokenMap).sort((left, right) =>
    left.localeCompare(right),
  );

  return {
    primarySnapshot,
    productIds: productSummaries
      .map((product) => Number(product?.product_id))
      .filter((value) => Number.isInteger(value) && value > 0),
    products: productSummaries,
    tokenMap,
    tokenKeys,
  };
};

const buildBlogSuggestionsForSelection = (snapshots = [], tokenMap = {}) => {
  const validSnapshots = Array.isArray(snapshots)
    ? snapshots.filter((snapshot) => {
        const productId = Number(snapshot?.product_id);
        return Number.isInteger(productId) && productId > 0;
      })
    : [];
  const primarySnapshot = validSnapshots[0] || null;
  const multiProductTemplates = [];

  if (validSnapshots.length > 1) {
    multiProductTemplates.push(
      "This roundup tracks {{product_names}} and highlights the biggest differences in pricing, positioning, and day-to-day value.",
      "Among the selected launches, {{product_1_name}} leads the story while {{secondary_product_names}} add the broader market context.",
    );

    if (validSnapshots.length >= 2) {
      multiProductTemplates.push(
        "{{product_1_name}} starts around {{product_1_price}}, while {{product_2_name}} is listed near {{product_2_price}} for buyers comparing the two releases.",
      );
    }
  }

  const primarySuggestions = primarySnapshot
    ? buildBlogSuggestions(primarySnapshot, tokenMap).map((entry) => entry.template)
    : [];
  const templates = [...multiProductTemplates, ...primarySuggestions];

  return templates.map((template, index) => ({
    id: index + 1,
    template,
    rendered: renderBlogTemplateWithTokens(template, tokenMap, {
      preserveUnknown: true,
    }),
  }));
};

const findExistingBlogByOrderedProductSet = async (
  productIds = [],
  queryable = db,
) => {
  const normalizedIds = normalizeBlogProductIds(productIds);
  if (!normalizedIds.length) return null;

  const result = await queryable.query(
    `
      SELECT
        bl.id,
        bl.product_id,
        bl.status,
        bl.slug
      FROM blogs bl
      WHERE COALESCE(
        (
          SELECT array_agg(bp.product_id ORDER BY bp.position ASC, bp.id ASC)
          FROM blog_products bp
          WHERE bp.blog_id = bl.id
        ),
        CASE
          WHEN bl.product_id IS NOT NULL THEN ARRAY[bl.product_id]::int[]
          ELSE ARRAY[]::int[]
        END
      ) = $1::int[]
      ORDER BY bl.updated_at DESC NULLS LAST, bl.id DESC
      LIMIT 1
    `,
    [normalizedIds],
  );

  return result.rows[0] || null;
};

const attachBlogProductsToRows = async (rows = []) => {
  if (!Array.isArray(rows) || rows.length === 0) return rows;

  const blogIds = Array.from(
    new Set(
      rows
        .map((row) => Number(row?.id))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );
  if (!blogIds.length) return rows;

  const result = await db.query(
    `
      SELECT
        bp.blog_id,
        bp.product_id,
        bp.position,
        p.name,
        p.product_type,
        b.name AS brand_name,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = bp.product_id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image
      FROM blog_products bp
      JOIN products p
        ON p.id = bp.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE bp.blog_id = ANY($1::int[])
      ORDER BY bp.blog_id ASC, bp.position ASC, bp.id ASC
    `,
    [blogIds],
  );

  const productsByBlogId = new Map();
  (result.rows || []).forEach((row) => {
    const blogId = Number(row?.blog_id);
    if (!Number.isInteger(blogId) || blogId <= 0) return;
    const current = productsByBlogId.get(blogId) || [];
    current.push({
      product_id: Number(row?.product_id) || null,
      product_type: normalizeProfileDeviceType(row?.product_type),
      name: String(row?.name || "").trim(),
      brand_name: String(row?.brand_name || "").trim(),
      image: row?.image || null,
    });
    productsByBlogId.set(blogId, current);
  });

  rows.forEach((row) => {
    const blogId = Number(row?.id);
    const existingProductMap = new Map(
      (Array.isArray(row?.products) ? row.products : [])
        .map((product) => [Number(product?.product_id), product])
        .filter(([productId]) => Number.isInteger(productId) && productId > 0),
    );
    const linkedProducts = (productsByBlogId.get(blogId) || []).map((product) => ({
      ...(existingProductMap.get(Number(product?.product_id)) || {}),
      ...product,
    }));
    const fallbackProducts =
      linkedProducts.length === 0 &&
      Number.isInteger(Number(row?.product_id)) &&
      Number(row?.product_id) > 0
        ? [
            {
              product_id: Number(row.product_id),
              product_type: normalizeProfileDeviceType(row?.product_type),
              name: String(row?.product_name || "").trim(),
              brand_name: String(row?.brand_name || "").trim(),
              image: row?.hero_image || null,
            },
          ]
        : [];
    const products = linkedProducts.length ? linkedProducts : fallbackProducts;
    const productIds = normalizeBlogProductIds(products);

    row.product_ids = productIds;
    row.products = products;
    row.linked_product_count = productIds.length;
    row.product_names = products
      .map((product) => String(product?.name || "").trim())
      .filter(Boolean)
      .join(", ");

    const primaryProduct = products[0] || null;
    if (primaryProduct) {
      row.primary_product_id = primaryProduct.product_id || null;
      row.product_id = row.product_id || primaryProduct.product_id || null;
      row.product_name = row.product_name || primaryProduct.name || "";
      row.product_type = row.product_type || primaryProduct.product_type || null;
      row.brand_name = row.brand_name || primaryProduct.brand_name || "";
    }
  });

  return rows;
};

const syncBlogProducts = async (queryable, blogId, productIds = []) => {
  const normalizedBlogId = Number(blogId);
  if (!Number.isInteger(normalizedBlogId) || normalizedBlogId <= 0) return;

  const orderedProductIds = normalizeBlogProductIds(productIds);
  await queryable.query(`DELETE FROM blog_products WHERE blog_id = $1`, [
    normalizedBlogId,
  ]);

  if (!orderedProductIds.length) return;

  await queryable.query(
    `
      INSERT INTO blog_products (blog_id, product_id, position)
      SELECT $1, linked_product_id, linked_position::int
      FROM unnest($2::int[]) WITH ORDINALITY AS linked(linked_product_id, linked_position)
    `,
    [normalizedBlogId, orderedProductIds],
  );
};

const resolvePublicBlogRow = async (
  row,
  profileConfig = null,
  snapshotCache = null,
) => {
  const blog = { ...row };
  if (!blog.author_name && blog.author_user_id) {
    const author = await resolveRbacUserById(blog.author_user_id).catch(
      () => null,
    );
    if (author?.display_name) {
      blog.author_name = author.display_name;
    }
  }
  const template = String(blog.content_template || "").trim();
  const productIds = normalizeBlogProductIds(blog.product_ids, blog.product_id);

  if (!template) return blog;

  let tokenMap = { ...toPlainObject(blog.token_snapshot) };

  if (productIds.length > 0) {
    const config =
      profileConfig ||
      (await readDeviceFieldProfilesConfig().catch(() => ({ profiles: [] })));
    const snapshots = [];

    for (const productId of productIds) {
      let snapshot =
        snapshotCache instanceof Map ? snapshotCache.get(productId) : undefined;

      if (typeof snapshot === "undefined") {
        snapshot = await fetchBlogProductSnapshot(productId, config?.profiles || []);
        if (snapshotCache instanceof Map) {
          snapshotCache.set(productId, snapshot || null);
        }
      }

      if (snapshot) {
        snapshots.push(snapshot);
      }
    }

    if (snapshots.length > 0) {
      const selectionContext = buildBlogSelectionContext(snapshots, tokenMap);
      const primarySnapshot = selectionContext.primarySnapshot;
      tokenMap = selectionContext.tokenMap;
      blog.product_ids = selectionContext.productIds;
      blog.products = selectionContext.products;
      blog.product_names = selectionContext.products
        .map((product) => String(product?.name || "").trim())
        .filter(Boolean)
        .join(", ");

      if (
        blog.hero_image_source !== "none" &&
        !blog.hero_image &&
        primarySnapshot?.hero_image
      ) {
        blog.hero_image = primarySnapshot.hero_image;
      }
      if (!blog.hero_image_source && blog.hero_image) {
        blog.hero_image_source =
          primarySnapshot?.hero_image && blog.hero_image === primarySnapshot.hero_image
            ? "asset"
            : "url";
      }
      if (!blog.brand_logo && primarySnapshot?.core?.brand_logo) {
        blog.brand_logo = primarySnapshot.core.brand_logo;
      }
      if (!blog.product_name && selectionContext.products[0]?.name) {
        blog.product_name = selectionContext.products[0].name;
      }
      if (!blog.product_type && selectionContext.products[0]?.product_type) {
        blog.product_type = selectionContext.products[0].product_type;
      }
      if (!blog.brand_name && selectionContext.products[0]?.brand_name) {
        blog.brand_name = selectionContext.products[0].brand_name;
      }
    } else {
      blog.product_ids = productIds;
    }
  }

  blog.content_rendered = renderBlogTemplateWithTokens(template, tokenMap, {
    preserveUnknown: false,
  });

  // Ensure proper HTML encoding for API responses
  blog.content_template = ensureProperHtmlEncoding(blog.content_template);
  blog.content_rendered = ensureProperHtmlEncoding(blog.content_rendered);

  return blog;
};

const resolveUniqueBlogSlug = async (
  requestedSlug,
  productId,
  blogId = null,
) => {
  const hasProductId =
    Number.isInteger(Number(productId)) && Number(productId) > 0;
  const fallbackSlug = hasProductId
    ? `product-${toPositiveInt(productId, Date.now())}`
    : `blog-${Date.now()}`;
  const baseSlug = toBlogSlug(requestedSlug, fallbackSlug);
  let slug = baseSlug;
  let counter = 2;

  while (true) {
    const existing = await db.query(
      "SELECT id, product_id FROM blogs WHERE slug = $1 LIMIT 1",
      [slug],
    );
    if (!existing.rows.length) return slug;

    const existingId = Number(existing.rows[0]?.id);
    if (
      Number.isInteger(Number(blogId)) &&
      Number(blogId) > 0 &&
      existingId === Number(blogId)
    ) {
      return slug;
    }
    slug = `${baseSlug}-${counter}`;
    counter += 1;
  }
};

/* -----------------------
  Migrations (all tables with   suffix)
------------------------*/
async function runMigrations() {
  try {
    // Helper to run migration queries but ignore duplicate pg_type errors
    async function safeQuery(sql, params = []) {
      try {
        await db.query(sql, params);
      } catch (err) {
        // Postgres may raise a unique violation on pg_type when a previous
        // failed attempt left a composite type with the same name.
        if (
          err &&
          err.code === "23505" &&
          err.constraint === "pg_type_typname_nsp_index"
        ) {
          console.warn(
            "Migration warning: duplicate pg_type detected, skipping:",
            err.detail || err.message,
          );
          return;
        }
        throw err;
      }
    }
    // Migrations - create tables in dependency order

    // 1) brands
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS brands (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        logo TEXT,
        category TEXT,
        status TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Add description column if it doesn't exist (ALTER TABLE method)
    await safeQuery(`
      ALTER TABLE brands
      ADD COLUMN IF NOT EXISTS description TEXT;
    `);

    await safeQuery(`
      ALTER TABLE brands
      ADD COLUMN IF NOT EXISTS website TEXT;
    `);

    // categories
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        product_type TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure there is no unique constraint on product_type (allow multiple
    // categories per product type). Drop the constraint if it exists.
    await safeQuery(`
      ALTER TABLE categories
      DROP CONSTRAINT IF EXISTS categories_product_type_key;
    `);

    // online_stores - stores used for variant store prices and links
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS online_stores (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        logo TEXT,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS store(
        id SERIAL PRIMARY KEY,
        store_name TEXT NOT NULL,
        logo_url TEXT NOT NULL,
        status TEXT,
        created_at TIMESTAMP DEFAULT now()
        );
        `);
    // 2) products (depends on brands)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        product_type TEXT CHECK (
          product_type IN ('smartphone','laptop','networking','tv','accessories')
        ) NOT NULL,
        brand_id INT REFERENCES brands(id),
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Existing installs may already have publish rows for legacy product types.
    // Remove those rows first so product cleanup below does not violate FK checks.
    await safeQuery(`
      DO $$
      BEGIN
        IF to_regclass('public.product_publish') IS NOT NULL THEN
          DELETE FROM product_publish
          WHERE product_id IN (
            SELECT id FROM products WHERE product_type = 'home_appliance'
          );
        END IF;
      END
      $$;
    `);

    // Remove legacy home_appliance products as the category is replaced by tv.
    await safeQuery(`
      DELETE FROM products
      WHERE product_type = 'home_appliance';
    `);

    // Ensure product_type check allows tv on existing installations.
    await safeQuery(`
      ALTER TABLE products
      DROP CONSTRAINT IF EXISTS products_product_type_check;
    `);
    await safeQuery(`
      ALTER TABLE products
      ADD CONSTRAINT products_product_type_check
      CHECK (
        product_type IN ('smartphone','laptop','networking','tv','accessories')
      );
    `);

    // 3) product_variants (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_variants (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        variant_key TEXT NOT NULL,
        attributes JSONB,
        base_price NUMERIC,
        created_at TIMESTAMP DEFAULT now(),
        CONSTRAINT unique_product_variant UNIQUE (product_id, variant_key)
      );
    `);

    // 4) product_images (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_images (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        image_url TEXT NOT NULL,
        position INT,
        UNIQUE (product_id, image_url)
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_variant_images (
        id SERIAL PRIMARY KEY,
        variant_id INT REFERENCES product_variants(id) ON DELETE CASCADE,
        image_url TEXT NOT NULL,
        position INT,
        UNIQUE (variant_id, image_url)
      );
    `);

    // 5) smartphones (depends on products)
    // Provide an internal id PK and keep product_id as FK to products
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS smartphones (
        id SERIAL PRIMARY KEY,
        product_id INT UNIQUE REFERENCES products(id) ON DELETE CASCADE,
        category TEXT,
        brand TEXT,
        model TEXT,
        launch_date DATE,
        official_preorder_url TEXT,
        launch_status_override TEXT,
        images JSONB,
        colors JSONB,
        build_design JSONB,
        display JSONB,
        performance JSONB,
        camera JSONB,
        battery JSONB,
        connectivity JSONB,
        network JSONB,
        ports JSONB,
        audio JSONB,
        multimedia JSONB,
        sensors JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure new `connectivity` and `network` columns exist for existing installations
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS connectivity JSONB;`,
    );
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS network JSONB;`,
    );
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS official_preorder_url TEXT;`,
    );
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS launch_status_override TEXT;`,
    );

    // If older `connectivity_network` column exists, copy its data into `connectivity` (preserve existing connectivity)
    await safeQuery(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='smartphones' AND column_name='connectivity_network'
        ) THEN
          UPDATE smartphones SET connectivity = connectivity_network WHERE connectivity IS NULL;
          -- optionally preserve the original column; no drop performed automatically
        END IF;
      END$$;
    `);

    // 6) smartphone_variants (depends on smartphones)

    // 7) variant_store_prices (depends on smartphone_variants)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS variant_store_prices (
        id SERIAL PRIMARY KEY,
        variant_id INT NOT NULL REFERENCES product_variants(id) ON DELETE CASCADE,
        store_name TEXT NOT NULL,
        price NUMERIC,
        url TEXT,
        offer_text TEXT,
        delivery_info TEXT,
        sale_start_date DATE,
        created_at TIMESTAMP DEFAULT now(),
        UNIQUE (variant_id, store_name)
      );
    `);
    await safeQuery(
      `ALTER TABLE variant_store_prices ADD COLUMN IF NOT EXISTS sale_start_date DATE;`,
    );

    // 8) smartphone_publish (depends on smartphones)

    // 8) Customers (customers table used by ratings)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS Customers(
        id SERIAL PRIMARY KEY,
        f_name TEXT NOT NULL,
        l_name TEXT NOT NULL,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        city TEXT,
        country TEXT,
        state TEXT,
        zip_code TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(
      `CREATE TABLE IF NOT EXISTS product_sphere_ratings (
        id SERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
      
        design JSONB,
        display JSONB,
        performance JSONB,
        camera JSONB,
        battery JSONB,
        connectivity JSONB,
        network JSONB,
      
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      
        UNIQUE(product_id)
      );
      `,
    );

    // 9) smartphone_ratings (depends on smartphones and Customers)

    // 10) tvs (depends on products)
    // Legacy home_appliance table is replaced by tvs.
    await safeQuery(`
      DROP TABLE IF EXISTS home_appliance CASCADE;
    `);
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS tvs (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        category TEXT,
        model TEXT,
        key_specs_json JSONB,
        basic_info_json JSONB,
        display_json JSONB,
        video_engine_json JSONB,
        audio_json JSONB,
        smart_tv_json JSONB,
        gaming_json JSONB,
        ports_json JSONB,
        connectivity_json JSONB,
        power_json JSONB,
        physical_json JSONB,
        product_details_json JSONB,
        in_the_box_json JSONB,
        warranty_json JSONB,
        images_json JSONB,
        variants_json JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 11) laptop (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS laptop (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        cpu JSONB,
        display JSONB,
        memory JSONB,
        storage JSONB,
        battery JSONB,
        connectivity JSONB,
        physical JSONB,
        software JSONB,
        features JSONB,
        warranty JSONB,
        meta JSONB,
        spec_sections JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure `meta` column exists for existing installations
    await safeQuery(`ALTER TABLE laptop ADD COLUMN IF NOT EXISTS meta JSONB;`);
    await safeQuery(
      `ALTER TABLE laptop ADD COLUMN IF NOT EXISTS spec_sections JSONB;`,
    );

    //12) networking (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS networking (
        product_id INT PRIMARY KEY
          REFERENCES products(id)
          ON DELETE CASCADE,
      
          device_type TEXT NOT NULL CHECK (
            device_type IN (
              'router',
              'modem',
              'switch',
              'mesh',
              'extender'
            )
          ),
                  model_number TEXT,
        release_year INT,
        country_of_origin TEXT,
      
        specifications JSONB,   -- speeds, ports, bands
        features JSONB,         -- QoS, parental control, MU-MIMO
        performance JSONB,      -- throughput, coverage
        connectivity JSONB,     -- wifi standards, ethernet
        physical_details JSONB, -- dimensions, weight
        warranty JSONB,
      
        created_at TIMESTAMP DEFAULT now()
      );
      `);

    // 12) ram_storage_long
    // Ensure legacy `long` column is renamed to `product_type` if present,
    // then create the table with `product_type` column.
    await safeQuery(`
      

      CREATE TABLE IF NOT EXISTS ram_storage_long (
        id SERIAL PRIMARY KEY,
        ram TEXT,
        storage TEXT,
        product_type TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 13) user (application users/admins)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS "user" (
        id SERIAL PRIMARY KEY,
        user_name TEXT,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        gender TEXT,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'admin',
        display_name TEXT,
        bio TEXT,
        department TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        avatar TEXT,
        permissions_override JSONB NOT NULL DEFAULT '[]'::jsonb,
        last_login TIMESTAMPTZ,
        updated_at TIMESTAMP DEFAULT now(),
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS display_name TEXT,
      ADD COLUMN IF NOT EXISTS bio TEXT,
      ADD COLUMN IF NOT EXISTS department TEXT,
      ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
      ADD COLUMN IF NOT EXISTS avatar TEXT,
      ADD COLUMN IF NOT EXISTS permissions_override JSONB NOT NULL DEFAULT '[]'::jsonb,
      ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_roles (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
        built_in BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_permissions (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        module_key TEXT,
        action TEXT,
        built_in BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_activity_log (
        id SERIAL PRIMARY KEY,
        actor_user_id INT REFERENCES "user"(id) ON DELETE SET NULL,
        actor_name TEXT,
        actor_role TEXT,
        module_key TEXT,
        action TEXT NOT NULL,
        target_type TEXT,
        target_id TEXT,
        target_label TEXT,
        note TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    for (const module of getPermissionMatrix()) {
      for (const permission of module.permissions || []) {
        await safeQuery(
          `
          INSERT INTO admin_permissions (
            name,
            description,
            module_key,
            action,
            built_in
          )
          VALUES ($1,$2,$3,$4,true)
          ON CONFLICT (name) DO NOTHING
        `,
          [
            permission.code,
            `Allows ${permission.action} on ${module.label}`,
            module.key,
            permission.action,
          ],
        );
      }
    }

    for (const [roleName, preset] of Object.entries(ROLE_PRESETS)) {
      await safeQuery(
        `
        INSERT INTO admin_roles (
          name,
          title,
          description,
          permissions,
          built_in
        )
        VALUES ($1,$2,$3,$4::jsonb,true)
        ON CONFLICT (name) DO NOTHING
      `,
        [
          roleName,
          preset.label,
          preset.description,
          JSON.stringify(getDefaultPermissionsForRole(roleName)),
        ],
      );
    }

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_security_config (
        id INT PRIMARY KEY CHECK (id = 1),
        organization_pin_hash TEXT,
        updated_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE admin_security_config
      ADD COLUMN IF NOT EXISTS organization_pin_hash TEXT,
      ADD COLUMN IF NOT EXISTS updated_by INT REFERENCES "user"(id) ON DELETE SET NULL,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
    `);

    await safeQuery(`
      INSERT INTO admin_security_config (id)
      VALUES (1)
      ON CONFLICT (id) DO NOTHING;
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_email_otp_challenges (
        id UUID PRIMARY KEY,
        user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        purpose TEXT NOT NULL,
        code_hash TEXT NOT NULL,
        login_ticket_hash TEXT,
        attempts INT NOT NULL DEFAULT 0,
        expires_at TIMESTAMPTZ NOT NULL,
        consumed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE admin_email_otp_challenges
      ADD COLUMN IF NOT EXISTS user_id INT REFERENCES "user"(id) ON DELETE CASCADE,
      ADD COLUMN IF NOT EXISTS email TEXT,
      ADD COLUMN IF NOT EXISTS purpose TEXT,
      ADD COLUMN IF NOT EXISTS code_hash TEXT,
      ADD COLUMN IF NOT EXISTS login_ticket_hash TEXT,
      ADD COLUMN IF NOT EXISTS attempts INT DEFAULT 0,
      ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_admin_email_otp_user_purpose
      ON admin_email_otp_challenges (user_id, purpose, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_admin_email_otp_expires_at
      ON admin_email_otp_challenges (expires_at);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS auth_webauthn_credentials (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
        credential_id TEXT NOT NULL UNIQUE,
        public_key BYTEA NOT NULL,
        counter BIGINT NOT NULL DEFAULT 0,
        transports JSONB NOT NULL DEFAULT '[]'::jsonb,
        credential_device_type TEXT,
        credential_backed_up BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_used_at TIMESTAMPTZ
      );
    `);

    await safeQuery(`
      ALTER TABLE auth_webauthn_credentials
      ADD COLUMN IF NOT EXISTS user_id INT REFERENCES "user"(id) ON DELETE CASCADE,
      ADD COLUMN IF NOT EXISTS credential_id TEXT,
      ADD COLUMN IF NOT EXISTS public_key BYTEA,
      ADD COLUMN IF NOT EXISTS counter BIGINT DEFAULT 0,
      ADD COLUMN IF NOT EXISTS transports JSONB DEFAULT '[]'::jsonb,
      ADD COLUMN IF NOT EXISTS credential_device_type TEXT,
      ADD COLUMN IF NOT EXISTS credential_backed_up BOOLEAN DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now(),
      ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_webauthn_credentials_user_id
      ON auth_webauthn_credentials (user_id);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS auth_webauthn_challenges (
        id TEXT PRIMARY KEY,
        user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
        purpose TEXT NOT NULL,
        login_ticket_hash TEXT NOT NULL,
        challenge TEXT NOT NULL,
        rp_id TEXT NOT NULL,
        expected_origin TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE auth_webauthn_challenges
      ADD COLUMN IF NOT EXISTS user_id INT REFERENCES "user"(id) ON DELETE CASCADE,
      ADD COLUMN IF NOT EXISTS purpose TEXT,
      ADD COLUMN IF NOT EXISTS login_ticket_hash TEXT,
      ADD COLUMN IF NOT EXISTS challenge TEXT,
      ADD COLUMN IF NOT EXISTS rp_id TEXT,
      ADD COLUMN IF NOT EXISTS expected_origin TEXT,
      ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_webauthn_challenges_user_id
      ON auth_webauthn_challenges (user_id);
    `);

    await safeQuery(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_webauthn_challenges_session_purpose
      ON auth_webauthn_challenges (login_ticket_hash, purpose);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_webauthn_challenges_expires_at
      ON auth_webauthn_challenges (expires_at);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_publish (
      product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
      is_published BOOLEAN DEFAULT FALSE,
      published_by INT REFERENCES "user"(id),
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_ratings (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        user_id INT NOT NULL REFERENCES Customers(id),
        overall_rating INT CHECK (overall_rating BETWEEN 1 AND 5),
        review TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (product_id, user_id)
      );
      `);
    // Ensure the foreign key for product_ratings.user_id references the customers table

    // trnding products
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_views (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        viewed_at TIMESTAMP DEFAULT now()
      );
      `);

    // Anonymous visitor key (hashed) enables "unique visitors" metrics.
    await safeQuery(`
      ALTER TABLE product_views
      ADD COLUMN IF NOT EXISTS visitor_key TEXT;
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_views_product_viewed_at
      ON product_views (product_id, viewed_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_views_product_visitor_viewed_at
      ON product_views (product_id, visitor_key, viewed_at DESC);
    `);
    //compared products table (cascade deletes to avoid FK blocks)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_comparisons (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        compared_with INT REFERENCES products(id) ON DELETE CASCADE,
        compared_at TIMESTAMP DEFAULT now()
      );
      `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS competitor_analysis (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        competitor_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        competition_score NUMERIC NOT NULL DEFAULT 0,
        spec_similarity_score NUMERIC NOT NULL DEFAULT 0,
        price_proximity_score NUMERIC NOT NULL DEFAULT 0,
        compare_frequency_score NUMERIC NOT NULL DEFAULT 0,
        reason TEXT,
        analysis_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        computed_at TIMESTAMP DEFAULT now(),
        UNIQUE (product_id, competitor_id)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_competitor_analysis_product_score
      ON competitor_analysis (product_id, competition_score DESC);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS published_compare_pages (
        id SERIAL PRIMARY KEY,
        slug TEXT NOT NULL UNIQUE,
        entity_type TEXT NOT NULL DEFAULT 'smartphone',
        primary_product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        segment_label TEXT,
        smartphone_type_label TEXT,
        title TEXT NOT NULL,
        meta_description TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'published',
        created_by INT REFERENCES "user"(id),
        updated_by INT REFERENCES "user"(id),
        published_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now(),
        CHECK (status IN ('draft', 'published'))
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS published_compare_page_items (
        id SERIAL PRIMARY KEY,
        compare_page_id INT NOT NULL REFERENCES published_compare_pages(id) ON DELETE CASCADE,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        position INT NOT NULL,
        created_at TIMESTAMP DEFAULT now(),
        UNIQUE (compare_page_id, product_id),
        UNIQUE (compare_page_id, position),
        CHECK (position BETWEEN 1 AND 3)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_published_compare_pages_status_updated
      ON published_compare_pages (status, updated_at DESC);
    `);

    await safeQuery(`
      ALTER TABLE published_compare_pages
      ALTER COLUMN status SET DEFAULT 'published';
    `);

    await safeQuery(`
      ALTER TABLE published_compare_pages
      ADD COLUMN IF NOT EXISTS compare_key TEXT,
      ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual',
      ADD COLUMN IF NOT EXISTS generation_reason TEXT,
      ADD COLUMN IF NOT EXISTS system_score NUMERIC NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS manual_compare_count INT NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS last_compared_at TIMESTAMP,
      ADD COLUMN IF NOT EXISTS generated_at TIMESTAMP DEFAULT now();
    `);

    await safeQuery(`
      ALTER TABLE published_compare_pages
      DROP CONSTRAINT IF EXISTS published_compare_pages_source_check;
    `);

    await safeQuery(`
      ALTER TABLE published_compare_pages
      ADD CONSTRAINT published_compare_pages_source_check
      CHECK (source IN ('manual', 'automatic'));
    `);

    await safeQuery(`
      UPDATE published_compare_pages cp
      SET compare_key = src.compare_key
      FROM (
        SELECT
          cpi.compare_page_id,
          string_agg(cpi.product_id::text, ':' ORDER BY cpi.product_id) AS compare_key
        FROM published_compare_page_items cpi
        GROUP BY cpi.compare_page_id
      ) src
      WHERE cp.id = src.compare_page_id
        AND (cp.compare_key IS NULL OR cp.compare_key = '');
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_published_compare_pages_compare_key
      ON published_compare_pages (compare_key);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_published_compare_pages_source_status
      ON published_compare_pages (source, status, updated_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_published_compare_page_items_page_position
      ON published_compare_page_items (compare_page_id, position ASC);
    `);

    // Hook Dynamic Score (precomputed ranking signals)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_dynamic_score (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        buyer_intent NUMERIC DEFAULT 0,
        trend_velocity NUMERIC DEFAULT 0,
        freshness NUMERIC DEFAULT 0,
        hook_score NUMERIC DEFAULT 0,
        calculated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_dynamic_score_hook
      ON product_dynamic_score (hook_score DESC);
    `);

    // Trending Scores (public "momentum" list) - can be recomputed on a schedule.
    // Manual override fields allow editorial/campaign boosts.
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_trending_score (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        views_7d INT NOT NULL DEFAULT 0,
        compares_7d INT NOT NULL DEFAULT 0,
        views_prev_7d INT NOT NULL DEFAULT 0,
        velocity NUMERIC NOT NULL DEFAULT 0,
        trending_score NUMERIC NOT NULL DEFAULT 0,
        calculated_at TIMESTAMP DEFAULT now(),
        manual_boost BOOLEAN DEFAULT false,
        manual_priority INT DEFAULT 0,
        manual_badge TEXT
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_trending_score_score
      ON product_trending_score (trending_score DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_trending_score_sort
      ON product_trending_score (manual_boost DESC, manual_priority DESC, trending_score DESC);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS compare_scoring_config (
        id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        weights JSONB NOT NULL DEFAULT '{"performance":0.36,"display":0.2,"camera":0.2,"battery":0.14,"priceValue":0.1}'::jsonb,
        chipset_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
        updated_by INT REFERENCES "user"(id),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      INSERT INTO compare_scoring_config (id)
      VALUES (1)
      ON CONFLICT (id) DO NOTHING;
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS device_field_profiles_config (
        id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        profiles JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_by INT REFERENCES "user"(id),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      INSERT INTO device_field_profiles_config (id)
      VALUES (1)
      ON CONFLICT (id) DO NOTHING;
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS wishlist (
        id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL
          REFERENCES customers(id)
          ON DELETE CASCADE,
      
        product_id INT NOT NULL
          REFERENCES products(id)
          ON DELETE CASCADE,
      
        created_at TIMESTAMP DEFAULT now(),
      
        UNIQUE (customer_id, product_id)
      );
      
      `);

    await safeQuery(`
        CREATE TABLE IF NOT EXISTS career_applications (
          id SERIAL PRIMARY KEY,
          role TEXT NOT NULL,
          gender TEXT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT NOT NULL,
        dob DATE,
        education JSONB,
        experience_level TEXT,
        employment_status TEXT,
        current_company TEXT,
        current_designation TEXT,
        notice_period TEXT,
        preferred_location TEXT,
        expected_ctc NUMERIC,
        skills TEXT,
        projects TEXT,
          cover_letter TEXT,
          assignment_pdf_url TEXT,
          assignment_due_date DATE,
          assignment_notes TEXT,
          interview_link TEXT,
          interview_scheduled_at TIMESTAMP,
          interview_notes TEXT,
          hr_scheduled_at TIMESTAMP,
          hr_notes TEXT,
          offer_pdf_url TEXT,
          offer_notes TEXT,
          application_place TEXT NOT NULL,
          application_date DATE,
          agree_terms BOOLEAN NOT NULL DEFAULT false,
          status TEXT NOT NULL DEFAULT 'new',
          source TEXT,
        payload JSONB,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_career_applications_created_at
      ON career_applications (created_at DESC);
    `);

    await safeQuery(`
        CREATE INDEX IF NOT EXISTS idx_career_applications_email
        ON career_applications (email);
      `);

    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS assignment_pdf_url TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS assignment_due_date DATE;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS assignment_notes TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS interview_link TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS interview_scheduled_at TIMESTAMP;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS interview_notes TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS hr_scheduled_at TIMESTAMP;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS hr_notes TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS offer_pdf_url TEXT;
      `);
    await safeQuery(`
        ALTER TABLE career_applications
        ADD COLUMN IF NOT EXISTS offer_notes TEXT;
      `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS blogs (
        id SERIAL PRIMARY KEY,
        product_id INT UNIQUE
          REFERENCES products(id)
          ON DELETE CASCADE,
        category TEXT NOT NULL DEFAULT 'news',
        title TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        excerpt TEXT,
        author_name TEXT,
        content_template TEXT NOT NULL,
        content_rendered TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        blog_eligible BOOLEAN NOT NULL DEFAULT false,
        eligibility_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        token_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        meta_title TEXT,
        meta_description TEXT,
        hero_image TEXT,
        hero_image_source TEXT,
        hero_image_alt TEXT,
        hero_image_caption TEXT,
        tags JSONB NOT NULL DEFAULT '[]'::jsonb,
        featured BOOLEAN NOT NULL DEFAULT false,
        trending BOOLEAN NOT NULL DEFAULT false,
        pinned BOOLEAN NOT NULL DEFAULT false,
        created_by INT REFERENCES "user"(id),
        updated_by INT REFERENCES "user"(id),
        published_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now(),
        CONSTRAINT blogs_status_check
          CHECK (status IN ('draft', 'published'))
      );
    `);

    // Blog can be product-linked or custom/general content.
    await safeQuery(`
      ALTER TABLE blogs
      ALTER COLUMN product_id DROP NOT NULL;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'news';
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS hero_image_source TEXT;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS author_name TEXT;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS author_user_id INT REFERENCES "user"(id) ON DELETE SET NULL;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS hero_image_alt TEXT;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS hero_image_caption TEXT;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS featured BOOLEAN NOT NULL DEFAULT false;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS trending BOOLEAN NOT NULL DEFAULT false;
    `);

    await safeQuery(`
      ALTER TABLE blogs
      ADD COLUMN IF NOT EXISTS pinned BOOLEAN NOT NULL DEFAULT false;
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blogs_status_published_at
      ON blogs (status, published_at DESC, updated_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blogs_product
      ON blogs (product_id);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS blog_products (
        id SERIAL PRIMARY KEY,
        blog_id INT NOT NULL REFERENCES blogs(id) ON DELETE CASCADE,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        position INT NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now(),
        CONSTRAINT blog_products_blog_product_unique UNIQUE (blog_id, product_id)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blog_products_blog_position
      ON blog_products (blog_id, position ASC, id ASC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blog_products_product
      ON blog_products (product_id, blog_id);
    `);

    await safeQuery(`
      INSERT INTO blog_products (blog_id, product_id, position)
      SELECT
        bl.id,
        bl.product_id,
        1
      FROM blogs bl
      WHERE bl.product_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM blog_products bp
          WHERE bp.blog_id = bl.id
            AND bp.product_id = bl.product_id
        );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS push_subscriptions (
        id SERIAL PRIMARY KEY,
        token TEXT NOT NULL,
        topic TEXT NOT NULL DEFAULT 'news-all',
        platform TEXT NOT NULL DEFAULT 'web',
        permission TEXT,
        user_agent TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        last_error TEXT,
        last_registered_at TIMESTAMP DEFAULT now(),
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now(),
        CONSTRAINT push_subscriptions_topic_token_unique UNIQUE (token, topic),
        CONSTRAINT push_subscriptions_status_check
          CHECK (status IN ('active', 'inactive', 'error'))
      );
    `);

    await safeQuery(`
      ALTER TABLE push_subscriptions
      ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'web',
      ADD COLUMN IF NOT EXISTS permission TEXT,
      ADD COLUMN IF NOT EXISTS user_agent TEXT,
      ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
      ADD COLUMN IF NOT EXISTS last_error TEXT,
      ADD COLUMN IF NOT EXISTS last_registered_at TIMESTAMP DEFAULT now(),
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();
    `);

    await safeQuery(`
      ALTER TABLE push_subscriptions
      DROP CONSTRAINT IF EXISTS push_subscriptions_status_check;
    `);

    await safeQuery(`
      ALTER TABLE push_subscriptions
      ADD CONSTRAINT push_subscriptions_status_check
      CHECK (status IN ('active', 'inactive', 'error'));
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_push_subscriptions_topic_status
      ON push_subscriptions (topic, status, updated_at DESC);
    `);

    // Marketing banners (campaigns / promotions)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS banners (
        id SERIAL PRIMARY KEY,
        title TEXT,
        placement TEXT NOT NULL,
        size_desktop TEXT,
        size_tablet TEXT,
        size_mobile TEXT,
        media_url TEXT NOT NULL,
        media_type TEXT,
        link_url TEXT,
        start_at TIMESTAMP,
        end_at TIMESTAMP,
        is_published BOOLEAN NOT NULL DEFAULT false,
        priority INT NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_banners_placement
      ON banners (placement);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_banners_active
      ON banners (is_published, start_at, end_at, priority);
    `);

    // Ensure banner schedule columns are timestamptz for correct timezone handling.
    await safeQuery(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='banners' AND column_name='start_at'
            AND data_type='timestamp without time zone'
        ) THEN
          ALTER TABLE banners
          ALTER COLUMN start_at TYPE timestamptz
          USING (start_at AT TIME ZONE 'UTC');
        END IF;

        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='banners' AND column_name='end_at'
            AND data_type='timestamp without time zone'
        ) THEN
          ALTER TABLE banners
          ALTER COLUMN end_at TYPE timestamptz
          USING (end_at AT TIME ZONE 'UTC');
        END IF;
      END
      $$;
    `);

    // Affiliate placements with manual page permissions and schedule windows
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS affiliate_placements (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        source_type TEXT NOT NULL DEFAULT 'manual',
        auto_key TEXT,
        auto_variant_id INT REFERENCES product_variants(id) ON DELETE SET NULL,
        auto_store_price_id INT REFERENCES variant_store_prices(id) ON DELETE SET NULL,
        title TEXT,
        description TEXT,
        cta_text TEXT,
        cta_subtext TEXT,
        badge_text TEXT,
        disclosure_text TEXT,
        store_name TEXT,
        store_logo_url TEXT,
        image_url TEXT,
        destination_url TEXT,
        affiliate_url TEXT,
        price NUMERIC,
        currency_code TEXT NOT NULL DEFAULT 'INR',
        priority INT NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'draft',
        publish_at TIMESTAMPTZ,
        unpublish_at TIMESTAMPTZ,
        duration_days INT,
        allow_product_list BOOLEAN NOT NULL DEFAULT false,
        allow_product_detail BOOLEAN NOT NULL DEFAULT false,
        allow_news BOOLEAN NOT NULL DEFAULT false,
        scope_type TEXT NOT NULL DEFAULT 'global',
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        blog_id INT REFERENCES blogs(id) ON DELETE CASCADE,
        brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
        category_name TEXT,
        list_slot TEXT NOT NULL DEFAULT 'product_card',
        detail_slot TEXT NOT NULL DEFAULT 'detail_highlight',
        news_slot TEXT NOT NULL DEFAULT 'inline_after_intro',
        created_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        updated_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE affiliate_placements
      ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'manual',
      ADD COLUMN IF NOT EXISTS auto_key TEXT,
      ADD COLUMN IF NOT EXISTS auto_variant_id INT REFERENCES product_variants(id) ON DELETE SET NULL,
      ADD COLUMN IF NOT EXISTS auto_store_price_id INT REFERENCES variant_store_prices(id) ON DELETE SET NULL;
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_placements_status_priority
      ON affiliate_placements (status, priority DESC, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_placements_scope
      ON affiliate_placements (scope_type, product_id, blog_id, brand_id);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_placements_page_flags
      ON affiliate_placements (allow_product_list, allow_product_detail, allow_news);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_placements_source_type
      ON affiliate_placements (source_type, product_id, updated_at DESC);
    `);

    await safeQuery(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_affiliate_placements_auto_key_unique
      ON affiliate_placements (auto_key)
      WHERE auto_key IS NOT NULL;
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS affiliate_clicks (
        id BIGSERIAL PRIMARY KEY,
        placement_id INT NOT NULL REFERENCES affiliate_placements(id) ON DELETE CASCADE,
        page_type TEXT,
        slot TEXT,
        product_id INT REFERENCES products(id) ON DELETE SET NULL,
        blog_id INT REFERENCES blogs(id) ON DELETE SET NULL,
        device_type TEXT,
        referer TEXT,
        user_agent TEXT,
        ip_address TEXT,
        target_url TEXT,
        was_live BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_clicks_placement_created
      ON affiliate_clicks (placement_id, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_affiliate_clicks_page_created
      ON affiliate_clicks (page_type, created_at DESC);
    `);

    // Popular feature clicks (analytics) - aggregated per day
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS feature_click_stats (
        id SERIAL PRIMARY KEY,
        device_type TEXT NOT NULL,
        feature_id TEXT NOT NULL,
        day DATE NOT NULL,
        clicks INT NOT NULL DEFAULT 0,
        last_clicked_at TIMESTAMP DEFAULT now(),
        created_at TIMESTAMP DEFAULT now(),
        UNIQUE (device_type, feature_id, day)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_feature_click_stats_device_day
      ON feature_click_stats (device_type, day);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS search_interest_events (
        id SERIAL PRIMARY KEY,
        event_id TEXT UNIQUE,
        query TEXT,
        normalized_query TEXT,
        product_id INT REFERENCES products(id) ON DELETE SET NULL,
        product_type TEXT,
        device_type TEXT,
        source TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_search_interest_events_product_created
      ON search_interest_events (product_id, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_search_interest_events_query_created
      ON search_interest_events (normalized_query, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_search_interest_events_created_at
      ON search_interest_events (created_at DESC);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS page_engagement_events (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        page_path TEXT,
        source TEXT,
        duration_ms INT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_page_engagement_events_product_created
      ON page_engagement_events (product_id, created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_page_engagement_events_created_at
      ON page_engagement_events (created_at DESC);
    `);

    console.log("✅ Migrations to   completed");
  } catch (err) {
    console.error("Migration error:", err);
    throw err;
  }
}

/* -----------------------
  Auth Middleware + Role-Based Access Control (RBAC)
------------------------*/

const WEBAUTHN_CREDENTIAL_TABLE = "auth_webauthn_credentials";
const WEBAUTHN_CHALLENGE_TABLE = "auth_webauthn_challenges";
const ADMIN_SECURITY_CONFIG_TABLE = "admin_security_config";
const ADMIN_EMAIL_OTP_TABLE = "admin_email_otp_challenges";
const ACCESS_TOKEN_TTL = "1h";
const PENDING_LOGIN_TOKEN_PURPOSE = "admin_pending_login";
const PENDING_LOGIN_TOKEN_TTL_SECONDS = 15 * 60;
const PENDING_LOGIN_TOKEN_TTL = `${PENDING_LOGIN_TOKEN_TTL_SECONDS}s`;
const WEBAUTHN_CHALLENGE_TTL_MS = 5 * 60 * 1000;
const ADMIN_PIN_SETUP_STEP = "pin_setup";
const ADMIN_PIN_STEP = "pin";
const ADMIN_PIN_MIN_LENGTH = 4;
const ADMIN_PIN_MAX_LENGTH = 10;
const ADMIN_EMAIL_OTP_LENGTH = 6;
const ADMIN_EMAIL_OTP_TTL_MINUTES = 10;
const ADMIN_EMAIL_OTP_TTL_MS = ADMIN_EMAIL_OTP_TTL_MINUTES * 60 * 1000;
const ADMIN_EMAIL_OTP_MAX_ATTEMPTS = 5;
const ADMIN_PIN_OTP_PURPOSE_SETUP = "organization_pin_setup";
const ADMIN_PIN_OTP_PURPOSE_UPDATE = "organization_pin_update";
const WEBAUTHN_RP_NAME =
  String(process.env.WEBAUTHN_RP_NAME || "Hooks Admin").trim() || "Hooks Admin";

const normalizeOrigin = (value) =>
  String(value || "")
    .trim()
    .replace(/\/$/, "");

const WEBAUTHN_ALLOWED_ORIGINS = new Set([
  ...Array.from(ALLOWED_ORIGINS).map(normalizeOrigin).filter(Boolean),
  ...String(process.env.WEBAUTHN_ALLOWED_ORIGINS || "")
    .split(",")
    .map(normalizeOrigin)
    .filter(Boolean),
]);

const loginInitiateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many login attempts. Please try again later.",
  },
});

const webAuthnVerifyLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many device verification attempts. Please try again later.",
  },
});

const adminOtpSendLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many verification code requests. Please try again later.",
  },
});

const adminOtpVerifyLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message:
      "Too many verification attempts. Please request a new code and try again.",
  },
});

const normalizeLoginEmail = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const parseBooleanInput = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return false;
};

const normalizeAdminPin = (value) =>
  String(value || "")
    .replace(/\s+/g, "")
    .trim();

const normalizeAdminOtp = (value) =>
  String(value || "")
    .replace(/\s+/g, "")
    .trim();

const isValidAdminPin = (value) =>
  new RegExp(`^\\d{${ADMIN_PIN_MIN_LENGTH},${ADMIN_PIN_MAX_LENGTH}}$`).test(
    normalizeAdminPin(value),
  );

const isValidAdminOtp = (value) =>
  new RegExp(`^\\d{${ADMIN_EMAIL_OTP_LENGTH}}$`).test(normalizeAdminOtp(value));

const normalizeTransportList = (value) =>
  (Array.isArray(value) ? value : []).filter(
    (transport) => typeof transport === "string" && transport.trim(),
  );

const parseJsonArray = (value, fallback = []) => {
  if (Array.isArray(value)) return value;
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : fallback;
    } catch {
      return fallback;
    }
  }
  return fallback;
};

const normalizeUserStatus = (value = "") => {
  const status = String(value || "")
    .trim()
    .toLowerCase();
  return status === "inactive" ? "inactive" : "active";
};

const normalizeAdminUserRow = (user, roleRecord = null) => {
  if (!user) return null;
  const firstName = String(user.first_name || "").trim();
  const lastName = String(user.last_name || "").trim();
  const displayName =
    String(user.display_name || "").trim() ||
    [firstName, lastName].filter(Boolean).join(" ").trim() ||
    String(user.user_name || "").trim() ||
    String(user.email || "").trim() ||
    "User";
  const permissionsOverride = parseJsonArray(user.permissions_override, []);
  const roleName = normalizeRole(user.role || roleRecord?.name || "viewer");
  const rolePermissions = parseJsonArray(roleRecord?.permissions || [], []).map(
    (permission) => normalizePermissionToken(permission),
  );
  const effectivePermissions = Array.from(
    new Set(
      [...rolePermissions, ...permissionsOverride]
        .map((permission) => normalizePermissionToken(permission))
        .filter(Boolean),
    ),
  );

  return {
    id: user.id,
    user_name: user.user_name || "",
    username: user.user_name || "",
    first_name: firstName,
    last_name: lastName,
    display_name: displayName,
    author_name: displayName,
    bio: String(user.bio || "").trim(),
    department: String(user.department || "").trim(),
    email: String(user.email || "").trim(),
    phone: String(user.phone || "").trim(),
    gender: String(user.gender || "").trim(),
    avatar: String(user.avatar || "").trim(),
    role: roleName,
    role_title: roleRecord?.title || getRolePreset(roleName).label,
    role_description:
      roleRecord?.description || getRolePreset(roleName).description,
    status: normalizeUserStatus(user.status),
    permissions_override: permissionsOverride,
    effective_permissions: effectivePermissions,
    permissions: effectivePermissions,
    last_login: user.last_login || null,
    created_at: user.created_at || null,
    updated_at: user.updated_at || user.created_at || null,
  };
};

const normalizeAdminRoleRow = (role) => {
  if (!role) return null;
  const roleName = normalizeRole(role.name || role.id || "viewer");
  const preset = getRolePreset(roleName);
  const permissions = Array.from(
    new Set(
      parseJsonArray(role.permissions, getDefaultPermissionsForRole(roleName))
        .map((permission) => normalizePermissionToken(permission))
        .filter(Boolean),
    ),
  );

  return {
    id: role.id || roleName,
    name: role.name || roleName,
    title: String(role.title || preset.label || roleName).trim(),
    description: String(role.description || preset.description || "").trim(),
    permissions,
    built_in: Boolean(role.built_in),
    created_at: role.created_at || null,
    updated_at: role.updated_at || role.created_at || null,
  };
};

const normalizeAdminPermissionRow = (permission) => {
  if (!permission) return null;
  return {
    id: permission.id || permission.name,
    name: permission.name || "",
    description: String(permission.description || "").trim(),
    module: String(permission.module_key || permission.module || "").trim(),
    module_label: getModuleLabel(
      permission.module_key || permission.module || "",
    ),
    action: String(permission.action || "").trim(),
    built_in: Boolean(permission.built_in),
    created_at: permission.created_at || null,
    updated_at: permission.updated_at || permission.created_at || null,
  };
};

const recordAdminActivity = async ({
  actorUserId = null,
  actorName = "",
  actorRole = "",
  moduleKey = "",
  action = "updated",
  targetType = "",
  targetId = null,
  targetLabel = "",
  note = "",
  meta = {},
} = {}) => {
  try {
    await db.query(
      `
      INSERT INTO admin_activity_log (
        actor_user_id,
        actor_name,
        actor_role,
        module_key,
        action,
        target_type,
        target_id,
        target_label,
        note,
        meta
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
    `,
      [
        actorUserId || null,
        String(actorName || "").trim() || "System",
        String(actorRole || "").trim() || "admin",
        String(moduleKey || "").trim() || null,
        String(action || "").trim() || "updated",
        String(targetType || "").trim() || null,
        targetId === null || typeof targetId === "undefined"
          ? null
          : String(targetId),
        String(targetLabel || "").trim() || null,
        String(note || "").trim() || null,
        JSON.stringify(toPlainObject(meta)),
      ],
    );
  } catch (err) {
    console.error("Failed to record admin activity:", err.message);
  }
};

const getRoleAccessRow = async (roleName = "") => {
  const normalized = normalizeRole(roleName);
  const result = await db.query(
    `
    SELECT id, name, title, description, permissions, built_in, created_at, updated_at
    FROM admin_roles
    WHERE LOWER(name) = $1
    ORDER BY updated_at DESC, id DESC
    LIMIT 1
  `,
    [normalized],
  );
  return (
    normalizeAdminRoleRow(result.rows[0]) ||
    normalizeAdminRoleRow({
      name: normalized,
      title: getRolePreset(normalized).label,
      description: getRolePreset(normalized).description,
      permissions: getDefaultPermissionsForRole(normalized),
      built_in: true,
    })
  );
};

const getRolePermissions = async (roleName = "") => {
  const role = await getRoleAccessRow(roleName);
  return Array.from(
    new Set(
      (role?.permissions || [])
        .map((permission) => normalizePermissionToken(permission))
        .filter(Boolean),
    ),
  );
};

const hasRolePermission = async (roleName = "", requested = "") => {
  const permissions = await getRolePermissions(roleName);
  return hasPermissionSet(permissions, requested);
};

const hasRoleAnyPermissions = async (roleName = "", requested = []) => {
  const permissions = await getRolePermissions(roleName);
  return hasAnyPermissionSet(permissions, requested);
};

const hasRoleAllPermissions = async (roleName = "", requested = []) => {
  const permissions = await getRolePermissions(roleName);
  return hasAllPermissionsSet(permissions, requested);
};

const requireRolePermissions =
  (requested = [], { any = false } = {}) =>
  async (req, res, next) => {
    try {
      const roleName = normalizeRole(req.user?.role || "viewer");
      const permitted = any
        ? await hasRoleAnyPermissions(roleName, requested)
        : await hasRoleAllPermissions(roleName, requested);
      if (!permitted) {
        return res.status(403).json({ message: "Insufficient permissions" });
      }
      return next();
    } catch (err) {
      console.error("Permission check failed:", err);
      return res.status(500).json({ message: "Failed to verify permissions" });
    }
  };

const serializeAdminUser = (user) => ({
  id: user.id,
  email: user.email,
  role: user.role,
  username: user.user_name,
  user_name: user.user_name,
  display_name:
    user.display_name || user.author_name || user.user_name || user.email || "",
  author_name:
    user.author_name || user.display_name || user.user_name || user.email || "",
  first_name: user.first_name || "",
  last_name: user.last_name || "",
  phone: user.phone || "",
  gender: user.gender || "",
  bio: user.bio || "",
  department: user.department || "",
  status: user.status || "active",
  avatar: user.avatar || "",
  permissions_override: Array.isArray(user.permissions_override)
    ? user.permissions_override
    : [],
  effective_permissions: Array.isArray(user.effective_permissions)
    ? user.effective_permissions
    : [],
  permissions: Array.isArray(user.effective_permissions)
    ? user.effective_permissions
    : [],
  last_login: user.last_login || null,
  created_at: user.created_at || null,
  updated_at: user.updated_at || null,
});

const issueAdminAccessToken = (user) =>
  jwt.sign(serializeAdminUser(user), SECRET, { expiresIn: ACCESS_TOKEN_TTL });

const buildSuccessfulAdminLoginResponse = (
  user,
  message = "Login successful",
) => ({
  message,
  token: issueAdminAccessToken(user),
  user: serializeAdminUser(user),
});

const issuePendingLoginTicket = (user, nextAction) =>
  jwt.sign(
    {
      purpose: PENDING_LOGIN_TOKEN_PURPOSE,
      id: user.id,
      email: user.email,
      role: user.role,
      username: user.user_name,
      nextAction,
    },
    SECRET,
    { expiresIn: PENDING_LOGIN_TOKEN_TTL },
  );

const verifyPendingLoginTicket = (loginTicket) => {
  const token = String(loginTicket || "").trim();
  if (!token) return null;

  try {
    const payload = jwt.verify(token, SECRET);
    if (
      payload?.purpose !== PENDING_LOGIN_TOKEN_PURPOSE ||
      !payload?.id ||
      !payload?.nextAction
    ) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
};

const buildPendingLoginResponse = (user, nextAction, message) => {
  const payload = {
    message,
    nextStep: nextAction,
    loginTicket: issuePendingLoginTicket(user, nextAction),
    pendingExpiresIn: PENDING_LOGIN_TOKEN_TTL_SECONDS,
  };

  if (nextAction === "device_auth") {
    payload.deviceAuthRequired = true;
  }

  if (nextAction === "device_setup") {
    payload.deviceSetupRequired = true;
  }

  if (nextAction === ADMIN_PIN_SETUP_STEP) {
    payload.pinSetupRequired = true;
  }

  if (nextAction === ADMIN_PIN_STEP) {
    payload.pinRequired = true;
  }

  return payload;
};

const hashLoginTicket = (loginTicket) =>
  crypto
    .createHash("sha256")
    .update(String(loginTicket || ""))
    .digest("hex");

const getAllowedWebAuthnOrigin = (req) => {
  const originHeader = normalizeOrigin(req.get("origin"));
  if (originHeader && WEBAUTHN_ALLOWED_ORIGINS.has(originHeader)) {
    return originHeader;
  }

  const refererHeader = String(req.get("referer") || "").trim();
  if (!refererHeader) return null;

  try {
    const refererOrigin = normalizeOrigin(new URL(refererHeader).origin);
    if (WEBAUTHN_ALLOWED_ORIGINS.has(refererOrigin)) {
      return refererOrigin;
    }
  } catch {
    return null;
  }

  return null;
};

const getWebAuthnRpId = (origin) => {
  const explicitRpId = String(process.env.WEBAUTHN_RP_ID || "").trim();
  if (explicitRpId) return explicitRpId;

  try {
    return new URL(origin).hostname;
  } catch {
    return null;
  }
};

async function getAdminUserById(userId) {
  const result = await db.query('SELECT * FROM "user" WHERE id = $1 LIMIT 1', [
    userId,
  ]);
  const user = result.rows[0] || null;
  if (!user) return null;
  const roleRecord = await getRoleAccessRow(user.role);
  return normalizeAdminUserRow(user, roleRecord);
}

async function getAdminSecurityConfig() {
  await db.query(
    `INSERT INTO ${ADMIN_SECURITY_CONFIG_TABLE} (id)
     VALUES (1)
     ON CONFLICT (id) DO NOTHING`,
  );

  const result = await db.query(
    `SELECT id, organization_pin_hash, updated_by, updated_at
     FROM ${ADMIN_SECURITY_CONFIG_TABLE}
     WHERE id = 1
     LIMIT 1`,
  );

  return (
    result.rows[0] || {
      id: 1,
      organization_pin_hash: null,
      updated_by: null,
      updated_at: null,
    }
  );
}

async function upsertAdminOrganizationPinHash(pinHash, updatedBy) {
  const result = await db.query(
    `INSERT INTO ${ADMIN_SECURITY_CONFIG_TABLE}
      (id, organization_pin_hash, updated_by, updated_at)
     VALUES (1, $1, $2, now())
     ON CONFLICT (id)
     DO UPDATE SET
       organization_pin_hash = EXCLUDED.organization_pin_hash,
       updated_by = EXCLUDED.updated_by,
       updated_at = now()
     RETURNING id, organization_pin_hash, updated_by, updated_at`,
    [pinHash, updatedBy || null],
  );

  return result.rows[0] || null;
}

const maskEmailAddress = (email) => {
  const normalized = String(email || "").trim();
  if (!normalized.includes("@")) return normalized;

  const [local, domain] = normalized.split("@");
  const maskedLocal =
    local.length <= 2
      ? `${local[0] || ""}*`
      : `${local.slice(0, 2)}${"*".repeat(Math.max(1, local.length - 2))}`;

  return `${maskedLocal}@${domain}`;
};

const hashAdminOtpCode = (code) =>
  crypto
    .createHash("sha256")
    .update(String(code || ""))
    .digest("hex");

const generateAdminOtpCode = () =>
  Array.from({ length: ADMIN_EMAIL_OTP_LENGTH }, () =>
    crypto.randomInt(0, 10),
  ).join("");

async function cleanupAdminOtpChallenges() {
  await db.query(
    `DELETE FROM ${ADMIN_EMAIL_OTP_TABLE}
     WHERE expires_at <= now()
        OR consumed_at IS NOT NULL`,
  );
}

async function createAdminOtpChallenge({ user, purpose, loginTicket }) {
  const userId = Number(user?.id || 0);
  const email = String(user?.email || "").trim();

  if (!userId || !email) {
    throw new Error("Unable to send verification code for this admin user.");
  }

  const challengeId = crypto.randomUUID();
  const otpCode = generateAdminOtpCode();
  const loginTicketHash = loginTicket ? hashLoginTicket(loginTicket) : null;
  const expiresAt = new Date(Date.now() + ADMIN_EMAIL_OTP_TTL_MS);

  await cleanupAdminOtpChallenges();
  await db.query(
    `DELETE FROM ${ADMIN_EMAIL_OTP_TABLE}
     WHERE user_id = $1
       AND purpose = $2
       AND consumed_at IS NULL`,
    [userId, purpose],
  );

  await db.query(
    `INSERT INTO ${ADMIN_EMAIL_OTP_TABLE}
      (id, user_id, email, purpose, code_hash, login_ticket_hash, attempts, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, 0, $7)`,
    [
      challengeId,
      userId,
      email,
      purpose,
      hashAdminOtpCode(otpCode),
      loginTicketHash,
      expiresAt,
    ],
  );

  const purposeLabel =
    purpose === ADMIN_PIN_OTP_PURPOSE_SETUP
      ? "create or confirm the organization PIN"
      : "change the organization PIN";

  await sendAdminOrganizationPinOtpEmail({
    email,
    userName:
      user.first_name ||
      user.user_name ||
      user.username ||
      user.email ||
      "Admin",
    otpCode,
    purposeLabel,
    expiresInMinutes: ADMIN_EMAIL_OTP_TTL_MINUTES,
  });

  return {
    challengeId,
    expiresIn: ADMIN_EMAIL_OTP_TTL_MINUTES * 60,
    maskedEmail: maskEmailAddress(email),
  };
}

async function consumeAdminOtpChallenge({
  challengeId,
  userId,
  purpose,
  otp,
  loginTicket,
}) {
  const normalizedChallengeId = String(challengeId || "").trim();
  const normalizedOtp = normalizeAdminOtp(otp);

  if (!normalizedChallengeId) {
    return {
      ok: false,
      status: 400,
      message: "Verification session is missing. Please request a new OTP.",
    };
  }

  if (!isValidAdminOtp(normalizedOtp)) {
    return {
      ok: false,
      status: 400,
      message: `Enter the ${ADMIN_EMAIL_OTP_LENGTH}-digit verification OTP sent to your email.`,
    };
  }

  await cleanupAdminOtpChallenges();

  const result = await db.query(
    `SELECT *
     FROM ${ADMIN_EMAIL_OTP_TABLE}
     WHERE id = $1
       AND user_id = $2
       AND purpose = $3
     LIMIT 1`,
    [normalizedChallengeId, userId, purpose],
  );

  const challenge = result.rows[0] || null;
  if (!challenge) {
    return {
      ok: false,
      status: 400,
      message: "Verification session expired. Please request a new OTP.",
    };
  }

  if (challenge.consumed_at) {
    return {
      ok: false,
      status: 400,
      message: "This verification OTP has already been used.",
    };
  }

  const expiresAtMs = new Date(challenge.expires_at).getTime();
  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
    await db.query(`DELETE FROM ${ADMIN_EMAIL_OTP_TABLE} WHERE id = $1`, [
      challenge.id,
    ]);
    return {
      ok: false,
      status: 400,
      message: "Verification OTP expired. Please request a new OTP.",
    };
  }

  if (loginTicket) {
    const expectedLoginTicketHash = hashLoginTicket(loginTicket);
    if (challenge.login_ticket_hash !== expectedLoginTicketHash) {
      return {
        ok: false,
        status: 401,
        message: "Login session expired. Please sign in again.",
      };
    }
  }

  const attempts = Number(challenge.attempts || 0);
  if (attempts >= ADMIN_EMAIL_OTP_MAX_ATTEMPTS) {
    await db.query(`DELETE FROM ${ADMIN_EMAIL_OTP_TABLE} WHERE id = $1`, [
      challenge.id,
    ]);
    return {
      ok: false,
      status: 429,
      message: "Too many invalid OTP attempts. Please request a new OTP.",
    };
  }

  const matches = hashAdminOtpCode(normalizedOtp) === challenge.code_hash;
  if (!matches) {
    await db.query(
      `UPDATE ${ADMIN_EMAIL_OTP_TABLE}
       SET attempts = attempts + 1
       WHERE id = $1`,
      [challenge.id],
    );
    return {
      ok: false,
      status: 401,
      message: "Invalid verification OTP",
    };
  }

  await db.query(
    `UPDATE ${ADMIN_EMAIL_OTP_TABLE}
     SET consumed_at = now()
     WHERE id = $1`,
    [challenge.id],
  );

  return { ok: true };
}

async function listUserWebAuthnCredentials(userId) {
  const result = await db.query(
    `SELECT id,
            user_id,
            credential_id,
            public_key,
            counter,
            transports,
            credential_device_type,
            credential_backed_up,
            created_at,
            last_used_at
     FROM ${WEBAUTHN_CREDENTIAL_TABLE}
     WHERE user_id = $1
     ORDER BY created_at ASC`,
    [userId],
  );

  return result.rows.map((row) => ({
    ...row,
    counter: Number(row.counter || 0),
    transports: normalizeTransportList(row.transports),
  }));
}

async function storeWebAuthnChallenge({
  userId,
  loginTicket,
  purpose,
  challenge,
  rpId,
  expectedOrigin,
}) {
  const loginTicketHash = hashLoginTicket(loginTicket);
  const expiresAt = new Date(Date.now() + WEBAUTHN_CHALLENGE_TTL_MS);

  await db.query(
    `DELETE FROM ${WEBAUTHN_CHALLENGE_TABLE} WHERE expires_at <= now()`,
  );
  await db.query(
    `DELETE FROM ${WEBAUTHN_CHALLENGE_TABLE}
     WHERE login_ticket_hash = $1
       AND purpose = $2`,
    [loginTicketHash, purpose],
  );

  await db.query(
    `INSERT INTO ${WEBAUTHN_CHALLENGE_TABLE}
      (id, user_id, purpose, login_ticket_hash, challenge, rp_id, expected_origin, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      crypto.randomUUID(),
      userId,
      purpose,
      loginTicketHash,
      challenge,
      rpId,
      expectedOrigin,
      expiresAt,
    ],
  );
}

async function readWebAuthnChallenge({ userId, loginTicket, purpose }) {
  const loginTicketHash = hashLoginTicket(loginTicket);
  const result = await db.query(
    `SELECT *
     FROM ${WEBAUTHN_CHALLENGE_TABLE}
     WHERE user_id = $1
       AND login_ticket_hash = $2
       AND purpose = $3
     ORDER BY created_at DESC
     LIMIT 1`,
    [userId, loginTicketHash, purpose],
  );

  const row = result.rows[0] || null;
  if (!row) return null;

  const expiresAtMs = new Date(row.expires_at).getTime();
  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
    await db.query(`DELETE FROM ${WEBAUTHN_CHALLENGE_TABLE} WHERE id = $1`, [
      row.id,
    ]);
    return null;
  }

  return row;
}

async function clearWebAuthnChallenges(loginTicket) {
  await db.query(
    `DELETE FROM ${WEBAUTHN_CHALLENGE_TABLE}
     WHERE login_ticket_hash = $1`,
    [hashLoginTicket(loginTicket)],
  );
}

/* -----------------------
  AUTH Routes
------------------------*/
app.post("/api/auth/register", async (req, res) => {
  try {
    const b = req.body || {};
    const user_name = String(b.user_name || b.username || "").trim() || null;
    const first_name = String(b.first_name || "").trim() || null;
    const last_name = String(b.last_name || "").trim() || null;
    const phone = String(b.phone || "").trim() || null;
    const gender = String(b.gender || "").trim() || null;
    const email = String(b.email || "")
      .trim()
      .toLowerCase();
    const password = String(
      b.password || `${Math.random()}${Date.now()}`,
    ).trim();
    const role = normalizeRole(b.role || "viewer");
    const displayName =
      String(b.display_name || "").trim() ||
      [first_name, last_name].filter(Boolean).join(" ").trim() ||
      user_name ||
      email ||
      "User";
    const bio = String(b.bio || "").trim() || null;
    const department = String(b.department || "").trim() || null;
    const status = normalizeUserStatus(b.status);
    const avatar = String(b.avatar || b.avatar_url || "").trim() || null;
    const permissionsOverride = Array.from(
      new Set(
        parseJsonArray(b.permissions_override || b.permissions, [])
          .map((permission) => normalizePermissionToken(permission))
          .filter(Boolean),
      ),
    );

    if (!email || !password) {
      return res.status(400).json({ message: "email and password required" });
    }

    const hashed = await bcrypt.hash(password, 10);

    const result = await db.query(
      `INSERT INTO "user"
        (user_name, first_name, last_name, phone, gender, email, password, role, display_name, bio, department, status, avatar, permissions_override)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
       RETURNING *`,
      [
        user_name,
        first_name,
        last_name,
        phone,
        gender,
        email,
        hashed,
        role,
        displayName,
        bio,
        department,
        status,
        avatar,
        JSON.stringify(permissionsOverride),
      ],
    );

    const roleRecord = await getRoleAccessRow(role);
    const user = normalizeAdminUserRow(result.rows[0], roleRecord);

    res.status(201).json({
      message: "User registered successfully. Email sent.",
      user,
    });
  } catch (err) {
    if (err.code === "23505") {
      return res.status(409).json({ message: "Email already registered" });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/auth/login", loginInitiateLimiter, async (req, res) => {
  try {
    const b = req.body || {};
    const email = normalizeLoginEmail(b.email);
    const password = String(b.password || "");

    if (!email || !password) {
      return res.status(400).json({ message: "email and password required" });
    }

    const result = await db.query(
      'SELECT * FROM "user" WHERE LOWER(email) = $1 LIMIT 1',
      [email],
    );
    if (!result.rows.length) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const user = result.rows[0];
    const roleRecord = await getRoleAccessRow(user.role);
    const normalizedUser = normalizeAdminUserRow(user, roleRecord);
    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const securityConfig = await getAdminSecurityConfig();
    if (securityConfig?.organization_pin_hash) {
      return res.json(
        buildPendingLoginResponse(
          user,
          ADMIN_PIN_STEP,
          "Enter your organization PIN to finish signing in.",
        ),
      );
    }

    return res.json(
      buildPendingLoginResponse(
        user,
        ADMIN_PIN_SETUP_STEP,
        "Organization PIN is not configured yet. Create it to finish signing in.",
      ),
    );
  } catch (err) {
    console.error("Login initiation error:", err);
    res.status(500).json({
      message: "Unable to start login. Please try again.",
    });
  }
});

app.post("/api/auth/login/pin", webAuthnVerifyLimiter, async (req, res) => {
  try {
    const loginTicket = String(req.body?.loginTicket || "").trim();
    const pin = normalizeAdminPin(req.body?.pin);
    const pendingLogin = verifyPendingLoginTicket(loginTicket);

    if (!pendingLogin || pendingLogin.nextAction !== ADMIN_PIN_STEP) {
      return res.status(401).json({
        message: "Login session expired. Please sign in again.",
      });
    }

    if (!isValidAdminPin(pin)) {
      return res.status(400).json({
        message: `Enter a valid ${ADMIN_PIN_MIN_LENGTH}-${ADMIN_PIN_MAX_LENGTH} digit organization PIN.`,
      });
    }

    const securityConfig = await getAdminSecurityConfig();
    if (!securityConfig?.organization_pin_hash) {
      return res.status(400).json({
        message:
          "Organization PIN is not configured. Contact an administrator.",
      });
    }

    const matches = await bcrypt.compare(
      pin,
      securityConfig.organization_pin_hash,
    );
    if (!matches) {
      return res.status(401).json({ message: "Invalid organization PIN" });
    }

    const user = await getAdminUserById(pendingLogin.id);
    if (!user) {
      return res.status(401).json({ message: "User not found" });
    }

    return res.json(
      buildSuccessfulAdminLoginResponse(user, "Login successful"),
    );
  } catch (err) {
    console.error("Organization PIN login verify error:", err);
    return res.status(500).json({
      message: "Unable to verify organization PIN. Please try again.",
    });
  }
});

app.post(
  "/api/auth/login/pin/setup/request-otp",
  adminOtpSendLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (!pendingLogin || pendingLogin.nextAction !== ADMIN_PIN_SETUP_STEP) {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      const securityConfig = await getAdminSecurityConfig();
      if (securityConfig?.organization_pin_hash) {
        return res.status(400).json({
          message:
            "Organization PIN is already configured. Sign in with the current PIN.",
        });
      }

      return res.json({
        success: true,
        message:
          "OTP verification is disabled. You can continue with organization PIN setup directly.",
      });
    } catch (err) {
      console.error("Organization PIN setup request error:", err);
      return res.status(500).json({
        message: "Unable to continue organization PIN setup. Please try again.",
      });
    }
  },
);

app.post(
  "/api/auth/login/pin/setup/verify",
  adminOtpVerifyLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const newPin = normalizeAdminPin(req.body?.newPin);
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (!pendingLogin || pendingLogin.nextAction !== ADMIN_PIN_SETUP_STEP) {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      if (!isValidAdminPin(newPin)) {
        return res.status(400).json({
          message: `Organization PIN must be ${ADMIN_PIN_MIN_LENGTH}-${ADMIN_PIN_MAX_LENGTH} digits.`,
        });
      }

      const securityConfig = await getAdminSecurityConfig();
      if (securityConfig?.organization_pin_hash) {
        return res.status(400).json({
          message:
            "Organization PIN is already configured. Sign in with the current PIN.",
        });
      }

      const user = await getAdminUserById(pendingLogin.id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      const hashedPin = await bcrypt.hash(newPin, 10);
      await upsertAdminOrganizationPinHash(hashedPin, user.id);

      return res.json(
        buildSuccessfulAdminLoginResponse(
          user,
          "Organization PIN created. Login successful.",
        ),
      );
    } catch (err) {
      console.error("Organization PIN setup verify error:", err);
      return res.status(500).json({
        message: "Unable to create the organization PIN.",
      });
    }
  },
);

app.post(
  "/api/auth/login/webauthn/options",
  loginInitiateLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (!pendingLogin || pendingLogin.nextAction !== "device_auth") {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      const expectedOrigin = getAllowedWebAuthnOrigin(req);
      if (!expectedOrigin) {
        return res.status(400).json({
          message:
            "This browser origin is not allowed for device verification.",
        });
      }

      const rpID = getWebAuthnRpId(expectedOrigin);
      if (!rpID) {
        return res.status(400).json({
          message: "Unable to resolve the device verification domain.",
        });
      }

      const credentials = await listUserWebAuthnCredentials(pendingLogin.id);
      if (!credentials.length) {
        return res.status(400).json({
          message: "No device credentials are registered for this account yet.",
        });
      }

      const options = await generateAuthenticationOptions({
        rpID,
        timeout: 60000,
        userVerification: "required",
        allowCredentials: credentials.map((credential) => ({
          id: credential.credential_id,
          transports: credential.transports,
        })),
      });

      await storeWebAuthnChallenge({
        userId: pendingLogin.id,
        loginTicket,
        purpose: "authentication",
        challenge: options.challenge,
        rpId: rpID,
        expectedOrigin,
      });

      return res.json({
        message: "Device verification ready",
        options,
      });
    } catch (err) {
      console.error("WebAuthn authentication options error:", err);
      return res.status(500).json({
        message: "Unable to start device verification. Please try again.",
      });
    }
  },
);

app.post(
  "/api/auth/login/webauthn/verify",
  webAuthnVerifyLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const response = req.body?.response;
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (
        !pendingLogin ||
        pendingLogin.nextAction !== "device_auth" ||
        !response ||
        typeof response !== "object"
      ) {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      const storedChallenge = await readWebAuthnChallenge({
        userId: pendingLogin.id,
        loginTicket,
        purpose: "authentication",
      });
      if (!storedChallenge) {
        return res.status(410).json({
          message:
            "Device verification expired. Please sign in again to request a new challenge.",
        });
      }

      const credentials = await listUserWebAuthnCredentials(pendingLogin.id);
      const matchedCredential = credentials.find(
        (credential) => credential.credential_id === response.id,
      );

      if (!matchedCredential) {
        return res.status(404).json({
          message: "This device credential is no longer registered.",
        });
      }

      let verification;
      try {
        verification = await verifyAuthenticationResponse({
          response,
          expectedChallenge: storedChallenge.challenge,
          expectedOrigin: storedChallenge.expected_origin,
          expectedRPID: storedChallenge.rp_id,
          credential: {
            id: matchedCredential.credential_id,
            publicKey: matchedCredential.public_key,
            counter: matchedCredential.counter,
            transports: matchedCredential.transports,
          },
          requireUserVerification: true,
        });
      } catch (verificationError) {
        console.warn(
          "WebAuthn authentication verification failed:",
          verificationError,
        );
        return res.status(401).json({
          message: "Device verification failed. Please try again.",
        });
      }

      if (!verification.verified) {
        return res.status(401).json({
          message: "Device verification failed. Please try again.",
        });
      }

      await db.query(
        `UPDATE ${WEBAUTHN_CREDENTIAL_TABLE}
         SET counter = $2,
             credential_device_type = $3,
             credential_backed_up = $4,
             last_used_at = now()
         WHERE credential_id = $1`,
        [
          matchedCredential.credential_id,
          verification.authenticationInfo.newCounter,
          verification.authenticationInfo.credentialDeviceType,
          verification.authenticationInfo.credentialBackedUp,
        ],
      );
      await clearWebAuthnChallenges(loginTicket);

      const user = await getAdminUserById(pendingLogin.id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      return res.json(
        buildSuccessfulAdminLoginResponse(
          user,
          "Device verified. Login successful.",
        ),
      );
    } catch (err) {
      console.error("WebAuthn authentication verify error:", err);
      return res.status(500).json({
        message: "Unable to finish device verification. Please try again.",
      });
    }
  },
);

app.post(
  "/api/auth/login/webauthn/register/options",
  loginInitiateLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (!pendingLogin || pendingLogin.nextAction !== "device_setup") {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      const expectedOrigin = getAllowedWebAuthnOrigin(req);
      if (!expectedOrigin) {
        return res.status(400).json({
          message:
            "This browser origin is not allowed for device verification.",
        });
      }

      const rpID = getWebAuthnRpId(expectedOrigin);
      if (!rpID) {
        return res.status(400).json({
          message: "Unable to resolve the device verification domain.",
        });
      }

      const user = await getAdminUserById(pendingLogin.id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      const credentials = await listUserWebAuthnCredentials(user.id);
      const options = await generateRegistrationOptions({
        rpName: WEBAUTHN_RP_NAME,
        rpID,
        userName: user.email,
        userID: Buffer.from(String(user.id)),
        userDisplayName:
          user.user_name || user.first_name || user.last_name || user.email,
        timeout: 60000,
        attestationType: "none",
        preferredAuthenticatorType: "localDevice",
        authenticatorSelection: {
          authenticatorAttachment: "platform",
          residentKey: "preferred",
          userVerification: "required",
        },
        excludeCredentials: credentials.map((credential) => ({
          id: credential.credential_id,
          transports: credential.transports,
        })),
      });

      await storeWebAuthnChallenge({
        userId: user.id,
        loginTicket,
        purpose: "registration",
        challenge: options.challenge,
        rpId: rpID,
        expectedOrigin,
      });

      return res.json({
        message: "Device setup ready",
        options,
      });
    } catch (err) {
      console.error("WebAuthn registration options error:", err);
      return res.status(500).json({
        message: "Unable to start device setup. Please try again.",
      });
    }
  },
);

app.post(
  "/api/auth/login/webauthn/register/verify",
  webAuthnVerifyLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const response = req.body?.response;
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (
        !pendingLogin ||
        pendingLogin.nextAction !== "device_setup" ||
        !response ||
        typeof response !== "object"
      ) {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      const storedChallenge = await readWebAuthnChallenge({
        userId: pendingLogin.id,
        loginTicket,
        purpose: "registration",
      });
      if (!storedChallenge) {
        return res.status(410).json({
          message:
            "Device setup expired. Please sign in again to request a new challenge.",
        });
      }

      let verification;
      try {
        verification = await verifyRegistrationResponse({
          response,
          expectedChallenge: storedChallenge.challenge,
          expectedOrigin: storedChallenge.expected_origin,
          expectedRPID: storedChallenge.rp_id,
          requireUserPresence: true,
          requireUserVerification: true,
        });
      } catch (verificationError) {
        console.warn(
          "WebAuthn registration verification failed:",
          verificationError,
        );
        return res.status(401).json({
          message: "Device setup failed. Please try again.",
        });
      }

      if (!verification.verified || !verification.registrationInfo) {
        return res.status(401).json({
          message: "Device setup failed. Please try again.",
        });
      }

      const { credential, credentialBackedUp, credentialDeviceType } =
        verification.registrationInfo;

      await db.query(
        `INSERT INTO ${WEBAUTHN_CREDENTIAL_TABLE}
          (user_id, credential_id, public_key, counter, transports, credential_device_type, credential_backed_up, last_used_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, now())
         ON CONFLICT (credential_id)
         DO UPDATE SET
           user_id = EXCLUDED.user_id,
           public_key = EXCLUDED.public_key,
           counter = EXCLUDED.counter,
           transports = EXCLUDED.transports,
           credential_device_type = EXCLUDED.credential_device_type,
           credential_backed_up = EXCLUDED.credential_backed_up,
           last_used_at = now()`,
        [
          pendingLogin.id,
          credential.id,
          Buffer.from(credential.publicKey),
          Number(credential.counter || 0),
          JSON.stringify(normalizeTransportList(credential.transports)),
          credentialDeviceType,
          Boolean(credentialBackedUp),
        ],
      );
      await clearWebAuthnChallenges(loginTicket);

      const user = await getAdminUserById(pendingLogin.id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      return res.json(
        buildSuccessfulAdminLoginResponse(
          user,
          "Device verification enabled. Login successful.",
        ),
      );
    } catch (err) {
      console.error("WebAuthn registration verify error:", err);
      return res.status(500).json({
        message: "Unable to finish device setup. Please try again.",
      });
    }
  },
);

app.post("/api/auth/login/finalize", loginInitiateLimiter, async (_req, res) =>
  res.status(400).json({
    message:
      "Device verification setup is required before you can finish signing in.",
  }),
);

/* ---- ADMIN Profile Endpoints ---- */
app.get("/api/auth/profile", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;

    const result = await db.query('SELECT * FROM "user" WHERE id = $1', [
      userId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const user = result.rows[0];

    res.json({
      success: true,
      message: "Admin profile retrieved successfully",
      user: {
        id: user.id,
        username: user.user_name,
        email: user.email,
        phone: user.phone || "",
        first_name: user.first_name || "",
        last_name: user.last_name || "",
        gender: user.gender || "",
        role: user.role || "",
        display_name: user.display_name || "",
        bio: user.bio || "",
        department: user.department || "",
        status: user.status || "active",
        avatar: user.avatar || "",
        permissions_override: Array.isArray(
          normalizedUser?.permissions_override,
        )
          ? normalizedUser.permissions_override
          : [],
        effective_permissions: Array.isArray(
          normalizedUser?.effective_permissions,
        )
          ? normalizedUser.effective_permissions
          : [],
      },
    });
  } catch (err) {
    console.error("Get admin profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- UPDATE Admin Profile ---- */
app.put("/api/auth/profile", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;
    const {
      email,
      phone,
      first_name,
      last_name,
      gender,
      display_name,
      bio,
      department,
      status,
      avatar,
    } = req.body;

    // Validate required fields
    if (!email) {
      return res.status(400).json({
        message: "Email is required",
      });
    }

    // Validate email format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      'SELECT id FROM "user" WHERE email = $1 AND id != $2',
      [email, userId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update admin profile
    const result = await db.query(
      `UPDATE "user" 
       SET email = $1,
           phone = $2,
           first_name = $3,
           last_name = $4,
           gender = $5,
           display_name = $6,
           bio = $7,
           department = $8,
           status = $9,
           avatar = $10,
           updated_at = now()
       WHERE id = $11
       RETURNING id, user_name, email, phone, first_name, last_name, gender, role, display_name, bio, department, status, avatar, permissions_override`,
      [
        email,
        phone || null,
        first_name || null,
        last_name || null,
        gender || null,
        display_name || null,
        bio || null,
        department || null,
        status || "active",
        avatar || null,
        userId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const updatedUser = result.rows[0];
    const roleRecord = await getRoleAccessRow(updatedUser.role);
    const normalizedUser = normalizeAdminUserRow(updatedUser, roleRecord);

    res.json({
      success: true,
      message: "Profile updated successfully",
      user: {
        id: updatedUser.id,
        username: updatedUser.user_name,
        email: updatedUser.email,
        phone: updatedUser.phone || "",
        first_name: updatedUser.first_name || "",
        last_name: updatedUser.last_name || "",
        gender: updatedUser.gender || "",
        role: updatedUser.role || "",
        display_name: updatedUser.display_name || "",
        bio: updatedUser.bio || "",
        department: updatedUser.department || "",
        status: updatedUser.status || "active",
        avatar: updatedUser.avatar || "",
        permissions_override: Array.isArray(
          normalizedUser?.permissions_override,
        )
          ? normalizedUser.permissions_override
          : [],
        effective_permissions: Array.isArray(
          normalizedUser?.effective_permissions,
        )
          ? normalizedUser.effective_permissions
          : [],
      },
    });
  } catch (err) {
    console.error("Update admin profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- CHANGE Admin Password ---- */
app.post("/api/auth/change-password", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;
    const { currentPassword, newPassword } = req.body;

    // Validate inputs
    if (!currentPassword || !newPassword) {
      return res.status(400).json({
        message: "Current password and new password are required",
      });
    }

    // Fetch user
    const userResult = await db.query(
      'SELECT password FROM "user" WHERE id = $1',
      [userId],
    );

    if (!userResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const user = userResult.rows[0];

    // Verify current password
    const passwordMatch = await bcrypt.compare(currentPassword, user.password);

    if (!passwordMatch) {
      return res.status(401).json({ message: "Current password is incorrect" });
    }

    // Check if new password is same as current
    const samePassword = await bcrypt.compare(newPassword, user.password);
    if (samePassword) {
      return res.status(400).json({
        message: "New password must be different from current password",
      });
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(newPassword, 10);

    // Update password
    await db.query('UPDATE "user" SET password = $1 WHERE id = $2', [
      hashedPassword,
      userId,
    ]);

    res.json({
      success: true,
      message: "Password changed successfully",
    });
  } catch (err) {
    console.error("Change admin password error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post(
  "/api/auth/organization-pin/request-otp",
  authenticate,
  adminOtpSendLimiter,
  async (req, res) => {
    return res.json({
      success: true,
      message:
        "OTP verification is disabled. You can update the organization PIN directly.",
    });
  },
);

app.get("/api/auth/organization-pin/status", authenticate, async (req, res) => {
  try {
    const securityConfig = await getAdminSecurityConfig();

    res.json({
      success: true,
      isConfigured: Boolean(securityConfig?.organization_pin_hash),
      updated_at: securityConfig?.updated_at || null,
      updated_by: securityConfig?.updated_by || null,
    });
  } catch (err) {
    console.error("Get organization PIN status error:", err);
    res.status(500).json({ message: "Failed to load organization PIN status" });
  }
});

app.put("/api/auth/organization-pin", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;
    const currentPin = normalizeAdminPin(req.body?.currentPin);
    const newPin = normalizeAdminPin(req.body?.newPin);

    if (!isValidAdminPin(newPin)) {
      return res.status(400).json({
        message: `Organization PIN must be ${ADMIN_PIN_MIN_LENGTH}-${ADMIN_PIN_MAX_LENGTH} digits.`,
      });
    }

    const securityConfig = await getAdminSecurityConfig();
    const currentHash = securityConfig?.organization_pin_hash || null;

    if (currentHash) {
      if (!currentPin) {
        return res.status(400).json({
          message: "Current organization PIN is required",
        });
      }

      const currentMatches = await bcrypt.compare(currentPin, currentHash);
      if (!currentMatches) {
        return res.status(401).json({
          message: "Current organization PIN is incorrect",
        });
      }

      const samePin = await bcrypt.compare(newPin, currentHash);
      if (samePin) {
        return res.status(400).json({
          message: "New organization PIN must be different from current PIN",
        });
      }
    }

    const hashedPin = await bcrypt.hash(newPin, 10);
    const updated = await upsertAdminOrganizationPinHash(hashedPin, userId);

    res.json({
      success: true,
      message: currentHash
        ? "Organization PIN updated successfully"
        : "Organization PIN created successfully",
      isConfigured: true,
      updated_at: updated?.updated_at || null,
      updated_by: updated?.updated_by || userId,
    });
  } catch (err) {
    console.error("Update organization PIN error:", err);
    res.status(500).json({ message: "Failed to update organization PIN" });
  }
});

/* ----customer auth */
app.post("/api/auth/customer/register", async (req, res) => {
  try {
    const {
      f_name,
      l_name,
      username: rawUsername,
      email: rawEmail,
      password,
      city,
      country,
      state,
      zip_code,
    } = req.body || {};

    // Basic required fields
    if (!f_name || !l_name || !rawUsername || !rawEmail || !password) {
      return res.status(400).json({
        message: "f_name, l_name, username, email, and password are required",
      });
    }

    // Normalize inputs
    const username = String(rawUsername).trim();
    const email = String(rawEmail).trim().toLowerCase();

    // Validate username: allow letters, numbers and underscores, 3-30 chars
    const usernameRegex = /^[A-Za-z0-9_]{3,30}$/;
    if (!usernameRegex.test(username)) {
      return res.status(400).json({
        message:
          "Invalid username. Use 3-30 characters: letters, numbers, and underscores only",
      });
    }

    // Validate email simple format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Validate password length
    if (String(password).length < 6) {
      return res
        .status(400)
        .json({ message: "Password must be at least 6 characters long" });
    }

    // Check uniqueness against Customers table
    const exists = await db.query(
      `SELECT id FROM Customers WHERE email = $1 OR username = $2`,
      [email, username],
    );

    if (exists.rows.length > 0) {
      return res
        .status(409)
        .json({ message: "Email or username already registered" });
    }

    // 3️⃣ Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // 4️⃣ Insert customer
    const result = await db.query(
      `INSERT INTO Customers
       (f_name, l_name, username, email, password, city, country, state, zip_code)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       RETURNING id, f_name, l_name, username, email, city, country, state, zip_code`,
      [
        f_name,
        l_name,
        username,
        email,
        hashedPassword,
        city || null,
        country || null,
        state || null,
        zip_code || null,
      ],
    );

    const customer = result.rows[0];

    // 5️⃣ Generate JWT token for automatic login
    const token = jwt.sign(
      {
        id: customer.id,
        email: customer.email,
        username: customer.username,
        type: "customer",
      },
      SECRET,
      { expiresIn: "7d" },
    );

    // 6️⃣ Success response with token and user data
    res.status(201).json({
      message: "Customer registered successfully. Automatically logged in.",
      token,
      user: customer,
    });
  } catch (err) {
    console.error("Customer register error:", err);
    res.status(500).json({ message: "Internal server error" });
  }
});

// Check username availability for signup form
app.get("/api/auth/check-username", async (req, res) => {
  try {
    const q = String(req.query.username || "").trim();
    if (!q) return res.json({ available: false });
    const usernameRegex = /^[A-Za-z0-9_]{3,30}$/;
    if (!usernameRegex.test(q)) return res.json({ available: false });

    const result = await db.query(
      `SELECT id FROM Customers WHERE username = $1 LIMIT 1`,
      [q],
    );
    return res.json({ available: result.rows.length === 0 });
  } catch (err) {
    console.error("check-username error:", err);
    return res.status(500).json({ available: false });
  }
});

// Check email availability for signup form
app.get("/api/auth/check-email", async (req, res) => {
  try {
    const q = String(req.query.email || "")
      .trim()
      .toLowerCase();
    if (!q) return res.json({ available: false });
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(q)) return res.json({ available: false });

    const result = await db.query(
      `SELECT id FROM Customers WHERE email = $1 LIMIT 1`,
      [q],
    );
    return res.json({ available: result.rows.length === 0 });
  } catch (err) {
    console.error("check-email error:", err);
    return res.status(500).json({ available: false });
  }
});
const listRbacPermissions = async () => {
  const defaultRows = getPermissionMatrix().flatMap((module) =>
    module.permissions.map((permission) => ({
      id: permission.code,
      name: permission.code,
      description: `Allows ${permission.action} on ${module.label}`,
      module: module.key,
      module_label: module.label,
      action: permission.action,
      built_in: true,
      created_at: null,
      updated_at: null,
    })),
  );

  const customRows = await db.query(
    `
    SELECT id, name, description, module_key, action, built_in, created_at, updated_at
    FROM admin_permissions
    ORDER BY built_in DESC, name ASC, id ASC
  `,
  );

  const merged = new Map();
  [...defaultRows, ...(customRows.rows || []).map(normalizeAdminPermissionRow)]
    .filter(Boolean)
    .forEach((permission) => {
      const key = String(permission.name || permission.id || "")
        .trim()
        .toLowerCase();
      if (!key) return;
      merged.set(key, permission);
    });

  return Array.from(merged.values()).sort((a, b) =>
    String(a.name || "").localeCompare(String(b.name || "")),
  );
};

const listRbacRoles = async () => {
  const defaultRows = Object.entries(ROLE_PRESETS).map(([name, preset]) => ({
    id: name,
    name,
    title: preset.label,
    description: preset.description,
    permissions: getDefaultPermissionsForRole(name),
    built_in: true,
    created_at: null,
    updated_at: null,
  }));

  const customRows = await db.query(
    `
    SELECT id, name, title, description, permissions, built_in, created_at, updated_at
    FROM admin_roles
    ORDER BY built_in DESC, title ASC, id ASC
  `,
  );

  const merged = new Map();
  [...defaultRows, ...(customRows.rows || []).map(normalizeAdminRoleRow)]
    .filter(Boolean)
    .forEach((role) => {
      const key = String(role.name || role.id || "")
        .trim()
        .toLowerCase();
      if (!key) return;
      merged.set(key, role);
    });

  return Array.from(merged.values()).sort((a, b) =>
    String(a.title || a.name || "").localeCompare(
      String(b.title || b.name || ""),
    ),
  );
};

const listRbacUsers = async ({ includeInactive = true } = {}) => {
  const [userRows, roleRows] = await Promise.all([
    db.query(
      `
      SELECT
        id,
        user_name,
        first_name,
        last_name,
        phone,
        gender,
        email,
        role,
        display_name,
        bio,
        department,
        status,
        avatar,
        permissions_override,
        last_login,
        created_at,
        updated_at
      FROM "user"
      ORDER BY created_at DESC, id DESC
    `,
    ),
    listRbacRoles(),
  ]);

  const roleMap = new Map(
    roleRows.map((role) => [normalizeRole(role.name || role.id), role]),
  );

  return userRows.rows
    .map((user) =>
      normalizeAdminUserRow(user, roleMap.get(normalizeRole(user.role))),
    )
    .filter(Boolean)
    .filter((user) => (includeInactive ? true : user.status !== "inactive"))
    .sort((a, b) =>
      String(a.display_name || "").localeCompare(String(b.display_name || "")),
    );
};

const listRbacActivities = async ({ limit = 200 } = {}) => {
  const result = await db.query(
    `
    SELECT
      id,
      actor_user_id,
      actor_name,
      actor_role,
      module_key,
      action,
      target_type,
      target_id,
      target_label,
      note,
      meta,
      created_at
    FROM admin_activity_log
    ORDER BY created_at DESC, id DESC
    LIMIT $1
  `,
    [Math.min(500, Math.max(1, Number(limit) || 200))],
  );

  return result.rows.map((entry) => ({
    id: entry.id,
    at: entry.created_at,
    actor: entry.actor_name || "System",
    actor_role: entry.actor_role || "admin",
    module: entry.module_key || "system",
    action: entry.action || "updated",
    target: entry.target_label || entry.target_type || entry.target_id || "",
    target_type: entry.target_type || "",
    target_id: entry.target_id || null,
    note: entry.note || "",
    meta: entry.meta || {},
  }));
};

const resolveRbacUserById = async (id) => {
  const userId = Number(id);
  if (!Number.isInteger(userId) || userId <= 0) return null;
  const users = await listRbacUsers({ includeInactive: true });
  return users.find((user) => Number(user.id) === userId) || null;
};

app.post("/api/auth/customer/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password)
      return res.status(400).json({ message: "email and password required" });

    const result = await db.query("SELECT * FROM customers WHERE email = $1", [
      email,
    ]);
    if (!result.rows.length)
      return res.status(401).json({ message: "Invalid credentials" });

    const customer = result.rows[0];
    const match = await bcrypt.compare(password, customer.password);
    if (!match) return res.status(401).json({ message: "Invalid credentials" });

    const token = jwt.sign(
      {
        id: customer.id,
        email: customer.email,
        username: customer.username,
        type: "customer",
      },
      SECRET,
      { expiresIn: "7d" },
    );

    res.json({
      message: "Login successful",
      token,
      user: {
        id: customer.id,
        f_name: customer.f_name,
        l_name: customer.l_name,
        username: customer.username,
        email: customer.email,
        city: customer.city,
        country: customer.country,
        state: customer.state,
        zip_code: customer.zip_code,
      },
    });
  } catch (err) {
    console.error("Customer login error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- GET User Profile ---- */
app.get("/api/auth/user-profile", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;

    const result = await db.query("SELECT * FROM customers WHERE id = $1", [
      customerId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const customer = result.rows[0];

    res.json({
      message: "User profile retrieved successfully",
      user: {
        id: customer.id,
        f_name: customer.f_name,
        l_name: customer.l_name,
        username: customer.username,
        email: customer.email,
        phone: customer.phone || "",
        city: customer.city || "",
        state: customer.state || "",
        country: customer.country || "",
        zip_code: customer.zip_code || "",
      },
    });
  } catch (err) {
    console.error("Get profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- UPDATE User Profile ---- */
app.put("/api/auth/update-profile", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;
    const { f_name, l_name, email, phone, city, state, country, zip_code } =
      req.body;

    // Validate required fields
    if (!f_name || !l_name || !email) {
      return res.status(400).json({
        message: "First name, last name, and email are required",
      });
    }

    // Validate email format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      "SELECT id FROM customers WHERE email = $1 AND id != $2",
      [email, customerId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update customer profile
    const result = await db.query(
      `UPDATE customers 
       SET f_name = $1, l_name = $2, email = $3, phone = $4, city = $5, state = $6, country = $7, zip_code = $8
       WHERE id = $9
       RETURNING id, f_name, l_name, username, email, phone, city, state, country, zip_code`,
      [
        f_name,
        l_name,
        email,
        phone || null,
        city || null,
        state || null,
        country || null,
        zip_code || null,
        customerId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const updatedCustomer = result.rows[0];

    res.json({
      message: "Profile updated successfully",
      user: {
        id: updatedCustomer.id,
        f_name: updatedCustomer.f_name,
        l_name: updatedCustomer.l_name,
        username: updatedCustomer.username,
        email: updatedCustomer.email,
        phone: updatedCustomer.phone || "",
        city: updatedCustomer.city || "",
        state: updatedCustomer.state || "",
        country: updatedCustomer.country || "",
        zip_code: updatedCustomer.zip_code || "",
      },
    });
  } catch (err) {
    console.error("Update profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- CHANGE Password ---- */
app.post("/api/change-password", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;
    const { currentPassword, newPassword } = req.body;

    // Validate inputs
    if (!currentPassword || !newPassword) {
      return res.status(400).json({
        message: "Current password and new password are required",
      });
    }

    // Validate new password strength
    const passwordRegex =
      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/;
    if (!passwordRegex.test(newPassword)) {
      return res.status(400).json({
        message:
          "New password must be at least 8 characters with uppercase, lowercase, number, and special character",
      });
    }

    // Fetch customer
    const customerResult = await db.query(
      "SELECT password FROM customers WHERE id = $1",
      [customerId],
    );

    if (!customerResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const customer = customerResult.rows[0];

    // Verify current password
    const passwordMatch = await bcrypt.compare(
      currentPassword,
      customer.password,
    );

    if (!passwordMatch) {
      return res.status(401).json({ message: "Current password is incorrect" });
    }

    // Check if new password is same as current
    const samePassword = await bcrypt.compare(newPassword, customer.password);
    if (samePassword) {
      return res.status(400).json({
        message: "New password must be different from current password",
      });
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(newPassword, 10);

    // Update password
    await db.query("UPDATE customers SET password = $1 WHERE id = $2", [
      hashedPassword,
      customerId,
    ]);

    res.json({
      message: "Password changed successfully",
    });
  } catch (err) {
    console.error("Change password error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- Careers (Public Apply + Admin View) ---- */
app.post("/api/careers", async (req, res) => {
  try {
    const b = req.body || {};

    const cleanText = (value) => {
      if (value === undefined || value === null) return null;
      const text = String(value).trim();
      return text.length ? text : null;
    };

    const cleanDate = (value) => {
      const text = cleanText(value);
      if (!text) return null;
      const parsed = new Date(text);
      if (Number.isNaN(parsed.getTime())) return null;
      return parsed.toISOString().slice(0, 10);
    };

    const cleanNumber = (value) => {
      if (value === undefined || value === null || value === "") return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    const role = cleanText(b.role || b.applied_role);
    const gender = cleanText(b.gender);
    const firstName = cleanText(b.first_name || b.firstName);
    const lastName = cleanText(b.last_name || b.lastName);
    const email = cleanText(b.email);
    const phone = cleanText(b.phone);
    const dob = cleanDate(b.dob);
    const education =
      b.education && typeof b.education === "object" ? b.education : null;
    const experienceLevel = cleanText(b.experience_level || b.experienceLevel);
    const employmentStatus = cleanText(
      b.employment_status || b.employmentStatus,
    );
    const currentCompany = cleanText(b.current_company || b.currentCompany);
    const currentRole = cleanText(b.current_role || b.currentRole);
    const noticePeriod = cleanText(b.notice_period || b.noticePeriod);
    const preferredLocation = cleanText(
      b.preferred_location || b.preferredLocation,
    );
    const expectedCtc = cleanNumber(b.expected_ctc ?? b.expectedCtc);
    const skills = cleanText(b.skills);
    const projects = cleanText(b.projects);
    const coverLetter = cleanText(b.cover_letter || b.coverLetter);
    const applicationPlace = cleanText(
      b.application_place || b.applicationPlace,
    );
    const applicationDate = cleanDate(b.application_date ?? b.applicationDate);
    const agreeTerms = Boolean(b.agree_terms ?? b.agreeTerms);
    const source = cleanText(b.source || "hooks-web-careers");

    if (
      !role ||
      !firstName ||
      !lastName ||
      !email ||
      !phone ||
      !applicationPlace ||
      !applicationDate ||
      !agreeTerms
    ) {
      return res.status(400).json({
        message:
          "role, first name, last name, email, phone, application place, application date and consent are required",
      });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    const inserted = await db.query(
      `INSERT INTO career_applications (
         role, gender, first_name, last_name, email, phone, dob, education,
         experience_level, employment_status, current_company, current_designation,
         notice_period, preferred_location, expected_ctc, skills, projects,
         cover_letter, application_place, application_date, agree_terms, source,
         payload
       ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8,
         $9, $10, $11, $12, $13, $14, $15, $16, $17,
         $18, $19, $20, $21, $22, $23
       )
       RETURNING id, created_at`,
      [
        role,
        gender,
        firstName,
        lastName,
        email.toLowerCase(),
        phone,
        dob,
        education,
        experienceLevel,
        employmentStatus,
        currentCompany,
        currentRole,
        noticePeriod,
        preferredLocation,
        expectedCtc,
        skills,
        projects,
        coverLetter,
        applicationPlace,
        applicationDate,
        agreeTerms,
        source,
        b,
      ],
    );

    try {
      await sendCareerApplicationEmail({
        email: email.toLowerCase(),
        role,
        firstName,
        lastName,
      });
    } catch (mailErr) {
      console.error("Career application email error:", mailErr);
    }

    return res.status(201).json({
      message: "Application submitted successfully",
      application: inserted.rows[0],
    });
  } catch (err) {
    console.error("Create career application error:", err);
    return res.status(500).json({ message: "Failed to submit application" });
  }
});

app.get(
  "/api/users",
  authenticate,
  requireRolePermissions(["users.view", "users.manage"], { any: true }),
  async (req, res) => {
    try {
      const includeInactive =
        String(req.query.includeInactive || "true")
          .trim()
          .toLowerCase() !== "false";
      const users = await listRbacUsers({ includeInactive });
      return res.json(users);
    } catch (err) {
      console.error("GET /api/users error:", err);
      return res.status(500).json({ message: "Failed to fetch users" });
    }
  },
);

app.get(
  "/api/rbac/users",
  authenticate,
  requireRolePermissions(["users.view", "users.manage"], { any: true }),
  async (req, res) => {
    try {
      const includeInactive =
        String(req.query.includeInactive || "true")
          .trim()
          .toLowerCase() !== "false";
      const users = await listRbacUsers({ includeInactive });
      return res.json(users);
    } catch (err) {
      console.error("GET /api/rbac/users error:", err);
      return res.status(500).json({ message: "Failed to fetch users" });
    }
  },
);

app.get(
  "/api/users/:id",
  authenticate,
  requireRolePermissions(["users.view", "users.manage"], { any: true }),
  async (req, res) => {
    try {
      const user = await resolveRbacUserById(req.params.id);
      if (!user) return res.status(404).json({ message: "User not found" });
      return res.json({ user });
    } catch (err) {
      console.error("GET /api/users/:id error:", err);
      return res.status(500).json({ message: "Failed to fetch user" });
    }
  },
);

app.put(
  "/api/users/:id",
  authenticate,
  requireRolePermissions(["users.edit", "users.manage"], { any: true }),
  async (req, res) => {
    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ message: "Invalid user id" });
      }

      const existingResult = await db.query(
        'SELECT * FROM "user" WHERE id = $1',
        [userId],
      );
      if (!existingResult.rows.length) {
        return res.status(404).json({ message: "User not found" });
      }

      const existing = existingResult.rows[0];
      const body = req.body || {};
      const user_name =
        String(
          body.user_name || body.username || existing.user_name || "",
        ).trim() || null;
      const first_name =
        String(body.first_name || existing.first_name || "").trim() || null;
      const last_name =
        String(body.last_name || existing.last_name || "").trim() || null;
      const phone = String(body.phone || existing.phone || "").trim() || null;
      const gender =
        String(body.gender || existing.gender || "").trim() || null;
      const email = String(body.email || existing.email || "")
        .trim()
        .toLowerCase();
      const role = normalizeRole(body.role || existing.role || "viewer");
      const displayName =
        String(body.display_name || "").trim() ||
        [first_name, last_name].filter(Boolean).join(" ").trim() ||
        user_name ||
        email ||
        "User";
      const bio = String(body.bio || existing.bio || "").trim() || null;
      const department =
        String(body.department || existing.department || "").trim() || null;
      const status = normalizeUserStatus(body.status || existing.status);
      const avatar =
        String(
          body.avatar || body.avatar_url || existing.avatar || "",
        ).trim() || null;
      const permissionsOverride = Array.from(
        new Set(
          parseJsonArray(
            body.permissions_override ||
              body.permissions ||
              existing.permissions_override ||
              [],
            [],
          )
            .map((permission) => normalizePermissionToken(permission))
            .filter(Boolean),
        ),
      );
      const password = String(body.password || "").trim();

      const duplicateEmail = await db.query(
        'SELECT id FROM "user" WHERE LOWER(email) = $1 AND id != $2 LIMIT 1',
        [email, userId],
      );
      if (duplicateEmail.rows.length) {
        return res.status(409).json({ message: "Email already registered" });
      }

      const hashedPassword = password ? await bcrypt.hash(password, 10) : null;

      const query = password
        ? `
          UPDATE "user"
          SET user_name = $1,
              first_name = $2,
              last_name = $3,
              phone = $4,
              gender = $5,
              email = $6,
              role = $7,
              display_name = $8,
              bio = $9,
              department = $10,
              status = $11,
              avatar = $12,
              permissions_override = $13::jsonb,
              password = $14,
              updated_at = now()
          WHERE id = $15
          RETURNING *`
        : `
          UPDATE "user"
          SET user_name = $1,
              first_name = $2,
              last_name = $3,
              phone = $4,
              gender = $5,
              email = $6,
              role = $7,
              display_name = $8,
              bio = $9,
              department = $10,
              status = $11,
              avatar = $12,
              permissions_override = $13::jsonb,
              updated_at = now()
          WHERE id = $14
          RETURNING *`;

      const params = password
        ? [
            user_name,
            first_name,
            last_name,
            phone,
            gender,
            email,
            role,
            displayName,
            bio,
            department,
            status,
            avatar,
            JSON.stringify(permissionsOverride),
            hashedPassword,
            userId,
          ]
        : [
            user_name,
            first_name,
            last_name,
            phone,
            gender,
            email,
            role,
            displayName,
            bio,
            department,
            status,
            avatar,
            JSON.stringify(permissionsOverride),
            userId,
          ];

      const result = await db.query(query, params);
      const roleRecord = await getRoleAccessRow(result.rows[0]?.role);
      const savedUser = normalizeAdminUserRow(result.rows[0], roleRecord);

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "users",
        action: "updated",
        targetType: "user",
        targetId: savedUser.id,
        targetLabel: savedUser.display_name,
        note: "Updated an admin user.",
        meta: { role: savedUser.role },
      });

      return res.json({
        message: "User updated successfully.",
        user: savedUser,
      });
    } catch (err) {
      console.error("PUT /api/users/:id error:", err);
      return res.status(500).json({ message: "Failed to update user" });
    }
  },
);

app.delete(
  "/api/users/:id",
  authenticate,
  requireRolePermissions(["users.delete", "users.manage"], { any: true }),
  async (req, res) => {
    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ message: "Invalid user id" });
      }

      const result = await db.query(
        'DELETE FROM "user" WHERE id = $1 RETURNING id, user_name, display_name, role',
        [userId],
      );

      if (!result.rows.length) {
        return res.status(404).json({ message: "User not found" });
      }

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "users",
        action: "deleted",
        targetType: "user",
        targetId: result.rows[0].id,
        targetLabel: result.rows[0].display_name || result.rows[0].user_name,
        note: "Deleted an admin user.",
        meta: { role: result.rows[0].role || "viewer" },
      });

      return res.json({
        message: "User deleted successfully",
        user: result.rows[0],
      });
    } catch (err) {
      console.error("DELETE /api/users/:id error:", err);
      return res.status(500).json({ message: "Failed to delete user" });
    }
  },
);

app.post(
  "/api/rbac/users/:id/roles",
  authenticate,
  requireRolePermissions(["users.assign", "roles.manage", "users.manage"], {
    any: true,
  }),
  async (req, res) => {
    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ message: "Invalid user id" });
      }

      const roleId = String(req.body?.role_id || req.body?.roleId || "").trim();
      if (!roleId) {
        return res.status(400).json({ message: "role_id is required" });
      }

      const roleResult = await db.query(
        `
        SELECT id, name, title, description, permissions, built_in, created_at, updated_at
        FROM admin_roles
        WHERE CAST(id AS TEXT) = $1 OR LOWER(name) = LOWER($1)
        LIMIT 1
      `,
        [roleId],
      );
      const roleRecord = normalizeAdminRoleRow(roleResult.rows[0] || null);
      const roleName = roleRecord?.name || normalizeRole(roleId);

      const updateResult = await db.query(
        `
        UPDATE "user"
        SET role = $1, updated_at = now()
        WHERE id = $2
        RETURNING *
      `,
        [roleName, userId],
      );
      if (!updateResult.rows.length) {
        return res.status(404).json({ message: "User not found" });
      }

      const savedUser = normalizeAdminUserRow(updateResult.rows[0], roleRecord);

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "roles",
        action: "assigned",
        targetType: "user",
        targetId: savedUser.id,
        targetLabel: savedUser.display_name,
        note: `Assigned ${roleName} role.`,
        meta: { role: roleName },
      });

      return res.json({
        message: "Role assigned successfully",
        user: savedUser,
        role: roleRecord,
      });
    } catch (err) {
      console.error("POST /api/rbac/users/:id/roles error:", err);
      return res.status(500).json({ message: "Failed to assign role" });
    }
  },
);

app.get(
  "/api/rbac/roles",
  authenticate,
  requireRolePermissions(["roles.view", "permissions.view", "roles.manage"], {
    any: true,
  }),
  async (_req, res) => {
    try {
      const roles = await listRbacRoles();
      return res.json(roles);
    } catch (err) {
      console.error("GET /api/rbac/roles error:", err);
      return res.status(500).json({ message: "Failed to fetch roles" });
    }
  },
);

app.post(
  "/api/rbac/roles",
  authenticate,
  requireRolePermissions(["roles.create", "roles.manage"], { any: true }),
  async (req, res) => {
    try {
      const name = normalizeRole(req.body?.name || req.body?.id || "");
      if (!name) {
        return res.status(400).json({ message: "Role name is required" });
      }

      const title = String(
        req.body?.title || getRolePreset(name).label || name,
      ).trim();
      const description = String(
        req.body?.description || getRolePreset(name).description || "",
      ).trim();
      const permissions = Array.from(
        new Set(
          parseJsonArray(req.body?.permissions || [], [])
            .map((permission) => normalizePermissionToken(permission))
            .filter(Boolean),
        ),
      );

      const result = await db.query(
        `
        INSERT INTO admin_roles (name, title, description, permissions, built_in)
        VALUES ($1,$2,$3,$4::jsonb,$5)
        ON CONFLICT (name)
        DO UPDATE SET
          title = EXCLUDED.title,
          description = EXCLUDED.description,
          permissions = EXCLUDED.permissions,
          built_in = EXCLUDED.built_in,
          updated_at = now()
        RETURNING id, name, title, description, permissions, built_in, created_at, updated_at
      `,
        [
          name,
          title,
          description,
          JSON.stringify(permissions),
          Boolean(req.body?.built_in),
        ],
      );

      const role = normalizeAdminRoleRow(result.rows[0]);
      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "roles",
        action: "created",
        targetType: "role",
        targetId: role.id,
        targetLabel: role.title,
        note: "Created or updated a role.",
        meta: { permissions: role.permissions },
      });

      return res.status(201).json({ role });
    } catch (err) {
      console.error("POST /api/rbac/roles error:", err);
      return res.status(500).json({ message: "Failed to save role" });
    }
  },
);

app.put(
  "/api/rbac/roles/:id",
  authenticate,
  requireRolePermissions(["roles.edit", "roles.manage"], { any: true }),
  async (req, res) => {
    try {
      const lookup = String(req.params.id || "").trim();
      const existing = await db.query(
        `
        SELECT id, name, title, description, permissions, built_in, created_at, updated_at
        FROM admin_roles
        WHERE CAST(id AS TEXT) = $1 OR LOWER(name) = LOWER($1)
        LIMIT 1
      `,
        [lookup],
      );
      if (!existing.rows.length) {
        return res.status(404).json({ message: "Role not found" });
      }

      const current = normalizeAdminRoleRow(existing.rows[0]);
      const name = normalizeRole(req.body?.name || current.name);
      const title = String(req.body?.title || current.title || name).trim();
      const description = String(
        req.body?.description || current.description || "",
      ).trim();
      const permissions = Array.from(
        new Set(
          parseJsonArray(req.body?.permissions || current.permissions || [], [])
            .map((permission) => normalizePermissionToken(permission))
            .filter(Boolean),
        ),
      );

      const result = await db.query(
        `
        UPDATE admin_roles
        SET name = $1,
            title = $2,
            description = $3,
            permissions = $4::jsonb,
            built_in = $5,
            updated_at = now()
        WHERE id = $6
        RETURNING id, name, title, description, permissions, built_in, created_at, updated_at
      `,
        [
          name,
          title,
          description,
          JSON.stringify(permissions),
          Boolean(req.body?.built_in),
          current.id,
        ],
      );

      const role = normalizeAdminRoleRow(result.rows[0]);
      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "roles",
        action: "updated",
        targetType: "role",
        targetId: role.id,
        targetLabel: role.title,
        note: "Updated a role.",
        meta: { permissions: role.permissions },
      });

      return res.json({ role });
    } catch (err) {
      console.error("PUT /api/rbac/roles/:id error:", err);
      return res.status(500).json({ message: "Failed to update role" });
    }
  },
);

app.delete(
  "/api/rbac/roles/:id",
  authenticate,
  requireRolePermissions(["roles.delete", "roles.manage"], { any: true }),
  async (req, res) => {
    try {
      const lookup = String(req.params.id || "").trim();
      const result = await db.query(
        `
        DELETE FROM admin_roles
        WHERE CAST(id AS TEXT) = $1 OR LOWER(name) = LOWER($1)
        RETURNING id, name, title
      `,
        [lookup],
      );
      if (!result.rows.length) {
        return res.status(404).json({ message: "Role not found" });
      }

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "roles",
        action: "deleted",
        targetType: "role",
        targetId: result.rows[0].id,
        targetLabel: result.rows[0].title || result.rows[0].name,
        note: "Deleted a role.",
      });

      return res.json({ role: result.rows[0] });
    } catch (err) {
      console.error("DELETE /api/rbac/roles/:id error:", err);
      return res.status(500).json({ message: "Failed to delete role" });
    }
  },
);

app.get(
  "/api/rbac/permissions",
  authenticate,
  requireRolePermissions(["permissions.view", "permissions.manage"], {
    any: true,
  }),
  async (_req, res) => {
    try {
      const permissions = await listRbacPermissions();
      return res.json(permissions);
    } catch (err) {
      console.error("GET /api/rbac/permissions error:", err);
      return res.status(500).json({ message: "Failed to fetch permissions" });
    }
  },
);

app.post(
  "/api/rbac/permissions",
  authenticate,
  requireRolePermissions(["permissions.create", "permissions.manage"], {
    any: true,
  }),
  async (req, res) => {
    try {
      const name = normalizePermissionToken(
        req.body?.name || req.body?.id || "",
      );
      if (!name) {
        return res.status(400).json({ message: "Permission name is required" });
      }

      const description = String(req.body?.description || "").trim();
      const moduleKey = String(
        req.body?.module || req.body?.module_key || "",
      ).trim();
      const action = String(req.body?.action || "").trim();
      const result = await db.query(
        `
        INSERT INTO admin_permissions (
          name,
          description,
          module_key,
          action,
          built_in
        )
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (name)
        DO UPDATE SET
          description = EXCLUDED.description,
          module_key = EXCLUDED.module_key,
          action = EXCLUDED.action,
          built_in = EXCLUDED.built_in,
          updated_at = now()
        RETURNING id, name, description, module_key, action, built_in, created_at, updated_at
      `,
        [
          name,
          description,
          moduleKey || null,
          action || null,
          Boolean(req.body?.built_in),
        ],
      );

      const permission = normalizeAdminPermissionRow({
        ...result.rows[0],
        module: result.rows[0]?.module_key,
      });

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "permissions",
        action: "created",
        targetType: "permission",
        targetId: permission.id,
        targetLabel: permission.name,
        note: "Created or updated a permission.",
      });

      return res.status(201).json({ permission });
    } catch (err) {
      console.error("POST /api/rbac/permissions error:", err);
      return res.status(500).json({ message: "Failed to save permission" });
    }
  },
);

app.put(
  "/api/rbac/permissions/:id",
  authenticate,
  requireRolePermissions(["permissions.edit", "permissions.manage"], {
    any: true,
  }),
  async (req, res) => {
    try {
      const lookup = String(req.params.id || "").trim();
      const existing = await db.query(
        `
        SELECT id, name, description, module_key, action, built_in, created_at, updated_at
        FROM admin_permissions
        WHERE CAST(id AS TEXT) = $1 OR LOWER(name) = LOWER($1)
        LIMIT 1
      `,
        [lookup],
      );
      if (!existing.rows.length) {
        return res.status(404).json({ message: "Permission not found" });
      }

      const current = normalizeAdminPermissionRow(existing.rows[0]);
      const name = normalizePermissionToken(req.body?.name || current.name);
      const description = String(
        req.body?.description || current.description || "",
      ).trim();
      const moduleKey = String(
        req.body?.module || req.body?.module_key || current.module || "",
      ).trim();
      const action = String(req.body?.action || current.action || "").trim();
      const result = await db.query(
        `
        UPDATE admin_permissions
        SET name = $1,
            description = $2,
            module_key = $3,
            action = $4,
            built_in = $5,
            updated_at = now()
        WHERE id = $6
        RETURNING id, name, description, module_key, action, built_in, created_at, updated_at
      `,
        [
          name,
          description,
          moduleKey || null,
          action || null,
          Boolean(req.body?.built_in),
          current.id,
        ],
      );

      const permission = normalizeAdminPermissionRow({
        ...result.rows[0],
        module: result.rows[0]?.module_key,
      });

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "permissions",
        action: "updated",
        targetType: "permission",
        targetId: permission.id,
        targetLabel: permission.name,
        note: "Updated a permission.",
      });

      return res.json({ permission });
    } catch (err) {
      console.error("PUT /api/rbac/permissions/:id error:", err);
      return res.status(500).json({ message: "Failed to update permission" });
    }
  },
);

app.delete(
  "/api/rbac/permissions/:id",
  authenticate,
  requireRolePermissions(["permissions.delete", "permissions.manage"], {
    any: true,
  }),
  async (req, res) => {
    try {
      const lookup = String(req.params.id || "").trim();
      const result = await db.query(
        `
        DELETE FROM admin_permissions
        WHERE CAST(id AS TEXT) = $1 OR LOWER(name) = LOWER($1)
        RETURNING id, name
      `,
        [lookup],
      );
      if (!result.rows.length) {
        return res.status(404).json({ message: "Permission not found" });
      }

      await recordAdminActivity({
        actorUserId: req.user?.id || null,
        actorName: req.user?.username || req.user?.email || "System",
        actorRole: req.user?.role || "admin",
        moduleKey: "permissions",
        action: "deleted",
        targetType: "permission",
        targetId: result.rows[0].id,
        targetLabel: result.rows[0].name,
        note: "Deleted a permission.",
      });

      return res.json({ permission: result.rows[0] });
    } catch (err) {
      console.error("DELETE /api/rbac/permissions/:id error:", err);
      return res.status(500).json({ message: "Failed to delete permission" });
    }
  },
);

app.get(
  "/api/rbac/activity",
  authenticate,
  requireRolePermissions(["activity.view"], { any: true }),
  async (req, res) => {
    try {
      const limit = Math.min(500, Number(req.query.limit || 100) || 100);
      const activities = await listRbacActivities({ limit });
      return res.json(activities);
    } catch (err) {
      console.error("GET /api/rbac/activity error:", err);
      return res.status(500).json({ message: "Failed to fetch activity log" });
    }
  },
);

app.get("/api/admin/careers", authenticate, async (req, res) => {
  try {
    const pageRaw = Number(req.query.page);
    const limitRaw = Number(req.query.limit);
    const page = Number.isFinite(pageRaw) && pageRaw > 0 ? pageRaw : 1;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 100) : 25;
    const offset = (page - 1) * limit;

    const [rowsResult, countResult] = await Promise.all([
      db.query(
        `SELECT id, role, first_name, last_name, email, phone,
                  gender, dob, education, experience_level, employment_status,
                  notice_period, preferred_location, expected_ctc, status,
                  created_at, updated_at, current_company, current_designation,
                  application_date, skills, projects, cover_letter,
                  application_place, source, assignment_pdf_url,
                  assignment_due_date, assignment_notes, interview_link,
                  interview_scheduled_at, interview_notes, hr_scheduled_at, hr_notes,
                  offer_pdf_url, offer_notes
           FROM career_applications
           ORDER BY created_at DESC
           LIMIT $1 OFFSET $2`,
        [limit, offset],
      ),
      db.query(`SELECT COUNT(*)::int AS total FROM career_applications`),
    ]);

    return res.json({
      page,
      limit,
      total: countResult.rows[0]?.total || 0,
      rows: rowsResult.rows,
    });
  } catch (err) {
    console.error("List career applications error:", err);
    return res.status(500).json({ message: "Failed to fetch applications" });
  }
});

const CAREER_APPLICATION_STATUSES = new Set([
  "new",
  "screening",
  "shortlisted",
  "interview_scheduled",
  "hr_round",
  "offered",
  "hired",
  "rejected",
]);

app.patch("/api/admin/careers/:id/status", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const rawStatus = String(req.body?.status || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z_]/g, "");

    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    if (!rawStatus) {
      return res.status(400).json({ message: "status is required" });
    }

    if (!CAREER_APPLICATION_STATUSES.has(rawStatus)) {
      return res.status(400).json({
        message:
          "Invalid status. Allowed values: new, screening, shortlisted, interview_scheduled, hr_round, offered, hired, rejected",
      });
    }

    const result = await db.query(
      `UPDATE career_applications
       SET status = $1, updated_at = now()
       WHERE id = $2
       RETURNING id, status, updated_at`,
      [rawStatus, id],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Application not found" });
    }

    return res.json({
      message: "Application status updated",
      application: result.rows[0],
    });
  } catch (err) {
    console.error("Update career application status error:", err);
    return res.status(500).json({ message: "Failed to update status" });
  }
});

const CAREER_NOTIFY_TYPES = new Set(["assignment", "interview", "hr", "offer"]);

const normalizeCareerText = (value) => {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text.length ? text : null;
};

const parseCareerDate = (value) => {
  const text = normalizeCareerText(value);
  if (!text) return null;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const parseCareerDateTime = (value) => {
  const text = normalizeCareerText(value);
  if (!text) return null;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
};

const formatCareerDateLabel = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleDateString("en-IN", {
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
};

app.post("/api/admin/careers/:id/notify", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const type = String(req.body?.type || "")
      .trim()
      .toLowerCase();
    if (!CAREER_NOTIFY_TYPES.has(type)) {
      return res.status(400).json({ message: "Invalid notify type" });
    }

    const subject = normalizeCareerText(req.body?.subject);
    const message = normalizeCareerText(req.body?.message);
    const pdfUrl = normalizeCareerText(req.body?.pdf_url || req.body?.pdfUrl);
    const offerUrl = normalizeCareerText(
      req.body?.offer_url || req.body?.offerUrl,
    );
    const meetLink = normalizeCareerText(
      req.body?.meet_link || req.body?.meetLink,
    );
    const dueDate = parseCareerDate(req.body?.due_date || req.body?.dueDate);
    const scheduledAt = parseCareerDateTime(
      req.body?.scheduled_at || req.body?.scheduledAt,
    );
    const timeZone = normalizeCareerText(
      req.body?.time_zone || req.body?.timeZone,
    );

    const result = await db.query(
      `SELECT id, role, first_name, last_name, email
       FROM career_applications
       WHERE id = $1`,
      [id],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Application not found" });
    }

    const applicant = result.rows[0];
    let status = null;
    let updateQuery = "";
    let updateValues = [];

    if (type === "assignment") {
      status = "shortlisted";
      const dueDateLabel = formatCareerDateLabel(dueDate);
      await sendCareerAssignmentEmail({
        email: applicant.email,
        role: applicant.role,
        firstName: applicant.first_name,
        lastName: applicant.last_name,
        subject,
        message,
        pdfUrl,
        dueDateLabel,
      });
      updateQuery = `
        UPDATE career_applications
        SET status = $1,
            assignment_pdf_url = $2,
            assignment_due_date = $3,
            assignment_notes = $4,
            updated_at = now()
        WHERE id = $5
        RETURNING id, status, updated_at, assignment_pdf_url, assignment_due_date, assignment_notes
      `;
      updateValues = [status, pdfUrl, dueDate, message, id];
    }

    if (type === "interview") {
      status = "interview_scheduled";
      await sendCareerInterviewEmail({
        email: applicant.email,
        role: applicant.role,
        firstName: applicant.first_name,
        lastName: applicant.last_name,
        subject,
        message,
        meetLink,
        scheduledAt,
        timeZone,
      });
      updateQuery = `
        UPDATE career_applications
        SET status = $1,
            interview_link = $2,
            interview_scheduled_at = $3,
            interview_notes = $4,
            updated_at = now()
        WHERE id = $5
        RETURNING id, status, updated_at, interview_link, interview_scheduled_at, interview_notes
      `;
      updateValues = [status, meetLink, scheduledAt, message, id];
    }

    if (type === "hr") {
      status = "hr_round";
      await sendCareerHrEmail({
        email: applicant.email,
        role: applicant.role,
        firstName: applicant.first_name,
        lastName: applicant.last_name,
        subject,
        message,
        scheduledAt,
        timeZone,
      });
      updateQuery = `
        UPDATE career_applications
        SET status = $1,
            hr_scheduled_at = $2,
            hr_notes = $3,
            updated_at = now()
        WHERE id = $4
        RETURNING id, status, updated_at, hr_scheduled_at, hr_notes
      `;
      updateValues = [status, scheduledAt, message, id];
    }

    if (type === "offer") {
      status = "offered";
      await sendCareerOfferEmail({
        email: applicant.email,
        role: applicant.role,
        firstName: applicant.first_name,
        lastName: applicant.last_name,
        subject,
        message,
        offerUrl,
      });
      updateQuery = `
        UPDATE career_applications
        SET status = $1,
            offer_pdf_url = $2,
            offer_notes = $3,
            updated_at = now()
        WHERE id = $4
        RETURNING id, status, updated_at, offer_pdf_url, offer_notes
      `;
      updateValues = [status, offerUrl, message, id];
    }

    const updateResult = await db.query(updateQuery, updateValues);

    return res.json({
      message: "Notification sent",
      application: updateResult.rows[0],
    });
  } catch (err) {
    console.error("Career notify error:", err);
    return res.status(500).json({ message: "Failed to send notification" });
  }
});

/* ---- Blogs (Eligibility + Suggestions + Editor) ---- */
app.get("/api/admin/blogs/candidates", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "view"))) return;

    const rawType = String(req.query.type || "smartphone")
      .trim()
      .toLowerCase();
    const type = normalizeProfileDeviceType(rawType);
    if (!BLOG_ALLOWED_PRODUCT_TYPES.has(type)) {
      return res.status(400).json({ message: "Invalid product type" });
    }

    const limit = Math.min(100, toPositiveInt(req.query.limit, 25));
    const profileConfig = await readDeviceFieldProfilesConfig();

    const baseResult = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        COALESCE(pub.is_published, false) AS is_published
      FROM products p
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_publish pub
        ON pub.product_id = p.id
      WHERE p.product_type = $1
      ORDER BY p.id DESC
      LIMIT $2
    `,
      [type, limit],
    );

    const rows = [];
    for (const baseRow of baseResult.rows || []) {
      const snapshot = await fetchBlogProductSnapshot(
        baseRow.product_id,
        profileConfig.profiles,
        baseRow,
      );
      if (!snapshot) continue;

      const price = formatBlogPrice(snapshot.lowest_price);
      rows.push({
        product_id: baseRow.product_id,
        product_type: type,
        name: baseRow.name || "",
        brand_name: baseRow.brand_name || "",
        is_published: Boolean(baseRow.is_published),
        spec_score: toSafeFiniteNumber(snapshot?.scored?.spec_score, 0),
        price: price || null,
        image: snapshot.hero_image || null,
      });
    }

    return res.json({
      type,
      limit,
      total: rows.length,
      rows,
    });
  } catch (err) {
    console.error("GET /api/admin/blogs/candidates error:", err);
    return res.status(500).json({ message: "Failed to fetch blog candidates" });
  }
});

app.get(
  "/api/admin/blogs/suggestions/:productId",
  authenticate,
  async (req, res) => {
    try {
      if (!(await ensureBlogManagerAccess(req, res, "view"))) return;

      const productId = Number(req.params.productId);
      if (!Number.isInteger(productId) || productId <= 0) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      const profileConfig = await readDeviceFieldProfilesConfig();
      const snapshot = await fetchBlogProductSnapshot(
        productId,
        profileConfig.profiles,
      );
      if (!snapshot) {
        return res.status(404).json({ message: "Product not found" });
      }

      const selectionContext = buildBlogSelectionContext([snapshot]);
      const suggestions = buildBlogSuggestionsForSelection(
        [snapshot],
        selectionContext.tokenMap,
      );
      const existingMatch = await findExistingBlogByOrderedProductSet([productId]);
      const existing = existingMatch?.id
        ? await db.query(
            `
              SELECT
                id,
                product_id,
                category,
                title,
                slug,
                excerpt,
                author_name,
                author_user_id,
                content_template,
                content_rendered,
                status,
                blog_eligible,
                meta_title,
                meta_description,
                hero_image_source,
                hero_image_alt,
                hero_image_caption,
                tags,
                featured,
                trending,
                pinned,
                CASE
                  WHEN hero_image_source = 'none' THEN NULL
                  ELSE COALESCE(
                    hero_image,
                    (
                      SELECT pi.image_url
                      FROM product_images pi
                      WHERE pi.product_id = blogs.product_id
                      ORDER BY pi.position ASC NULLS LAST, pi.id ASC
                      LIMIT 1
                    )
                  )
                END AS hero_image,
                published_at,
                created_at,
                updated_at
              FROM blogs
              WHERE id = $1
              LIMIT 1
            `,
            [existingMatch.id],
          )
        : { rows: [] };

      return res.json({
        primary_product_id: selectionContext.productIds[0] || null,
        product: selectionContext.products[0] || null,
        products: selectionContext.products,
        product_ids: selectionContext.productIds,
        token_map: selectionContext.tokenMap,
        token_keys: selectionContext.tokenKeys,
        suggestions,
        existing_blog: existing.rows[0] || null,
      });
    } catch (err) {
      console.error("GET /api/admin/blogs/suggestions/:productId error:", err);
      return res
        .status(500)
        .json({ message: "Failed to fetch blog suggestions" });
    }
  },
);

app.post("/api/admin/blogs/preview", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "edit"))) return;

    const productIds = orderBlogProductIds(
      req.body?.product_ids ?? req.body?.productIds,
      req.body?.primary_product_id ?? req.body?.primaryProductId ?? req.body?.product_id,
    );
    const content = String(req.body?.content || "");
    if (!content.trim()) {
      return res.status(400).json({ message: "content is required" });
    }

    let tokenMap = toPlainObject(req.body?.token_map);
    if (productIds.length > 0) {
      const profileConfig = await readDeviceFieldProfilesConfig();
      const snapshotResult = await fetchBlogSnapshotsByProductIds(
        productIds,
        profileConfig.profiles,
      );
      if (snapshotResult.missingIds.length) {
        return res.status(404).json({
          message: `Products not found: ${snapshotResult.missingIds.join(", ")}`,
        });
      }
      tokenMap = buildBlogSelectionContext(
        snapshotResult.snapshots,
        tokenMap,
      ).tokenMap;
    }
    const rendered = renderBlogTemplateWithTokens(content, tokenMap, {
      preserveUnknown: true,
    });
    const unresolved = collectTemplateTokens(rendered);

    return res.json({
      rendered_content: rendered,
      unresolved_tokens: unresolved,
      token_map: tokenMap,
    });
  } catch (err) {
    console.error("POST /api/admin/blogs/preview error:", err);
    return res.status(500).json({ message: "Failed to preview blog content" });
  }
});

app.post("/api/admin/blogs", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "edit"))) return;

    const rawBlogId = Number(req.body?.blog_id);
    const hasBlogId = Number.isInteger(rawBlogId) && rawBlogId > 0;
    const productIds = orderBlogProductIds(
      req.body?.product_ids ?? req.body?.productIds ?? req.body?.products,
      req.body?.primary_product_id ??
        req.body?.primaryProductId ??
        req.body?.product_id,
    );
    let targetBlogId = hasBlogId ? rawBlogId : null;
    const productId = productIds[0] || null;

    const title = String(req.body?.title || "").trim();
    const excerpt = String(req.body?.excerpt || "").trim();
    const contentTemplate = String(req.body?.content_template || "").trim();
    const requestedSlug = String(req.body?.slug || "").trim();
    const requestedCategory = String(req.body?.category || "news")
      .trim()
      .toLowerCase();
    const requestedStatus = String(req.body?.status || "draft")
      .trim()
      .toLowerCase();
    const category = BLOG_ALLOWED_CATEGORIES.has(requestedCategory)
      ? requestedCategory
      : "news";
    const status = BLOG_ALLOWED_STATUSES.has(requestedStatus)
      ? requestedStatus
      : "draft";
    const metaTitle = String(req.body?.meta_title || "").trim();
    const metaDescription = String(req.body?.meta_description || "").trim();
    const authorName = String(
      req.body?.author_name || req.body?.authorName || "",
    ).trim();
    const authorUserIdRaw = Number(req.body?.author_user_id);
    const authorUserId =
      Number.isInteger(authorUserIdRaw) && authorUserIdRaw > 0
        ? authorUserIdRaw
        : null;
    const heroImageAlt = String(
      req.body?.hero_image_alt || req.body?.heroImageAlt || "",
    ).trim();
    const heroImageCaption = String(
      req.body?.hero_image_caption || req.body?.heroImageCaption || "",
    ).trim();
    const tags = parseBlogTags(req.body?.tags || req.body?.keywords);
    const featured = parseBlogBoolean(req.body?.featured);
    const trending = parseBlogBoolean(req.body?.trending);
    const pinned = parseBlogBoolean(req.body?.pinned);
    const publishedAtValue = parseBlogDate(
      req.body?.published_at || req.body?.publishedAt,
    );
    const heroImageSourceRaw = String(req.body?.hero_image_source || "")
      .trim()
      .toLowerCase();
    const heroImageSource =
      heroImageSourceRaw === "asset" ||
      heroImageSourceRaw === "url" ||
      heroImageSourceRaw === "none"
        ? heroImageSourceRaw
        : null;

    if (!title) return res.status(400).json({ message: "title is required" });
    if (!contentTemplate) {
      return res.status(400).json({ message: "content_template is required" });
    }

    if (!targetBlogId && productIds.length > 0) {
      const existingBySelection = await findExistingBlogByOrderedProductSet(
        productIds,
      );
      targetBlogId = Number(existingBySelection?.id) || null;
    }

    let existingBlog = null;
    if (targetBlogId) {
      const existingBlogResult = await db.query(
        `
        SELECT id, status, slug
        FROM blogs
        WHERE id = $1
        LIMIT 1
      `,
        [targetBlogId],
      );
      existingBlog = existingBlogResult.rows[0] || null;
    }

    let selectionContext = {
      primarySnapshot: null,
      productIds: [],
      products: [],
      tokenMap: toPlainObject(req.body?.token_map),
      tokenKeys: Object.keys(toPlainObject(req.body?.token_map)).sort(),
    };
    if (productIds.length > 0) {
      const profileConfig = await readDeviceFieldProfilesConfig();
      const snapshotResult = await fetchBlogSnapshotsByProductIds(
        productIds,
        profileConfig.profiles,
      );
      if (snapshotResult.missingIds.length) {
        return res.status(404).json({
          message: `Products not found: ${snapshotResult.missingIds.join(", ")}`,
        });
      }
      selectionContext = buildBlogSelectionContext(
        snapshotResult.snapshots,
        req.body?.token_map,
      );
    }

    const eligibilitySnapshot = {
      advisory_only: true,
      product_linked: productIds.length > 0,
      linked_product_count: productIds.length,
      product_ids: productIds,
    };
    const tokenMap = selectionContext.tokenMap;

    const contentRendered = renderBlogTemplateWithTokens(
      contentTemplate,
      tokenMap,
      {
        preserveUnknown: true,
      },
    );
    const slug = await resolveUniqueBlogSlug(
      requestedSlug || title || selectionContext.primarySnapshot?.core?.name,
      productId,
      targetBlogId,
    );
    const heroImage =
      heroImageSource === "none"
        ? ""
        : String(
            req.body?.hero_image ||
              selectionContext.primarySnapshot?.hero_image ||
              "",
          ).trim();
    const authorUser = authorUserId
      ? await resolveRbacUserById(authorUserId)
      : null;
    const resolvedAuthorName =
      authorName ||
      authorUser?.display_name ||
      authorUser?.author_name ||
      authorUser?.user_name ||
      null;
    const actorId =
      Number.isInteger(Number(req.user?.id)) && Number(req.user?.id) > 0
        ? Number(req.user.id)
        : null;

    const client = await db.connect();
    let writeResult;

    try {
      await client.query("BEGIN");

      if (targetBlogId) {
        writeResult = await client.query(
          `
            UPDATE blogs
            SET
              product_id = $2,
              category = $3,
              title = $4,
              slug = $5,
              excerpt = $6,
              author_name = $7,
              author_user_id = $8,
              content_template = $9,
              content_rendered = $10,
              status = $11,
              blog_eligible = $12,
              eligibility_snapshot = $13::jsonb,
              token_snapshot = $14::jsonb,
              meta_title = $15,
              meta_description = $16,
              hero_image = $17,
              hero_image_source = $18,
              hero_image_alt = $19,
              hero_image_caption = $20,
              tags = $21::jsonb,
              featured = $22,
              trending = $23,
              pinned = $24,
              updated_by = $25,
              published_at = CASE
                WHEN $11 = 'published' THEN COALESCE($26, published_at, now())
                ELSE $26
              END,
              updated_at = now()
            WHERE id = $1
            RETURNING
              id,
              product_id,
              category,
              title,
              slug,
              excerpt,
              author_name,
              author_user_id,
              content_template,
              content_rendered,
              status,
              blog_eligible,
              eligibility_snapshot,
              token_snapshot,
              meta_title,
              meta_description,
              hero_image,
              hero_image_source,
              hero_image_alt,
              hero_image_caption,
              tags,
              featured,
              trending,
              pinned,
              published_at,
              created_at,
              updated_at
          `,
          [
            targetBlogId,
            productId,
            category,
            title,
            slug,
            excerpt || null,
            resolvedAuthorName || null,
            authorUserId,
            contentTemplate,
            contentRendered,
            status,
            productIds.length > 0,
            JSON.stringify(eligibilitySnapshot),
            JSON.stringify(tokenMap),
            metaTitle || null,
            metaDescription || null,
            heroImage || null,
            heroImageSource,
            heroImageAlt || null,
            heroImageCaption || null,
            JSON.stringify(tags),
            featured,
            trending,
            pinned,
            actorId,
            publishedAtValue,
          ],
        );
        if (!writeResult.rows.length) {
          await client.query("ROLLBACK");
          return res.status(404).json({ message: "Blog not found" });
        }
      } else {
        writeResult = await client.query(
          `
            INSERT INTO blogs (
              product_id,
              category,
              title,
              slug,
              excerpt,
              author_name,
              author_user_id,
              content_template,
              content_rendered,
              status,
              blog_eligible,
              eligibility_snapshot,
              token_snapshot,
              meta_title,
              meta_description,
              hero_image,
              hero_image_source,
              hero_image_alt,
              hero_image_caption,
              tags,
              featured,
              trending,
              pinned,
              created_by,
              updated_by,
              published_at,
              created_at,
              updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14,$15,$16,$17,$18,$19,$20::jsonb,$21,$22,$23,$24,$25,$26,now(),now()
            )
            RETURNING
              id,
              product_id,
              category,
              title,
              slug,
              excerpt,
              author_name,
              author_user_id,
              content_template,
              content_rendered,
              status,
              blog_eligible,
              eligibility_snapshot,
              token_snapshot,
              meta_title,
              meta_description,
              hero_image,
              hero_image_source,
              hero_image_alt,
              hero_image_caption,
              tags,
              featured,
              trending,
              pinned,
              published_at,
              created_at,
              updated_at
          `,
          [
            productId,
            category,
            title,
            slug,
            excerpt || null,
            resolvedAuthorName || null,
            authorUserId,
            contentTemplate,
            contentRendered,
            status,
            productIds.length > 0,
            JSON.stringify(eligibilitySnapshot),
            JSON.stringify(tokenMap),
            metaTitle || null,
            metaDescription || null,
            heroImage || null,
            heroImageSource,
            heroImageAlt || null,
            heroImageCaption || null,
            JSON.stringify(tags),
            featured,
            trending,
            pinned,
            actorId,
            actorId,
            publishedAtValue,
          ],
        );
      }

      const savedBlogId = Number(writeResult.rows[0]?.id) || null;
      await syncBlogProducts(client, savedBlogId, productIds);
      await client.query("COMMIT");
    } catch (writeErr) {
      await client.query("ROLLBACK");
      throw writeErr;
    } finally {
      client.release();
    }

    const savedBlog = writeResult.rows[0] || null;
    if (savedBlog) {
      savedBlog.product_ids = productIds;
      savedBlog.products = selectionContext.products;
      savedBlog.linked_product_count = productIds.length;
      savedBlog.token_map = tokenMap;
      savedBlog.token_keys = selectionContext.tokenKeys;
      savedBlog.product_names = selectionContext.products
        .map((product) => String(product?.name || "").trim())
        .filter(Boolean)
        .join(", ");
    }
    const shouldSendPublishedPush =
      savedBlog?.status === "published" && existingBlog?.status !== "published";
    let pushNotification = null;

    if (shouldSendPublishedPush) {
      try {
        pushNotification = await sendPublishedNewsPush(savedBlog);
      } catch (pushErr) {
        console.error("News push dispatch failed:", pushErr);
      }
    }

    // Ensure proper HTML encoding for API responses
    if (savedBlog) {
      await attachBlogProductsToRows([savedBlog]);
      savedBlog.content_template = ensureProperHtmlEncoding(
        savedBlog.content_template,
      );
      savedBlog.content_rendered = ensureProperHtmlEncoding(
        savedBlog.content_rendered,
      );
    }

    return res.status(201).json({
      message: "Blog saved successfully",
      blog: savedBlog,
      unresolved_tokens: collectTemplateTokens(contentRendered),
      push_notification: pushNotification,
    });
  } catch (err) {
    console.error("POST /api/admin/blogs error:", err);
    return res.status(500).json({ message: "Failed to save blog" });
  }
});

app.get("/api/admin/blogs", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "view"))) return;

    const page = toPositiveInt(req.query.page, 1);
    const limit = Math.min(100, toPositiveInt(req.query.limit, 20));
    const offset = (page - 1) * limit;
    const rawStatus = String(req.query.status || "")
      .trim()
      .toLowerCase();
    const status = BLOG_ALLOWED_STATUSES.has(rawStatus) ? rawStatus : null;

    const whereSql = status
      ? "WHERE bl.status = $1"
      : "WHERE bl.status IN ('draft', 'published')";
    const params = status ? [status, limit, offset] : [limit, offset];

    const listSql = `
      SELECT
        bl.id,
        bl.product_id,
        bl.category,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.author_name,
        bl.author_user_id,
        bl.status,
        bl.blog_eligible,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        CASE
          WHEN bl.hero_image_source = 'none' THEN NULL
          ELSE COALESCE(
            bl.hero_image,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = bl.product_id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            )
          )
        END AS hero_image,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        b.logo AS brand_logo
      FROM blogs bl
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      ${whereSql}
      ORDER BY bl.updated_at DESC, bl.id DESC
      LIMIT $${status ? 2 : 1} OFFSET $${status ? 3 : 2}
    `;

    const countSql = `
      SELECT COUNT(*)::int AS total
      FROM blogs bl
      ${status ? "WHERE bl.status = $1" : "WHERE bl.status IN ('draft', 'published')"}
    `;
    const countParams = status ? [status] : [];

    const [listRes, countRes] = await Promise.all([
      db.query(listSql, params),
      db.query(countSql, countParams),
    ]);
    const rows = Array.isArray(listRes.rows) ? listRes.rows : [];
    await attachBlogProductsToRows(rows);

    return res.json({
      page,
      limit,
      total: countRes.rows[0]?.total || 0,
      rows,
    });
  } catch (err) {
    console.error("GET /api/admin/blogs error:", err);
    return res.status(500).json({ message: "Failed to fetch blogs" });
  }
});

app.get("/api/admin/blogs/:id", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "view"))) return;

    const blogId = Number(req.params.id);
    if (!Number.isInteger(blogId) || blogId <= 0) {
      return res.status(400).json({ message: "Invalid blog id" });
    }

    const result = await db.query(
      `
      SELECT
        bl.id,
        bl.product_id,
        bl.category,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.author_name,
        bl.author_user_id,
        bl.content_template,
        bl.content_rendered,
        bl.status,
        bl.blog_eligible,
        bl.eligibility_snapshot,
        bl.token_snapshot,
        bl.meta_title,
        bl.meta_description,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        CASE
          WHEN bl.hero_image_source = 'none' THEN NULL
          ELSE COALESCE(
            bl.hero_image,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = bl.product_id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            )
          )
        END AS hero_image,
        bl.published_at,
        bl.created_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name
      FROM blogs bl
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE bl.id = $1
      LIMIT 1
    `,
      [blogId],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Blog not found" });
    }

    const blog = result.rows[0];
    await attachBlogProductsToRows([blog]);
    const productIds = normalizeBlogProductIds(blog.product_ids, blog.product_id);

    if (productIds.length > 0) {
      const profileConfig = await readDeviceFieldProfilesConfig().catch(() => ({
        profiles: [],
      }));
      const snapshotResult = await fetchBlogSnapshotsByProductIds(
        productIds,
        profileConfig.profiles,
      );
      const selectionContext = buildBlogSelectionContext(
        snapshotResult.snapshots,
        blog.token_snapshot,
      );

      blog.product_ids = selectionContext.productIds;
      blog.products = selectionContext.products;
      blog.linked_product_count = selectionContext.productIds.length;
      blog.product_names = selectionContext.products
        .map((product) => String(product?.name || "").trim())
        .filter(Boolean)
        .join(", ");
      blog.token_map = selectionContext.tokenMap;
      blog.token_keys = selectionContext.tokenKeys;
      if (!blog.product_name && selectionContext.products[0]?.name) {
        blog.product_name = selectionContext.products[0].name;
      }
      if (!blog.product_type && selectionContext.products[0]?.product_type) {
        blog.product_type = selectionContext.products[0].product_type;
      }
      if (!blog.brand_name && selectionContext.products[0]?.brand_name) {
        blog.brand_name = selectionContext.products[0].brand_name;
      }
    }

    // Ensure proper HTML encoding for API responses
    blog.content_template = ensureProperHtmlEncoding(blog.content_template);
    blog.content_rendered = ensureProperHtmlEncoding(blog.content_rendered);

    return res.json({ blog });
  } catch (err) {
    console.error("GET /api/admin/blogs/:id error:", err);
    return res.status(500).json({ message: "Failed to fetch blog" });
  }
});

app.delete("/api/admin/blogs/:id", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "delete"))) return;

    const blogId = Number(req.params.id);
    if (!Number.isInteger(blogId) || blogId <= 0) {
      return res.status(400).json({ message: "Invalid blog id" });
    }

    const deleteResult = await db.query(
      `
      DELETE FROM blogs
      WHERE id = $1
      RETURNING id, title, product_id
    `,
      [blogId],
    );

    if (!deleteResult.rows.length) {
      return res.status(404).json({ message: "Blog not found" });
    }

    return res.json({
      message: "Blog deleted successfully",
      blog: deleteResult.rows[0],
    });
  } catch (err) {
    console.error("DELETE /api/admin/blogs/:id error:", err);
    return res.status(500).json({ message: "Failed to delete blog" });
  }
});

app.get("/api/public/push/fcm/status", (_req, res) => {
  return res.json({
    configured: isFirebaseAdminConfigured(),
    topic: NEWS_PUSH_TOPIC,
    routes: {
      register: "/api/public/push/fcm/register",
      unregister: "/api/public/push/fcm/unregister",
    },
  });
});

app.post("/api/public/push/fcm/register", async (req, res) => {
  const token = normalizePushToken(req.body?.token);
  const topic = normalizePushTopic(req.body?.topic || NEWS_PUSH_TOPIC);
  const permission = normalizePushPermission(req.body?.permission);
  const userAgent = String(req.get("user-agent") || "").trim() || null;

  if (!token) {
    return res.status(400).json({ message: "A valid FCM token is required" });
  }

  if (!topic) {
    return res.status(400).json({ message: "Unsupported push topic" });
  }

  if (!isFirebaseAdminConfigured()) {
    return res.status(503).json({
      message: "Push notifications are not configured on the server yet",
    });
  }

  try {
    await subscribeTokenToTopic(token, topic);

    const result = await db.query(
      `
      INSERT INTO push_subscriptions (
        token,
        topic,
        platform,
        permission,
        user_agent,
        status,
        last_error,
        last_registered_at,
        updated_at
      ) VALUES ($1,$2,'web',$3,$4,'active',NULL,now(),now())
      ON CONFLICT (token, topic)
      DO UPDATE SET
        platform = 'web',
        permission = EXCLUDED.permission,
        user_agent = EXCLUDED.user_agent,
        status = 'active',
        last_error = NULL,
        last_registered_at = now(),
        updated_at = now()
      RETURNING id, topic, status, last_registered_at, updated_at
    `,
      [token, topic, permission, userAgent],
    );

    return res.status(201).json({
      message: "News alerts enabled",
      subscription: result.rows[0] || null,
    });
  } catch (err) {
    console.error("POST /api/public/push/fcm/register error:", err);

    await db
      .query(
        `
        INSERT INTO push_subscriptions (
          token,
          topic,
          platform,
          permission,
          user_agent,
          status,
          last_error,
          last_registered_at,
          updated_at
        ) VALUES ($1,$2,'web',$3,$4,'error',$5,now(),now())
        ON CONFLICT (token, topic)
        DO UPDATE SET
          permission = EXCLUDED.permission,
          user_agent = EXCLUDED.user_agent,
          status = 'error',
          last_error = EXCLUDED.last_error,
          updated_at = now()
      `,
        [token, topic, permission, userAgent, err?.message || "Unknown error"],
      )
      .catch(() => undefined);

    return res.status(502).json({
      message: "Unable to enable news alerts right now",
    });
  }
});

app.post("/api/public/push/fcm/unregister", async (req, res) => {
  const token = normalizePushToken(req.body?.token);
  const topic = normalizePushTopic(req.body?.topic || NEWS_PUSH_TOPIC);

  if (!token) {
    return res.status(400).json({ message: "A valid FCM token is required" });
  }

  if (!topic) {
    return res.status(400).json({ message: "Unsupported push topic" });
  }

  try {
    if (isFirebaseAdminConfigured()) {
      await unsubscribeTokenFromTopic(token, topic).catch((err) => {
        console.warn("FCM unsubscribe warning:", err?.message || err);
      });
    }

    const result = await db.query(
      `
      INSERT INTO push_subscriptions (
        token,
        topic,
        platform,
        status,
        last_error,
        updated_at
      ) VALUES ($1,$2,'web','inactive',NULL,now())
      ON CONFLICT (token, topic)
      DO UPDATE SET
        status = 'inactive',
        last_error = NULL,
        updated_at = now()
      RETURNING id, topic, status, updated_at
    `,
      [token, topic],
    );

    return res.json({
      message: "News alerts disabled",
      subscription: result.rows[0] || null,
    });
  } catch (err) {
    console.error("POST /api/public/push/fcm/unregister error:", err);
    return res.status(500).json({
      message: "Unable to disable news alerts right now",
    });
  }
});

app.get("/api/public/blogs", async (req, res) => {
  try {
    const limit = Math.min(50, toPositiveInt(req.query.limit, 12));
    const rawCategory = String(req.query.category || "")
      .trim()
      .toLowerCase();
    const category = BLOG_ALLOWED_CATEGORIES.has(rawCategory)
      ? rawCategory
      : null;
    const productId = toPositiveInt(req.query.productId, null);
    const rawProductType = String(
      req.query.productType || req.query.product_type || "",
    )
      .trim()
      .toLowerCase();
    const productType = BLOG_ALLOWED_PRODUCT_TYPES.has(rawProductType)
      ? rawProductType
      : null;
    const params = [limit];
    const whereClauses = [`bl.status = 'published'`];

    if (category) {
      params.push(category);
      whereClauses.push(`bl.category = $${params.length}`);
    }

    if (productId) {
      params.push(productId);
      whereClauses.push(`
        EXISTS (
          SELECT 1
          FROM blog_products bp
          WHERE bp.blog_id = bl.id
            AND bp.product_id = $${params.length}
        )
      `);
    }

    if (productType) {
      params.push(productType);
      whereClauses.push(`
        EXISTS (
          SELECT 1
          FROM blog_products bp
          INNER JOIN products related_product
            ON related_product.id = bp.product_id
          WHERE bp.blog_id = bl.id
            AND related_product.product_type = $${params.length}
        )
      `);
    }

    const result = await db.query(
      `
      SELECT
        bl.id,
        bl.product_id,
        bl.slug,
        bl.category,
        bl.title,
        bl.excerpt,
        bl.author_name,
        bl.author_user_id,
        bl.content_template,
        bl.content_rendered,
        bl.token_snapshot,
        bl.meta_title,
        bl.meta_description,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        CASE
          WHEN bl.hero_image_source = 'none' THEN NULL
          ELSE COALESCE(
            bl.hero_image,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = bl.product_id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            )
          )
        END AS hero_image,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        b.logo AS brand_logo
      FROM blogs bl
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE ${whereClauses.join("\n        AND ")}
      ORDER BY bl.published_at DESC NULLS LAST, bl.updated_at DESC
      LIMIT $1
    `,
      params,
    );

    const rows = result.rows || [];
    await attachBlogProductsToRows(rows);
    const needsResolution = rows.some((row) => Number(row.product_id) > 0);
    const blogs = needsResolution
      ? await (async () => {
          const profileConfig = await readDeviceFieldProfilesConfig().catch(
            () => ({ profiles: [] }),
          );
          const snapshotCache = new Map();
          return Promise.all(
            rows.map((row) =>
              resolvePublicBlogRow(row, profileConfig, snapshotCache),
            ),
          );
        })()
      : rows.map((blog) => ({
          ...blog,
          content_template: ensureProperHtmlEncoding(blog.content_template),
          content_rendered: ensureProperHtmlEncoding(blog.content_rendered),
        }));

    return res.json({
      limit,
      category,
      productId,
      productType,
      blogs,
    });
  } catch (err) {
    console.error("GET /api/public/blogs error:", err);
    return res.status(500).json({ message: "Failed to fetch blogs" });
  }
});

app.get("/api/public/blogs/:slug", async (req, res) => {
  try {
    const slug = String(req.params.slug || "")
      .trim()
      .toLowerCase();
    if (!slug) return res.status(400).json({ message: "Invalid slug" });

    const result = await db.query(
      `
      SELECT
        bl.id,
        bl.product_id,
        bl.category,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.author_name,
        bl.author_user_id,
        bl.content_template,
        bl.content_rendered,
        bl.token_snapshot,
        bl.meta_title,
        bl.meta_description,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        CASE
          WHEN bl.hero_image_source = 'none' THEN NULL
          ELSE COALESCE(
            bl.hero_image,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = bl.product_id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            )
          )
        END AS hero_image,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name
      FROM blogs bl
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE bl.slug = $1
        AND bl.status = 'published'
      LIMIT $2
    `,
      [slug, 1],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Blog not found" });
    }

    await attachBlogProductsToRows(result.rows);
    const blog = await resolvePublicBlogRow(result.rows[0]);
    return res.json({ blog });
  } catch (err) {
    console.error("GET /api/public/blogs/:slug error:", err);
    return res.status(500).json({ message: "Failed to fetch blog" });
  }
});

/*--- ADMIN Customer Management ---*/
app.get("/api/admin/customers", authenticate, async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, f_name, l_name, username, email, phone, city, state, country, zip_code, created_at 
       FROM Customers ORDER BY created_at DESC`,
    );

    res.json({
      success: true,
      message: "Customers retrieved successfully",
      customers: result.rows,
    });
  } catch (err) {
    console.error("Get customers error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;

    const result = await db.query(`SELECT * FROM Customers WHERE id = $1`, [
      customerId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      customer: result.rows[0],
    });
  } catch (err) {
    console.error("Get customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;
    const { f_name, l_name, email, phone, city, state, country, zip_code } =
      req.body;

    // Validate required fields
    if (!f_name || !l_name || !email) {
      return res.status(400).json({
        message: "First name, last name, and email are required",
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      "SELECT id FROM Customers WHERE email = $1 AND id != $2",
      [email, customerId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update customer
    const result = await db.query(
      `UPDATE Customers 
       SET f_name = $1, l_name = $2, email = $3, phone = $4, city = $5, state = $6, country = $7, zip_code = $8
       WHERE id = $9
       RETURNING id, f_name, l_name, username, email, phone, city, state, country, zip_code`,
      [
        f_name,
        l_name,
        email,
        phone || null,
        city || null,
        state || null,
        country || null,
        zip_code || null,
        customerId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      message: "Customer updated successfully",
      customer: result.rows[0],
    });
  } catch (err) {
    console.error("Update customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;

    const result = await db.query(
      "DELETE FROM Customers WHERE id = $1 RETURNING id",
      [customerId],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      message: "Customer deleted successfully",
    });
  } catch (err) {
    console.error("Delete customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

/*--- ratings smartphones  ---*/
app.post(
  "/api/public/products/:productId/ratings",
  authenticateCustomer,
  async (req, res) => {
    try {
      const productId = Number(req.params.productId);
      const userId = req.customer.id;
      const { overall, review } = req.body;

      if (!productId) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      if (typeof overall !== "number" || overall < 1 || overall > 5) {
        return res
          .status(400)
          .json({ message: "Rating must be between 1 and 5" });
      }

      await db.query(
        `
        INSERT INTO product_ratings (product_id, user_id, overall_rating, review)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (product_id, user_id)
        DO UPDATE SET
          overall_rating = EXCLUDED.overall_rating,
          review = EXCLUDED.review,
          created_at = CURRENT_TIMESTAMP
        `,
        [productId, userId, Math.round(overall), review || null],
      );

      res.status(201).json({
        message: "Rating submitted successfully",
      });
    } catch (err) {
      console.error("POST rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  },
);

app.get("/api/brand", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        b.id,
        b.name,
        b.logo,
        MAX(to_jsonb(b)->>'website') AS website,
        b.description,
        b.category,
        b.status,
        b.created_at,
        COUNT(DISTINCT p.id)::int AS product_count,
        COUNT(DISTINCT p.id) FILTER (WHERE pp.is_published = true)::int AS published_products
      FROM brands b
      LEFT JOIN products p
        ON p.brand_id = b.id
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
      GROUP BY
        b.id,
        b.name,
        b.logo,
        b.description,
        b.category,
        b.status,
        b.created_at
      ORDER BY b.name ASC
    `);

    res.json({ brands: result.rows });
  } catch (err) {
    console.error("GET /api/brand error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/public/products/:productId/ratings", async (req, res) => {
  try {
    const productId = Number(req.params.productId);
    if (!productId) {
      return res.status(400).json({ message: "Invalid product id" });
    }

    // Optional customer authentication: if token provided and valid, identify customer id
    let requestingCustomerId = null;
    try {
      const authHeader = req.headers.authorization;
      const token = authHeader && authHeader.split(" ")[1];
      if (token) {
        const decoded = jwt.verify(token, SECRET);
        if (decoded && decoded.type === "customer") {
          requestingCustomerId = decoded.id;
        }
      }
    } catch (e) {
      // ignore token errors for public endpoint
      requestingCustomerId = null;
    }

    // Aggregate average and total
    const agg = await db.query(
      `SELECT ROUND(AVG(overall_rating)::numeric, 1) AS average_rating, COUNT(*) AS total_ratings
       FROM product_ratings WHERE product_id = $1`,
      [productId],
    );

    // Fetch individual reviews with reviewer info
    const reviewsRes = await db.query(
      `SELECT pr.id, pr.user_id, pr.overall_rating, pr.review, pr.created_at,
              c.f_name, c.l_name, c.username
       FROM product_ratings pr
       LEFT JOIN customers c ON pr.user_id = c.id
       WHERE pr.product_id = $1
       ORDER BY pr.created_at DESC`,
      [productId],
    );

    const reviews = reviewsRes.rows.map((r) => ({
      id: r.id,
      user_id: r.user_id,
      name: r.f_name || r.username || null,
      username: r.username || null,
      overall_rating: r.overall_rating,
      review: r.review,
      created_at: r.created_at,
      is_user_review: requestingCustomerId
        ? Number(r.user_id) === Number(requestingCustomerId)
        : false,
    }));

    res.json({
      productId,
      averageRating: agg.rows[0].average_rating || 0,
      totalRatings: Number(agg.rows[0].total_ratings || 0),
      reviews,
    });
  } catch (err) {
    console.error("GET ratings error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put(
  "/api/private/smartphone/:smartphoneId/rating",
  authenticate,

  async (req, res) => {
    console.log(req.body);
    console.log(req.params.smartphoneId);
    try {
      const smartphoneId = Number(req.params.smartphoneId);
      if (!smartphoneId) {
        return res.status(400).json({ message: "Invalid smartphone id" });
      }

      const { display, performance, camera, battery, design } = req.body;

      // Validation
      const ratings = [display, performance, camera, battery, design];
      if (ratings.some((r) => typeof r !== "number" || r < 0 || r > 5)) {
        return res.status(400).json({
          message: "All rating values must be numbers between 0 and 5",
        });
      }

      // Calculate overall rating
      const overallRating =
        (display + performance + camera + battery + design) / 5;

      // Resolve product_id
      const sres = await db.query(
        "SELECT product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [smartphoneId],
      );
      n;
      if (!sres.rows.length) {
        return res.status(404).json({ message: "Smartphone not found" });
      }
      const productId = sres.rows[0].product_id;

      // For admin/private update, accept overall rating and review
      const overall =
        Math.round(
          ((display + performance + camera + battery + design) / 5) * 10,
        ) / 10;

      const result = await db.query(
        `
        UPDATE product_ratings
        SET overall_rating = $1, review = $2, created_at = CURRENT_TIMESTAMP
        WHERE id = (
          SELECT id FROM product_ratings WHERE product_id = $3 ORDER BY created_at DESC LIMIT 1
        )
        RETURNING *;
        `,
        [overall, req.body.review || null, productId],
      );

      if (result.rowCount === 0) {
        return res
          .status(404)
          .json({ message: "No rating found to update for this product" });
      }

      res.json({
        message: "Rating updated successfully",
        data: result.rows[0],
      });
    } catch (err) {
      console.error("PUT rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  },
);

app.delete(
  "/api/private/smartphone/:smartphoneId/rating",
  authenticate,
  async (req, res) => {
    try {
      const smartphoneId = Number(req.params.smartphoneId);
      if (!smartphoneId)
        return res.status(400).json({ message: "Invalid smartphone id" });

      const sres = await db.query(
        "SELECT product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [smartphoneId],
      );
      if (!sres.rows.length)
        return res.status(404).json({ message: "Smartphone not found" });

      const productId = sres.rows[0].product_id;
      await db.query(`DELETE FROM product_ratings WHERE product_id = $1`, [
        productId,
      ]);

      res.json({ message: "All ratings deleted" });
    } catch (err) {
      console.error("DELETE ratings error:", err);
      res.status(500).json({ error: err.message });
    }
  },
);

// Delete a single review (only by the owning customer)
app.delete("/api/reviews/:id", authenticateCustomer, async (req, res) => {
  try {
    const reviewId = Number(req.params.id);
    if (!reviewId)
      return res.status(400).json({ message: "Invalid review id" });

    const result = await db.query(
      "SELECT user_id, product_id FROM product_ratings WHERE id = $1",
      [reviewId],
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Review not found" });

    const review = result.rows[0];
    if (Number(review.user_id) !== Number(req.customer.id)) {
      return res
        .status(403)
        .json({ message: "Not allowed to delete this review" });
    }

    await db.query("DELETE FROM product_ratings WHERE id = $1", [reviewId]);
    res.json({ message: "Review deleted successfully" });
  } catch (err) {
    console.error("DELETE review error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Smartphones CRUD (Option B input format) - tables with   suffix
------------------------*/

// Create smartphone (with variants + variant_store_prices )
app.post("/api/smartphones", authenticate, async (req, res) => {
  const client = await db.connect();

  try {
    const { product, smartphone, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* ---------- 1. INSERT PRODUCT ---------- */
    const productRes = await client.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1, 'smartphone', $2)
      RETURNING id
      `,
      [product.name, product.brand_id],
    );

    const productId = productRes.rows[0].id;
    const launchStatusOverride = normalizeLaunchStatusOverride(
      smartphone?.launch_status_override ||
        smartphone?.launchStatusOverride ||
        req.body.launch_status_override ||
        req.body.launchStatusOverride,
    );

    /* ---------- 2. INSERT SMARTPHONE ---------- */
    const smartphoneRes = await client.query(
      `
      INSERT INTO smartphones (
        product_id, category, brand, model, launch_date,
        official_preorder_url,
        launch_status_override,
        images, colors, build_design, display, performance,
        camera, battery, connectivity, network,
        ports, audio, multimedia, sensors
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,
        $8,$9,$10,$11,$12,
        $13,$14,$15,$16,$17,$18,$19,$20
      )
      RETURNING id
      `,
      [
        productId,
        smartphone.category || smartphone.segment || null,
        smartphone.brand || null,
        smartphone.model || null,
        smartphone.launch_date || null,
        smartphone.official_preorder_url ||
          smartphone.officialPreorderUrl ||
          req.body.official_preorder_url ||
          req.body.officialPreorderUrl ||
          null,
        launchStatusOverride,
        JSON.stringify(images || []),
        JSON.stringify(smartphone.colors || []),
        JSON.stringify(smartphone.build_design || {}),
        JSON.stringify(smartphone.display || {}),
        JSON.stringify(smartphone.performance || {}),
        JSON.stringify(smartphone.camera || {}),
        JSON.stringify(smartphone.battery || {}),
        JSON.stringify(smartphone.connectivity || {}),
        JSON.stringify(smartphone.network || {}),
        JSON.stringify(smartphone.ports || {}),
        JSON.stringify(smartphone.audio || {}),
        JSON.stringify(smartphone.multimedia || {}),
        // sensors param (if present)
        smartphone.sensors === null
          ? null
          : JSON.stringify(smartphone.sensors || []),
      ],
    );

    const smartphoneId = smartphoneRes.rows[0].id;

    // ---------- 2.b UPSERT SPHERE RATINGS (per-section JSON objects) ----------
    try {
      const upsertRes = await client.query(
        `INSERT INTO product_sphere_ratings
          (product_id, design, display, performance, camera, battery, connectivity, network)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (product_id) DO UPDATE SET
           design = EXCLUDED.design,
           display = EXCLUDED.display,
           performance = EXCLUDED.performance,
           camera = EXCLUDED.camera,
           battery = EXCLUDED.battery,
           connectivity = EXCLUDED.connectivity,
           network = EXCLUDED.network,
           updated_at = CURRENT_TIMESTAMP
        `,
        [
          productId,
          JSON.stringify(smartphone.build_design?.sphere_rating || null),
          JSON.stringify(smartphone.display?.sphere_rating || null),
          JSON.stringify(smartphone.performance?.sphere_rating || null),
          JSON.stringify(smartphone.camera?.sphere_rating || null),
          JSON.stringify(smartphone.battery?.sphere_rating || null),
          JSON.stringify(smartphone.connectivity?.sphere_rating || null),
          JSON.stringify(smartphone.network?.sphere_rating || null),
        ],
      );
    } catch (esr) {
      console.error(
        "Sphere ratings upsert error (create):",
        esr.message || esr,
      );
    }

    /* ---------- 3. INSERT PRODUCT IMAGES ---------- */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `
        INSERT INTO product_images (product_id, image_url, position)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        `,
        [productId, url, i + 1],
      );
    }

    /* ---------- 4. INSERT VARIANTS + STORE PRICES (into product_variants) ---------- */
    for (const v of variants) {
      // Build a stable variant_key. Prefer explicit `variant_key` from request,
      // otherwise fallback to ram_storage combination.
      const variantKey =
        (v.variant_key && String(v.variant_key).trim()) ||
        `${String(v.ram || "na")}_${String(v.storage || "na")}`;

      // Build attributes JSON merging any provided `attributes` object
      // with well-known fields like ram/storage.
      const attrsObj = Object.assign({}, v.attributes || {});
      if (v.ram !== undefined) attrsObj.ram = v.ram;
      if (v.storage !== undefined) attrsObj.storage = v.storage;

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (product_id, variant_key)
        DO UPDATE SET base_price = EXCLUDED.base_price, attributes = EXCLUDED.attributes
        RETURNING id
        `,
        [productId, variantKey, JSON.stringify(attrsObj), v.base_price || null],
      );

      const variantId = variantRes.rows[0].id;

      for (const sp of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text, sale_start_date)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (variant_id, store_name)
          DO UPDATE SET
            price = EXCLUDED.price,
            url = EXCLUDED.url,
            offer_text = EXCLUDED.offer_text,
            sale_start_date = EXCLUDED.sale_start_date
          `,
          [
            variantId,
            sp.store_name,
            sp.price || null,
            sp.url || null,
            sp.offer_text || null,
            normalizeDateOnlyInput(
              sp.sale_start_date ?? sp.sale_date ?? sp.saleStartDate ?? null,
            ),
          ],
        );
      }
    }

    await client.query("COMMIT");

    res.status(201).json({
      message: "Smartphone created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

//posting product to table products
app.post("/api/products", authenticate, async (req, res) => {
  try {
    const { name, product_type, brand_id } = req.body;

    if (!name || !product_type || !brand_id) {
      return res.status(400).json({
        message: "name, product_type and brand_id are required",
      });
    }

    const allowedTypes = [
      "smartphone",
      "laptop",
      "networking",
      "tv",
      "accessories",
    ];

    if (!allowedTypes.includes(product_type)) {
      return res.status(400).json({ message: "Invalid product_type" });
    }

    const r = await db.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1,$2,$3)
      RETURNING *
      `,
      [name, product_type, brand_id],
    );

    res.status(201).json({
      message: "Product created",
      data: r.rows[0],
    });
  } catch (err) {
    console.error("POST /api/products error:", err);

    if (err.code === "23503") {
      return res.status(400).json({ message: "Invalid brand_id" });
    }

    res.status(500).json({ error: err.message });
  }
});

// Get published smartphones (public) - returns flattened variants as separate rows
// Get published smartphones (public) - nested structure
app.get("/api/smartphones", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,
        b.logo AS brand_logo,
        MAX(to_jsonb(b)->>'website') AS brand_website,

        s.category,
        s.model,
        s.launch_date,
        s.official_preorder_url,
        s.launch_status_override,
        s.colors,
        s.build_design,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity,
        s.network,
        s.ports,
        s.audio,
        s.multimedia,
        s.sensors,
        s.created_at,

        /* ---------- Hook Dynamic Score ---------- */
        MAX(ds.hook_score) AS hook_score,
        MAX(ds.buyer_intent) AS buyer_intent,
        MAX(ds.trend_velocity) AS trend_velocity,
        MAX(ds.freshness) AS freshness,
        MAX(ds.calculated_at) AS hook_calculated_at,
 
        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'ram', v.attributes->>'ram',
              'storage', v.attributes->>'storage',
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info,
                      'sale_start_date', sp.sale_start_date
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN smartphones s
        ON s.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
 
      WHERE p.product_type = 'smartphone'
 
      GROUP BY
        p.id, p.name, p.product_type, p.brand_id, b.name, b.logo, b.id,
        s.category, s.model, s.launch_date, s.official_preorder_url, s.launch_status_override,
        s.colors, s.build_design, s.display, s.performance,
        s.camera, s.battery, s.connectivity, s.network,
        s.ports, s.audio, s.multimedia, s.sensors, s.created_at

      ORDER BY COALESCE(MAX(ds.hook_score), 0) DESC, p.id DESC;
    `);

    const normalizeHookNumber = (value, fallback = null) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : fallback;
    };
    const toMillis = (value) => {
      if (!value) return 0;
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? 0 : date.getTime();
    };
    const computeFallbackHookScore = (row) => {
      const buyerIntent = normalizeHookNumber(row?.buyer_intent, 0);
      const trendVelocity = normalizeHookNumber(row?.trend_velocity, 0);
      const freshness = normalizeHookNumber(row?.freshness, 0);
      return Number(
        (buyerIntent * 0.6 + trendVelocity * 0.25 + freshness * 0.15).toFixed(
          2,
        ),
      );
    };

    const todayIndia = getIndiaDateOnly();
    const smartphones = applySpecScoreToRows(
      "smartphone",
      (result.rows || []).map((row) => {
        const item = { ...(row || {}) };
        const hookScore = normalizeHookNumber(item.hook_score, null);
        const buyerIntent = normalizeHookNumber(item.buyer_intent, 0);
        const trendVelocity = normalizeHookNumber(item.trend_velocity, 0);
        const freshness = normalizeHookNumber(item.freshness, 0);
        const hookRankScore =
          hookScore !== null ? hookScore : computeFallbackHookScore(item);

        const variants = (
          Array.isArray(item.variants) ? item.variants : []
        ).map((variant) => ({
          ...toPlainObject(variant),
          store_prices: decorateStorePriceList(
            toPlainObject(variant).store_prices || [],
          ),
        }));
        item.variants = variants;
        item.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
        item.price = resolveEffectiveSmartphonePrice(
          variants,
          item.price ?? null,
        );
        const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
        item.launch_status = launchStage;
        item.launchStatus = launchStage;
        applySmartphoneLaunchPolicy(item, launchStage);
        item.hook_score = hookScore;
        item.hookScore = hookScore;
        item.Hookss_score = hookScore;
        item.HookssScore = hookScore;
        item.buyer_intent = buyerIntent;
        item.buyerIntent = buyerIntent;
        item.trend_velocity = trendVelocity;
        item.trendVelocity = trendVelocity;
        item.freshness = freshness;
        item.hook_rank_score = hookRankScore;
        item.hook_calculated_at = item.hook_calculated_at ?? null;
        item.Hookss_calculated_at = item.hook_calculated_at;
        return stripScoreRecursively(item);
      }),
      profileConfig.profiles,
    );

    const sortedSmartphones = [...smartphones].sort((a, b) => {
      const hookDelta =
        normalizeHookNumber(b?.hook_rank_score, 0) -
        normalizeHookNumber(a?.hook_rank_score, 0);
      if (hookDelta !== 0) return hookDelta;

      const buyerDelta =
        normalizeHookNumber(b?.buyer_intent, 0) -
        normalizeHookNumber(a?.buyer_intent, 0);
      if (buyerDelta !== 0) return buyerDelta;

      const velocityDelta =
        normalizeHookNumber(b?.trend_velocity, 0) -
        normalizeHookNumber(a?.trend_velocity, 0);
      if (velocityDelta !== 0) return velocityDelta;

      const freshnessDelta =
        normalizeHookNumber(b?.freshness, 0) -
        normalizeHookNumber(a?.freshness, 0);
      if (freshnessDelta !== 0) return freshnessDelta;

      const launchDateDelta =
        toMillis(b?.launch_date) - toMillis(a?.launch_date);
      if (launchDateDelta !== 0) return launchDateDelta;

      return (
        normalizeHookNumber(b?.product_id, 0) -
        normalizeHookNumber(a?.product_id, 0)
      );
    });

    res.json({ smartphones: sortedSmartphones });
  } catch (err) {
    console.error("GET /api/smartphones error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get all smartphones (authenticated) — full data
app.get("/api/smartphone", authenticate, async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,
        MAX(ds.hook_score) AS hook_score,
        MAX(ds.buyer_intent) AS buyer_intent,
        MAX(ds.trend_velocity) AS trend_velocity,
        MAX(ds.freshness) AS freshness,
        MAX(ds.calculated_at) AS hook_calculated_at,

        s.category,
        s.model,
        s.launch_date,
        s.official_preorder_url,
        s.launch_status_override,
        s.colors,
        s.build_design,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity,
        s.network,
        s.ports,
        s.audio,
        s.multimedia,
        s.sensors,
        s.created_at,

        COALESCE(pub.is_published, false) AS is_published,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'attributes', v.attributes,
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info,
                      'sale_start_date', sp.sale_start_date
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN smartphones s
        ON s.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id

      WHERE p.product_type = 'smartphone'

      GROUP BY
        p.id, p.name, p.product_type, p.brand_id, b.name, b.id,
        s.category, s.model, s.launch_date, s.official_preorder_url, s.launch_status_override,
        s.colors, s.build_design, s.display, s.performance,
        s.camera, s.battery, s.connectivity, s.network,
        s.ports, s.audio, s.multimedia, s.sensors, s.created_at, pub.is_published

      ORDER BY p.id DESC;
    `);

    const todayIndia = getIndiaDateOnly();
    const smartphones = applySpecScoreToRows(
      "smartphone",
      (result.rows || []).map((row) => {
        const item = { ...(row || {}) };
        const variants = (
          Array.isArray(item.variants) ? item.variants : []
        ).map((variant) => ({
          ...toPlainObject(variant),
          store_prices: decorateStorePriceList(
            toPlainObject(variant).store_prices || [],
          ),
        }));
        item.variants = variants;
        item.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
        item.price = resolveEffectiveSmartphonePrice(
          variants,
          item.price ?? null,
        );
        const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
        item.launch_status = launchStage;
        item.launchStatus = launchStage;
        applySmartphoneLaunchPolicy(item, launchStage);
        return stripScoreRecursively(item);
      }),
      profileConfig.profiles,
    );

    res.json({ smartphones });
  } catch (err) {
    console.error("GET /api/smartphones error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get smartphone by id with variants and store prices
// Accept either internal `smartphones.id` or `product_id` (product's id).
app.get("/api/smartphone/:id", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const rawId = req.params.id;
    const sid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    let smartphone = null;
    if (!Number.isNaN(sid)) {
      // Prefer product_id match first to avoid collisions with smartphones.id
      const byProduct = await db.query(
        "SELECT * FROM smartphones WHERE product_id = $1 LIMIT 1",
        [sid],
      );
      if (byProduct.rows.length) {
        smartphone = byProduct.rows[0];
      } else {
        const byId = await db.query(
          "SELECT * FROM smartphones WHERE id = $1 LIMIT 1",
          [sid],
        );
        if (byId.rows.length) smartphone = byId.rows[0];
      }
    } else {
      const sres2 = await db.query(
        "SELECT * FROM smartphones WHERE model = $1 OR brand = $1 LIMIT 1",
        [rawId],
      );
      if (sres2.rows.length) smartphone = sres2.rows[0];
    }

    if (!smartphone) {
      return res.status(404).json({ message: "Not found" });
    }

    const productId = smartphone.product_id;
    // Fetch product name from products table and include in response
    const prodRes = await db.query(
      `SELECT
        p.name,
        p.brand_id,
        b.logo AS brand_logo,
        (to_jsonb(b)->>'website') AS brand_website
      FROM products p
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE p.id = $1
      LIMIT 1`,
      [productId],
    );
    const productName = prodRes.rows[0] ? prodRes.rows[0].name : null;
    const productBrandId = prodRes.rows[0] ? prodRes.rows[0].brand_id : null;
    const productBrandLogo = prodRes.rows[0]
      ? prodRes.rows[0].brand_logo
      : null;
    const productBrandWebsite = prodRes.rows[0]
      ? prodRes.rows[0].brand_website
      : null;
    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variants = [];
    const todayIndia = getIndiaDateOnly();
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
        [v.id],
      );
      const ram = v.attributes ? v.attributes.ram || null : null;
      const storage = v.attributes ? v.attributes.storage || null : null;
      variants.push({
        ...v,
        ram,
        storage,
        store_prices: decorateStorePriceList(stores.rows, todayIndia),
      });
    }

    // Sanitize response (remove internal ids)
    const sanitize = (smartphoneObj, variantsArr) => {
      const { id, product_id, ...rest } = smartphoneObj || {};
      // sanitize colors
      let colors = rest.colors;
      if (Array.isArray(colors)) {
        colors = colors.map((c) => {
          if (c && typeof c === "object") {
            const { id: _cid, ...rc } = c;
            return rc;
          }
          return c;
        });
      }

      const sanitizedVariants = (variantsArr || []).map((v) => {
        const { id: _vid, product_id: _vpid, store_prices, ...rv } = v || {};
        const sanitizedStorePrices = Array.isArray(store_prices)
          ? store_prices.map((sp) => {
              const { id: _spid, variant_id: _vref, ...rsp } = sp || {};
              return rsp;
            })
          : store_prices;
        return { ...rv, store_prices: sanitizedStorePrices };
      });

      return stripScoreRecursively({
        ...rest,
        colors,
        variants: sanitizedVariants,
      });
    };

    const sanitized = sanitize(smartphone, variants);
    sanitized.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
    sanitized.name = productName;
    sanitized.brand_logo = productBrandLogo || null;
    sanitized.brand_website = productBrandWebsite || null;
    sanitized.launch_date = smartphone.launch_date || null;
    sanitized.created_at = smartphone.created_at || null;
    const launchStage = resolveSmartphoneLaunchStage(
      { ...sanitized, variants },
      todayIndia,
    );
    sanitized.launch_status = launchStage;
    sanitized.launchStatus = launchStage;
    applySmartphoneLaunchPolicy(sanitized, launchStage);

    const scored = applySpecScoreToRow(
      "smartphone",
      sanitized,
      profileConfig.profiles,
    );

    return res.json({ data: scored });
  } catch (err) {
    console.error("GET /api/smartphone/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.post("/api/laptops", authenticate, async (req, res) => {
  const client = await db.connect();

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const {
      product = {},
      laptop = {},
      images = [],
      variants = [],
    } = req.body || {};
    const requestBody = toPlainObject(req.body);
    const productInput = toPlainObject(product);
    const laptopRaw = toPlainObject(laptop);
    const laptopInput =
      Object.keys(laptopRaw).length > 0 ? laptopRaw : requestBody;
    const laptopMetadataInput = toPlainObject(laptopInput.metadata);
    const normalizedLaptop = normalizeLaptopPayload(laptopInput);

    const basicInfoForName = mergeSectionObject(
      toPlainObject(laptopInput.basic_info_json),
      toPlainObject(laptopInput.basic_info),
    );
    const productNameCandidates = [
      productInput.name,
      requestBody.name,
      requestBody.product_name,
      basicInfoForName.product_name,
      basicInfoForName.title,
      basicInfoForName.model_name,
      laptopInput.model,
      laptopInput.model_name,
      laptopInput.title,
    ];
    const productName =
      productNameCandidates.map(normalizeNullableText).find(Boolean) || null;

    if (!productName) {
      return res.status(400).json({
        message:
          "Missing required field: product.name (or basic_info.title / basic_info_json.title / model_name).",
      });
    }

    const brandIdRaw = productInput.brand_id ?? requestBody.brand_id ?? null;
    let brandId = null;
    if (brandIdRaw !== null && brandIdRaw !== undefined && brandIdRaw !== "") {
      const parsedBrandId = Number(brandIdRaw);
      if (!Number.isFinite(parsedBrandId)) {
        return res
          .status(400)
          .json({ message: "product.brand_id must be a numeric value." });
      }
      brandId = parsedBrandId;
    }

    const imageList = Array.isArray(images)
      ? images
      : Array.isArray(requestBody.images)
        ? requestBody.images
        : Array.isArray(laptopMetadataInput.images)
          ? laptopMetadataInput.images
          : [];
    const variantList = Array.isArray(variants)
      ? variants
      : Array.isArray(requestBody.variants)
        ? requestBody.variants
        : Array.isArray(laptopMetadataInput.variants)
          ? laptopMetadataInput.variants
          : [];

    await client.query("BEGIN");

    /* 1️⃣ Product */
    const productRes = await client.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1, 'laptop', $2)
      RETURNING id
      `,
      [productName, brandId],
    );
    const productId = productRes.rows[0].id;

    /* 2️⃣ Laptop table (JSONB SAFE) */
    await client.query(
      `
      INSERT INTO laptop (
        product_id, cpu, display, memory, storage, battery,
        connectivity, physical, software, features, warranty, meta, spec_sections
      )
      VALUES (
        $1,
        $2::jsonb,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,
        $7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb
      )
      `,
      [
        productId,
        JSON.stringify(normalizedLaptop.legacy.cpu || {}),
        JSON.stringify(normalizedLaptop.legacy.display || {}),
        JSON.stringify(normalizedLaptop.legacy.memory || {}),
        JSON.stringify(normalizedLaptop.legacy.storage || {}),
        JSON.stringify(normalizedLaptop.legacy.battery || {}),
        JSON.stringify(normalizedLaptop.legacy.connectivity || {}),
        JSON.stringify(normalizedLaptop.legacy.physical || {}),
        JSON.stringify(normalizedLaptop.legacy.software || {}),
        JSON.stringify(normalizedLaptop.legacy.features || []),
        JSON.stringify(normalizedLaptop.legacy.warranty || {}),
        JSON.stringify(normalizedLaptop.meta || {}),
        JSON.stringify(normalizedLaptop.spec_sections || {}),
      ],
    );

    /* 3️⃣ Images */
    for (let i = 0; i < imageList.length; i++) {
      const url = imageList[i];
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position)
         VALUES ($1,$2,$3)`,
        [productId, url, i + 1],
      );
    }

    /* 4️⃣ Variants + Store Prices */
    for (const v of variantList) {
      const variantObj = toPlainObject(v);
      const variantAttributes = {
        ...toPlainObject(variantObj.attributes),
        ram:
          variantObj.ram ??
          toPlainObject(variantObj.attributes).ram ??
          toPlainObject(variantObj.attributes).RAM ??
          null,
        storage:
          variantObj.storage ??
          toPlainObject(variantObj.attributes).storage ??
          toPlainObject(variantObj.attributes).rom ??
          null,
      };

      const variantKey =
        normalizeNullableText(variantObj.variant_key) ||
        `${variantAttributes.ram || "na"}_${variantAttributes.storage || "na"}`;

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [
          productId,
          variantKey,
          JSON.stringify(variantAttributes),
          variantObj.base_price || variantObj.price || null,
        ],
      );

      const variantId = variantRes.rows[0].id;

      const stores = Array.isArray(variantObj.stores)
        ? variantObj.stores
        : Array.isArray(variantObj.store_prices)
          ? variantObj.store_prices
          : [];

      for (const s of stores) {
        const storeObj = toPlainObject(s);
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [
            variantId,
            storeObj.store_name || storeObj.store || "Store",
            storeObj.price || null,
            storeObj.url || null,
            storeObj.offer_text || storeObj.offerText || null,
          ],
        );
      }
    }

    /* 5️⃣ Publish default */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId],
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Laptop created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("POST /api/laptops error:", err);
    if (err && err.code === "23505") {
      return res.status(409).json({
        error:
          "Duplicate product. A laptop with this product name already exists.",
      });
    }
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/laptops", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.meta,
        l.spec_sections,
        l.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'ram', v.attributes->>'ram',
              'storage', v.attributes->>'storage',
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN laptop l
        ON l.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id

      WHERE p.product_type = 'laptop'

      GROUP BY
        p.id,
        b.name,
        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.meta,
        l.spec_sections,
        l.created_at

      ORDER BY COALESCE(MAX(ds.hook_score), 0) DESC, p.id DESC;
    `,
    );

    const laptops = applySpecScoreToRows(
      "laptop",
      (result.rows || []).map(toCanonicalLaptopProductResponse),
      profileConfig.profiles,
    ).map(toPublicLaptopCatalogResponseRow);
    res.json({ laptops });
  } catch (err) {
    console.error("GET /api/laptops error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get laptop by id (accepts internal laptop.id or product_id)
app.get("/api/laptops/:id", authenticate, async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const rawId = req.params.id;
    const lid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const lres = await db.query(
      "SELECT * FROM laptop WHERE product_id = $1 LIMIT 1",
      [lid],
    );

    if (!lres.rows.length) {
      return res.status(404).json({ message: "Not found" });
    }

    const laptop = lres.rows[0];
    const productId = laptop.product_id;

    // product name
    const prodRes = await db.query(
      "SELECT name, brand_id FROM products WHERE id = $1 LIMIT 1",
      [productId],
    );
    const productName = prodRes.rows[0] ? prodRes.rows[0].name : null;
    const productBrandId = prodRes.rows[0] ? prodRes.rows[0].brand_id : null;

    // images
    const imagesRes = await db.query(
      "SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC",
      [productId],
    );
    const images = imagesRes.rows.map((r) => r.image_url);

    // variants + store prices
    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variants = [];
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY id ASC",
        [v.id],
      );
      const ram = v.attributes ? v.attributes.ram || null : null;
      const storage = v.attributes ? v.attributes.storage || null : null;
      variants.push({ ...v, ram, storage, stores: stores.rows });
    }

    // publish info
    const pubRes = await db.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    const published = pubRes.rows[0] ? pubRes.rows[0].is_published : false;

    // Prepare sanitized response
    const sanitize = (lobj, variantsArr) => {
      const { id, product_id, meta, spec_sections, ...rest } = lobj || {};
      const metaObj = toPlainObject(meta);
      const sectionsObj = toPlainObject(spec_sections);
      const base = removeSectionKeyCollisions(rest, sectionsObj);
      return stripScoreRecursively({
        ...base,
        ...metaObj,
        spec_sections: sectionsObj,
        variants: variantsArr || [],
      });
    };

    const sanitized = sanitize(laptop, variants);
    sanitized.name = productName;
    const scoredLaptop = applySpecScoreToRow(
      "laptop",
      sanitized,
      profileConfig.profiles,
    );

    return res.json({
      product: { name: productName, brand_id: productBrandId },
      laptop: scoredLaptop,
      images,
      variants,
      published,
    });
  } catch (err) {
    console.error("GET /api/laptops/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update laptop (product, laptop jsonb, images, variants, publish)
app.put("/api/laptops/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    if (req.user.role !== "admin")
      return res.status(403).json({ message: "Admin access required" });

    const rawId = req.params.id;
    const lid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const lres = await db.query(
      "SELECT * FROM laptop WHERE product_id = $1 LIMIT 1",
      [lid],
    );
    if (!lres.rows.length)
      return res.status(404).json({ message: "Not found" });

    const laptopRow = lres.rows[0];
    const productId = laptopRow.product_id;

    const {
      product = {},
      laptop = {},
      images = [],
      variants = [],
      published,
    } = req.body;
    const requestBody = toPlainObject(req.body);
    const productInput = toPlainObject(product);
    const laptopRaw = toPlainObject(laptop);
    const laptopInput =
      Object.keys(laptopRaw).length > 0 ? laptopRaw : requestBody;
    const laptopMetadataInput = toPlainObject(laptopInput.metadata);
    const normalizedLaptop = normalizeLaptopPayload(laptopInput, laptopRow);
    const imageList = Array.isArray(images)
      ? images
      : Array.isArray(requestBody.images)
        ? requestBody.images
        : Array.isArray(laptopMetadataInput.images)
          ? laptopMetadataInput.images
          : [];
    const variantList = Array.isArray(variants)
      ? variants
      : Array.isArray(requestBody.variants)
        ? requestBody.variants
        : Array.isArray(laptopMetadataInput.variants)
          ? laptopMetadataInput.variants
          : [];

    await client.query("BEGIN");

    // Update product
    if (productInput.name || productInput.brand_id !== undefined) {
      await client.query(
        "UPDATE products SET name = $1, brand_id = $2 WHERE id = $3",
        [productInput.name || null, productInput.brand_id || null, productId],
      );
    }

    // Update laptop JSONB fields and meta
    await client.query(
      `
      UPDATE laptop SET
        cpu = $1::jsonb,
        display = $2::jsonb,
        memory = $3::jsonb,
        storage = $4::jsonb,
        battery = $5::jsonb,
        connectivity = $6::jsonb,
        physical = $7::jsonb,
        software = $8::jsonb,
        features = $9::jsonb,
        warranty = $10::jsonb,
        meta = $11::jsonb,
        spec_sections = $12::jsonb
      WHERE product_id = $13
      `,
      [
        JSON.stringify(normalizedLaptop.legacy.cpu || {}),
        JSON.stringify(normalizedLaptop.legacy.display || {}),
        JSON.stringify(normalizedLaptop.legacy.memory || {}),
        JSON.stringify(normalizedLaptop.legacy.storage || {}),
        JSON.stringify(normalizedLaptop.legacy.battery || {}),
        JSON.stringify(normalizedLaptop.legacy.connectivity || {}),
        JSON.stringify(normalizedLaptop.legacy.physical || {}),
        JSON.stringify(normalizedLaptop.legacy.software || {}),
        JSON.stringify(normalizedLaptop.legacy.features || []),
        JSON.stringify(normalizedLaptop.legacy.warranty || {}),
        JSON.stringify(normalizedLaptop.meta || {}),
        JSON.stringify(normalizedLaptop.spec_sections || {}),
        productId,
      ],
    );

    // Replace images: delete existing and insert new
    await client.query("DELETE FROM product_images WHERE product_id = $1", [
      productId,
    ]);
    for (let i = 0; i < imageList.length; i++) {
      await client.query(
        "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
        [productId, imageList[i], i + 1],
      );
    }

    // Replace variants: delete existing variant store prices and variants, then insert new ones
    const oldVarRes = await client.query(
      "SELECT id FROM product_variants WHERE product_id = $1",
      [productId],
    );
    for (const v of oldVarRes.rows) {
      await client.query(
        "DELETE FROM variant_store_prices WHERE variant_id = $1",
        [v.id],
      );
    }
    await client.query("DELETE FROM product_variants WHERE product_id = $1", [
      productId,
    ]);

    for (const v of variantList || []) {
      const variantKey = `${v.ram || ""}_${v.storage || ""}`;
      const variantRes = await client.query(
        `INSERT INTO product_variants (product_id, variant_key, attributes, base_price) VALUES ($1,$2,$3::jsonb,$4) RETURNING id`,
        [
          productId,
          variantKey,
          JSON.stringify({ ram: v.ram, storage: v.storage }),
          v.base_price || null,
        ],
      );
      const variantId = variantRes.rows[0].id;
      for (const s of v.stores || []) {
        await client.query(
          `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info) VALUES ($1,$2,$3,$4,$5,$6)`,
          [
            variantId,
            s.store_name || null,
            s.price || null,
            s.url || null,
            s.offer_text || null,
            s.delivery_info || null,
          ],
        );
      }
    }

    // Update publish state
    if (published !== undefined) {
      const up = await client.query(
        "UPDATE product_publish SET is_published = $1 WHERE product_id = $2",
        [published, productId],
      );
      if (up.rowCount === 0) {
        await client.query(
          "INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)",
          [productId, published],
        );
      }
    }

    await client.query("COMMIT");

    return res.json({ message: "Laptop updated", product_id: productId });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/laptops/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

const TV_JSON_OBJECT_SECTIONS = [
  "key_specs_json",
  "basic_info_json",
  "display_json",
  "video_engine_json",
  "audio_json",
  "smart_tv_json",
  "gaming_json",
  "ports_json",
  "connectivity_json",
  "power_json",
  "physical_json",
  "product_details_json",
  "in_the_box_json",
  "warranty_json",
];

const toNumericPrice = (value) => {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const cleaned = String(value).replace(/[^0-9.]/g, "");
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeImageArray = (value) => {
  if (!Array.isArray(value)) return [];
  return value.map((img) => normalizeNullableText(img)).filter(Boolean);
};

const parseFirstNumeric = (value) => {
  if (value === undefined || value === null) return null;
  const matched = String(value).match(/(\d+(?:\.\d+)?)/);
  if (!matched) return null;
  const parsed = Number(matched[1]);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeTvStorePriceRows = (rows = []) => {
  if (!Array.isArray(rows)) return [];
  const byStore = new Map();
  for (const row of rows) {
    const store = toPlainObject(row);
    const storeName = normalizeNullableText(store.store_name || store.store);
    if (!storeName) continue;
    const candidate = {
      store_name: storeName,
      price: toNumericPrice(store.price),
      url: normalizeNullableText(store.url),
      offer_text: normalizeNullableText(store.offer_text || store.offer),
      delivery_info: normalizeNullableText(store.delivery_info),
    };
    const key = storeName.toLowerCase();
    const previous = byStore.get(key);
    if (!previous) {
      byStore.set(key, candidate);
      continue;
    }

    const previousPrice =
      typeof previous.price === "number" && Number.isFinite(previous.price)
        ? previous.price
        : null;
    const candidatePrice =
      typeof candidate.price === "number" && Number.isFinite(candidate.price)
        ? candidate.price
        : null;

    // Keep the best price for duplicate stores while preserving other metadata.
    const pickCandidate =
      (candidatePrice !== null && previousPrice === null) ||
      (candidatePrice !== null &&
        previousPrice !== null &&
        candidatePrice < previousPrice);
    if (pickCandidate) {
      byStore.set(key, {
        ...previous,
        ...candidate,
      });
    }
  }
  return Array.from(byStore.values()).sort((a, b) => {
    const priceA =
      typeof a.price === "number" && Number.isFinite(a.price) ? a.price : null;
    const priceB =
      typeof b.price === "number" && Number.isFinite(b.price) ? b.price : null;
    if (priceA !== null && priceB !== null && priceA !== priceB) {
      return priceA - priceB;
    }
    if (priceA !== null && priceB === null) return -1;
    if (priceA === null && priceB !== null) return 1;
    return String(a.store_name || "").localeCompare(String(b.store_name || ""));
  });
};

const normalizeTvVariantInput = (variantInput = {}, index = 0) => {
  const variant = toPlainObject(variantInput);
  const inferredSize = normalizeNullableText(
    variant.screen_size ||
      variant.size ||
      variant.display_size ||
      variant.variant_key,
  );
  const variantKey =
    normalizeNullableText(variant.variant_key || inferredSize) ||
    `tv_variant_${index + 1}`;
  const screenSize = inferredSize || variantKey;
  const screenSizeValue = parseFirstNumeric(screenSize);

  const storePriceRows = normalizeTvStorePriceRows(
    Array.isArray(variant.store_prices)
      ? variant.store_prices
      : Array.isArray(variant.stores)
        ? variant.stores
        : [],
  );

  const images = normalizeImageArray(
    Array.isArray(variant.images)
      ? variant.images
      : Array.isArray(variant.images_json)
        ? variant.images_json
        : Array.isArray(variant.variant_images)
          ? variant.variant_images
          : [],
  );

  const attributes = { ...variant };
  delete attributes.base_price;
  delete attributes.store_prices;
  delete attributes.stores;
  delete attributes.variant_id;
  delete attributes.images;
  delete attributes.images_json;
  delete attributes.variant_images;

  if (!attributes.screen_size && screenSize) {
    attributes.screen_size = screenSize;
  }

  return {
    variant_key: variantKey,
    screen_size: screenSize,
    screen_size_value: screenSizeValue,
    base_price: toNumericPrice(variant.base_price ?? variant.price),
    store_prices: storePriceRows,
    images,
    attributes,
  };
};

const normalizeTvVariantsInput = (variants = []) => {
  if (!Array.isArray(variants)) return [];
  return variants.map((variant, index) =>
    normalizeTvVariantInput(variant, index),
  );
};

const normalizeTvPayloadInput = (input = {}) => {
  const body = toPlainObject(input);
  const nestedTv = toPlainObject(body.tv);
  const merged = Object.keys(nestedTv).length ? { ...body, ...nestedTv } : body;
  const product = toPlainObject(merged.product);
  const legacyHomeAppliance = toPlainObject(merged.home_appliance);

  const normalized = { ...merged };
  if (!hasOwn(normalized, "product_name") && hasOwn(product, "name")) {
    normalized.product_name = product.name;
  }
  if (!hasOwn(normalized, "brand_id") && hasOwn(product, "brand_id")) {
    normalized.brand_id = product.brand_id;
  }
  if (!hasOwn(normalized, "publish") && hasOwn(normalized, "published")) {
    normalized.publish = normalized.published;
  }
  if (
    !Array.isArray(normalized.images_json) &&
    Array.isArray(normalized.images)
  ) {
    normalized.images_json = normalized.images;
  }
  if (
    !Array.isArray(normalized.variants_json) &&
    Array.isArray(normalized.variants)
  ) {
    normalized.variants_json = normalized.variants;
  }
  if (Array.isArray(normalized.variants_json)) {
    normalized.variants_json = normalizeTvVariantsInput(
      normalized.variants_json,
    );
  }

  // Backward compatibility: map legacy home_appliance payload into TV sections.
  if (Object.keys(legacyHomeAppliance).length) {
    if (!hasOwn(normalized, "category") && legacyHomeAppliance.appliance_type) {
      normalized.category = legacyHomeAppliance.appliance_type;
    }

    if (!hasOwn(normalized, "model") && legacyHomeAppliance.model_number) {
      normalized.model = legacyHomeAppliance.model_number;
    }

    if (!hasOwn(normalized, "basic_info_json")) {
      normalized.basic_info_json = {
        model_number: legacyHomeAppliance.model_number || null,
        launch_year: legacyHomeAppliance.release_year || null,
      };
    }

    if (
      !hasOwn(normalized, "product_details_json") &&
      (legacyHomeAppliance.country_of_origin ||
        legacyHomeAppliance.release_year)
    ) {
      normalized.product_details_json = {
        country_of_origin: legacyHomeAppliance.country_of_origin || null,
        launch_year: legacyHomeAppliance.release_year || null,
      };
    }

    if (
      !hasOwn(normalized, "display_json") &&
      legacyHomeAppliance.specifications &&
      typeof legacyHomeAppliance.specifications === "object"
    ) {
      normalized.display_json = legacyHomeAppliance.specifications;
    }

    if (
      !hasOwn(normalized, "video_engine_json") &&
      legacyHomeAppliance.performance &&
      typeof legacyHomeAppliance.performance === "object"
    ) {
      normalized.video_engine_json = legacyHomeAppliance.performance;
    }

    if (
      !hasOwn(normalized, "physical_json") &&
      legacyHomeAppliance.physical_details &&
      typeof legacyHomeAppliance.physical_details === "object"
    ) {
      normalized.physical_json = legacyHomeAppliance.physical_details;
    }

    if (
      !hasOwn(normalized, "warranty_json") &&
      legacyHomeAppliance.warranty &&
      typeof legacyHomeAppliance.warranty === "object"
    ) {
      normalized.warranty_json = legacyHomeAppliance.warranty;
    }

    if (
      !hasOwn(normalized, "smart_tv_json") &&
      Array.isArray(legacyHomeAppliance.features)
    ) {
      normalized.smart_tv_json = {
        smart_features: legacyHomeAppliance.features,
      };
    }
  }

  return normalized;
};

const resolveBrandIdByName = async (client, brandName) => {
  const normalizedName = normalizeNullableText(brandName);
  if (!normalizedName) return null;

  const brandRes = await client.query(
    `SELECT id
     FROM brands
     WHERE LOWER(name) = LOWER($1)
     LIMIT 1`,
    [normalizedName],
  );

  return brandRes.rows[0] ? brandRes.rows[0].id : null;
};

app.post("/api/tvs", authenticate, async (req, res) => {
  const client = await db.connect();
  const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const payload = normalizeTvPayloadInput(req.body || {});
    const productName = normalizeNullableText(
      payload.product_name ||
        payload.name ||
        toPlainObject(payload.basic_info_json).title ||
        payload.model,
    );

    if (!productName) {
      return res.status(400).json({ message: "product_name is required" });
    }

    const model = normalizeNullableText(
      payload.model ||
        toPlainObject(payload.basic_info_json).model_number ||
        toPlainObject(payload.basic_info_json).model,
    );
    const category = normalizeNullableText(payload.category);
    const publish = hasOwn(payload, "publish")
      ? Boolean(payload.publish)
      : false;

    let brandId =
      payload.brand_id !== undefined && payload.brand_id !== null
        ? Number(payload.brand_id)
        : null;
    if (!Number.isInteger(brandId) || brandId <= 0) {
      brandId = await resolveBrandIdByName(client, payload.brand_name);
    }

    const imagesJson = Array.isArray(payload.images_json)
      ? payload.images_json
      : [];
    const variantsJson = normalizeTvVariantsInput(
      Array.isArray(payload.variants_json) ? payload.variants_json : [],
    );
    const variantsJsonForRow = variantsJson.map((variant) => ({
      variant_key: variant.variant_key,
      screen_size: variant.screen_size,
      screen_size_value: variant.screen_size_value,
      base_price: variant.base_price,
      store_prices: variant.store_prices,
      images: variant.images,
      ...toPlainObject(variant.attributes),
    }));

    await client.query("BEGIN");

    const productRes = await client.query(
      `
      INSERT INTO products (name, brand_id, product_type)
      VALUES ($1,$2,'tv')
      RETURNING id
      `,
      [productName, brandId],
    );
    const productId = productRes.rows[0].id;

    const sectionValues = {};
    for (const key of TV_JSON_OBJECT_SECTIONS) {
      sectionValues[key] = toPlainObject(payload[key]);
    }

    await client.query(
      `
      INSERT INTO tvs (
        product_id,
        category,
        model,
        key_specs_json,
        basic_info_json,
        display_json,
        video_engine_json,
        audio_json,
        smart_tv_json,
        gaming_json,
        ports_json,
        connectivity_json,
        power_json,
        physical_json,
        product_details_json,
        in_the_box_json,
        warranty_json,
        images_json,
        variants_json
      )
      VALUES (
        $1,$2,$3,
        $4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,
        $11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb,
        $18::jsonb,$19::jsonb
      )
      `,
      [
        productId,
        category,
        model,
        toJSON(sectionValues.key_specs_json),
        toJSON(sectionValues.basic_info_json),
        toJSON(sectionValues.display_json),
        toJSON(sectionValues.video_engine_json),
        toJSON(sectionValues.audio_json),
        toJSON(sectionValues.smart_tv_json),
        toJSON(sectionValues.gaming_json),
        toJSON(sectionValues.ports_json),
        toJSON(sectionValues.connectivity_json),
        toJSON(sectionValues.power_json),
        toJSON(sectionValues.physical_json),
        toJSON(sectionValues.product_details_json),
        toJSON(sectionValues.in_the_box_json),
        toJSON(sectionValues.warranty_json),
        toJSON(imagesJson),
        toJSON(variantsJsonForRow),
      ],
    );

    for (let i = 0; i < imagesJson.length; i++) {
      const imageUrl = normalizeNullableText(imagesJson[i]);
      if (!imageUrl) continue;
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position)
         VALUES ($1,$2,$3)`,
        [productId, imageUrl, i + 1],
      );
    }

    for (let i = 0; i < variantsJson.length; i++) {
      const variant = variantsJson[i];
      const variantRes = await client.query(
        `
        INSERT INTO product_variants (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [
          productId,
          variant.variant_key,
          JSON.stringify(variant.attributes),
          variant.base_price,
        ],
      );

      const variantId = variantRes.rows[0].id;
      for (const store of variant.store_prices) {
        if (!store.store_name) continue;

        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text, delivery_info)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (variant_id, store_name)
          DO UPDATE SET
            price = EXCLUDED.price,
            url = EXCLUDED.url,
            offer_text = EXCLUDED.offer_text,
            delivery_info = EXCLUDED.delivery_info
          `,
          [
            variantId,
            store.store_name,
            store.price,
            store.url,
            store.offer_text,
            store.delivery_info,
          ],
        );
      }

      for (
        let imageIndex = 0;
        imageIndex < variant.images.length;
        imageIndex++
      ) {
        const imageUrl = variant.images[imageIndex];
        await client.query(
          `
          INSERT INTO product_variant_images (variant_id, image_url, position)
          VALUES ($1,$2,$3)
          ON CONFLICT (variant_id, image_url)
          DO UPDATE SET position = EXCLUDED.position
          `,
          [variantId, imageUrl, imageIndex + 1],
        );
      }
    }

    await client.query(
      `
      INSERT INTO product_publish (product_id, is_published)
      VALUES ($1,$2)
      `,
      [productId, publish],
    );

    await client.query("COMMIT");

    return res.status(201).json({
      message: "TV created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("POST /api/tvs error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/tvs", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        t.category,
        t.model,
        COALESCE(pub.is_published, false) AS publish,

        t.key_specs_json,
        t.basic_info_json,
        t.display_json,
        t.video_engine_json,
        t.audio_json,
        t.smart_tv_json,
        t.gaming_json,
        t.ports_json,
        t.connectivity_json,
        t.power_json,
        t.physical_json,
        t.product_details_json,
        t.in_the_box_json,
        t.warranty_json,

        COALESCE(
          (
            SELECT json_agg(pi.image_url ORDER BY pi.position ASC NULLS LAST, pi.id ASC)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          COALESCE(t.images_json, '[]'::jsonb)::json
        ) AS images_json,

        COALESCE(
          (
            SELECT json_agg(
              jsonb_build_object(
                'variant_key', v.variant_key,
                'screen_size', COALESCE(v.attributes->>'screen_size', v.attributes->>'size'),
                'screen_size_value', NULLIF(
                  regexp_replace(
                    COALESCE(v.attributes->>'screen_size', v.attributes->>'size', v.variant_key, ''),
                    '[^0-9.]',
                    '',
                    'g'
                  ),
                  ''
                )::numeric,
                'base_price', v.base_price,
                'variant_id', v.id,
                'images', (
                  CASE
                    WHEN EXISTS (
                      SELECT 1 FROM product_variant_images pvi0 WHERE pvi0.variant_id = v.id
                    )
                    THEN (
                      SELECT COALESCE(
                        json_agg(pvi.image_url ORDER BY pvi.position ASC NULLS LAST, pvi.id ASC),
                        '[]'::json
                      )
                      FROM product_variant_images pvi
                      WHERE pvi.variant_id = v.id
                    )
                    ELSE COALESCE(v.attributes->'images', v.attributes->'images_json', '[]'::jsonb)::json
                  END
                ),
                'store_prices', (
                  SELECT COALESCE(
                    json_agg(
                      jsonb_build_object(
                        'id', sp.id,
                        'store_name', sp.store_name,
                        'price', sp.price,
                        'url', sp.url,
                        'offer_text', sp.offer_text,
                        'delivery_info', sp.delivery_info
                      )
                      ORDER BY sp.price ASC NULLS LAST, sp.id ASC
                    ),
                    '[]'::json
                  )
                  FROM variant_store_prices sp
                  WHERE sp.variant_id = v.id
                )
              )
              ORDER BY v.id ASC
            )
            FROM product_variants v
            WHERE v.product_id = p.id
          ),
          COALESCE(t.variants_json, '[]'::jsonb)::json
        ) AS variants_json

      FROM products p
      INNER JOIN tvs t
        ON t.product_id = p.id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      WHERE p.product_type = 'tv'
      ORDER BY COALESCE(ds.hook_score, 0) DESC, p.id DESC
    `);

    const tvs = applySpecScoreToRows(
      "tv",
      (result.rows || []).map((row) => stripScoreRecursively(row || {})),
      profileConfig.profiles,
    ).map(toPublicTvCatalogResponseRow);
    return res.json({ tvs });
  } catch (err) {
    console.error("GET /api/tvs error:", err);
    return res.status(500).json({ error: err.message });
  }
});
app.post("/api/networking", authenticate, async (req, res) => {
  const client = await db.connect();
  const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const { product, networking, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* ---------- 1️⃣ Insert product ---------- */
    const productRes = await client.query(
      `
      INSERT INTO products (name, brand_id, product_type)
      VALUES ($1,$2,'networking')
      RETURNING id
      `,
      [product.name, product.brand_id],
    );

    const productId = productRes.rows[0].id;

    /* ---------- 2️⃣ Insert networking (DB validates device_type) ---------- */
    await client.query(
      `
      INSERT INTO networking (
        product_id,
        device_type,
        model_number,
        release_year,
        country_of_origin,
        specifications,
        features,
        performance,
        connectivity,
        physical_details,
        warranty
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb
      )
      `,
      [
        productId,
        networking.device_type, // CHECK constraint enforces validity
        networking.model_number,
        networking.release_year,
        networking.country_of_origin,
        toJSON(networking.specifications),
        toJSON(networking.features),
        toJSON(networking.performance),
        toJSON(networking.connectivity),
        toJSON(networking.physical_details),
        toJSON(networking.warranty),
      ],
    );

    /* ---------- 3️⃣ Images ---------- */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position)
         VALUES ($1,$2,$3)`,
        [productId, url, i + 1],
      );
    }

    /* ---------- 4️⃣ Variants + Store Prices ---------- */
    for (const v of variants) {
      if (!v.variant_key) {
        throw new Error("variant_key is required");
      }

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [productId, v.variant_key, JSON.stringify(v), v.base_price],
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [variantId, s.store_name, s.price, s.url, s.offer_text || null],
        );
      }
    }

    /* ---------- 5️⃣ Publish default false ---------- */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId],
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Networking product created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");

    /* ---------- Handle CHECK constraint error ---------- */
    if (err.code === "23514") {
      return res.status(400).json({
        error: "Invalid device_type value",
      });
    }

    console.error("POST /api/networking error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/networking", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        n.device_type,
        n.model_number,
        n.release_year,
        n.country_of_origin,

        /* ---------- JSONB Columns ---------- */
        n.specifications,
        n.features,
        n.performance,
        n.connectivity,
        n.physical_details,
        n.warranty,

        n.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Rating ---------- */
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'attributes', v.attributes,
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN networking n
        ON n.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      WHERE p.product_type = 'networking'

      GROUP BY
        p.id, b.name,
        n.device_type,
        n.model_number,
        n.release_year,
        n.country_of_origin,
        n.specifications,
        n.features,
        n.performance,
        n.connectivity,
        n.physical_details,
        n.warranty,
        n.created_at

      ORDER BY p.id DESC;
    `);

    res.json({ networking: result.rows });
  } catch (err) {
    console.error("GET /api/networking error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Update smartphone (replace variants & variant_store_prices  if provided) - authenticated
app.put("/api/smartphone/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  console.log(req.body);
  // Accept payloads that wrap a single smartphone inside { smartphones: [ { ... } ] }
  if (
    req.body &&
    Array.isArray(req.body.smartphones) &&
    req.body.smartphones.length > 0
  ) {
    const first = req.body.smartphones[0];

    // deep-merge `first` into `req.body` but do not overwrite existing scalar values
    const isPlainObject = (v) =>
      v && typeof v === "object" && !Array.isArray(v);

    const mergeInto = (target, source) => {
      for (const key of Object.keys(source)) {
        const srcVal = source[key];
        const tgtVal = target[key];

        if (tgtVal === undefined) {
          // if target missing, copy entire value
          target[key] = srcVal;
          continue;
        }

        // if both are plain objects, recurse to preserve nested fields
        if (isPlainObject(tgtVal) && isPlainObject(srcVal)) {
          mergeInto(tgtVal, srcVal);
          continue;
        }

        // if target is array and source is array, prefer source only if target is empty
        if (Array.isArray(tgtVal) && Array.isArray(srcVal)) {
          if (tgtVal.length === 0 && srcVal.length > 0) target[key] = srcVal;
          continue;
        }

        // otherwise leave existing target value intact
      }
    };

    mergeInto(req.body, first);
  }
  try {
    await client.query("BEGIN");

    // Resolve the smartphone record by either internal id or product_id
    const rawId = req.params.id;
    const parsedId = Number(rawId);
    if (Number.isNaN(parsedId) || parsedId <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Invalid id" });
    }

    let findRes = await client.query(
      "SELECT id, product_id FROM smartphones WHERE product_id = $1 LIMIT 1",
      [parsedId],
    );
    if (!findRes.rows.length) {
      findRes = await client.query(
        "SELECT id, product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [parsedId],
      );
    }
    if (!findRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const sid = findRes.rows[0].id; // internal smartphone id

    const n = normalizeBodyKeys(req.body || {});
    // Accept several name aliases: `name`, `product_name`, `productName`, or normalized variants
    const name =
      n.name ||
      n.productname ||
      req.body.name ||
      req.body.product_name ||
      req.body.productName ||
      req.body.productName?.toString();
    if (!name) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Name is required" });
    }

    const launchStatusOverride = normalizeLaunchStatusOverride(
      req.body.launch_status_override ||
        req.body.launchStatusOverride ||
        req.body.smartphone?.launch_status_override ||
        req.body.smartphone?.launchStatusOverride,
    );

    /* ---------- UPDATE SMARTPHONE (PARENT) ---------- */
    const updatePhoneSQL = `
      UPDATE smartphones SET
        category=$1, brand=$2, model=$3, launch_date=$4,
        official_preorder_url=$5, launch_status_override=$6,
        images=$7, colors=$8, build_design=$9, display=$10, performance=$11,
        camera=$12, battery=$13, connectivity=$14, network=$15, ports=$16,
        audio=$17, multimedia=$18, sensors=$19
      WHERE id=$20
      RETURNING *;
    `;

    const phoneRes = await client.query(updatePhoneSQL, [
      req.body.category || req.body.segment || null,
      req.body.brand || null,
      req.body.model || null,
      parseDateForImport(req.body.launch_date),
      req.body.official_preorder_url ||
        req.body.officialPreorderUrl ||
        req.body.smartphone?.official_preorder_url ||
        req.body.smartphone?.officialPreorderUrl ||
        null,
      launchStatusOverride,
      JSON.stringify(req.body.images || []),
      JSON.stringify(req.body.colors || []),
      JSON.stringify(req.body.build_design || {}),
      JSON.stringify(req.body.display || {}),
      JSON.stringify(req.body.performance || {}),
      JSON.stringify(req.body.camera || {}),
      JSON.stringify(req.body.battery || {}),
      JSON.stringify(req.body.connectivity || {}),
      JSON.stringify(req.body.network || {}),
      JSON.stringify(req.body.ports || {}),
      JSON.stringify(req.body.audio || {}),
      JSON.stringify(req.body.multimedia || {}),
      req.body.sensors === null ? null : JSON.stringify(req.body.sensors || []),
      sid,
    ]);

    if (!phoneRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    // Update product name in products table (product name stored on products)
    const productId = phoneRes.rows[0].product_id || null;
    if (productId && name) {
      await client.query(`UPDATE products SET name = $1 WHERE id = $2`, [
        name,
        productId,
      ]);
    }

    // Replace product_images to reflect new images array (if provided)
    try {
      if (productId) {
        await client.query("DELETE FROM product_images WHERE product_id = $1", [
          productId,
        ]);
        const imgs = Array.isArray(req.body.images) ? req.body.images : [];
        for (let i = 0; i < imgs.length; i++) {
          await client.query(
            "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
            [productId, imgs[i], i + 1],
          );
        }
      }
    } catch (piErr) {
      console.error(
        "Failed to replace product_images:",
        piErr.message || piErr,
      );
      // non-fatal: continue without aborting the whole update
    }

    /* ---------- UPSERT VARIANTS ---------- */
    if (Array.isArray(req.body.variants)) {
      // Ensure we have the product_id for this smartphone
      const productId = phoneRes.rows[0].product_id || null;
      if (!productId) {
        await client.query("ROLLBACK");
        return res
          .status(500)
          .json({ message: "Missing product_id for smartphone" });
      }

      const variantUpsertSQL = `
        INSERT INTO product_variants 
          (id, product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (id)
        DO UPDATE SET
          variant_key=EXCLUDED.variant_key,
          attributes=EXCLUDED.attributes,
          base_price=EXCLUDED.base_price
        RETURNING id;
      `;

      const insertVariantSQL = `
        INSERT INTO product_variants 
          (product_id, variant_key, attributes, base_price)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (product_id, variant_key)
        DO UPDATE SET
          attributes = EXCLUDED.attributes,
          base_price = EXCLUDED.base_price
        RETURNING id;
      `;

      // Map input variant index -> DB id (useful when client sends variant indices)
      const variantIdMap = [];
      const keepVariantIds = [];

      for (let vi = 0; vi < req.body.variants.length; vi++) {
        const v = req.body.variants[vi];
        const ram = v.ram || null;
        const storageVal = v.storage || v.storage_size || null;
        const base_price = v.base_price ?? null;

        const variantKey =
          (v.variant_key && String(v.variant_key).trim()) ||
          `${String(ram || "na")}_${String(storageVal || "na")}`;
        const attrsObj = Object.assign({}, v.attributes || {});
        if (ram !== null) attrsObj.ram = ram;
        if (storageVal !== null) attrsObj.storage = storageVal;

        if (v.id) {
          const r = await client.query(variantUpsertSQL, [
            v.id,
            productId,
            variantKey,
            JSON.stringify(attrsObj),
            base_price,
          ]);
          variantIdMap[vi] = r.rows[0].id;
          keepVariantIds.push(r.rows[0].id);
        } else {
          const r = await client.query(insertVariantSQL, [
            productId,
            variantKey,
            JSON.stringify(attrsObj),
            base_price,
          ]);
          variantIdMap[vi] = r.rows[0].id;
          keepVariantIds.push(r.rows[0].id);
        }
      }

      // expose the mapping for later price handling
      req._variantIdMap = variantIdMap;

      // Replace semantics: if `variants` array is provided, remove variants not present anymore.
      // This enables "delete variant" from the admin UI by simply omitting that variant from the payload.
      const keepIds = keepVariantIds
        .map((x) => Number(x))
        .filter((n) => Number.isInteger(n) && n > 0);
      if (keepIds.length === 0) {
        await client.query(
          "DELETE FROM product_variants WHERE product_id = $1",
          [productId],
        );
      } else {
        await client.query(
          "DELETE FROM product_variants WHERE product_id = $1 AND NOT (id = ANY($2::int[]))",
          [productId, keepIds],
        );
      }
    }

    /* ---------- UPSERT STORE PRICES ---------- */
    if (Array.isArray(req.body.variant_store_prices)) {
      // Replace semantics: if `variant_store_prices` is provided, ensure removed
      // store entries are deleted as well (so "delete store from variant" persists).
      if (!productId) {
        await client.query("ROLLBACK");
        return res
          .status(500)
          .json({ message: "Missing product_id for smartphone" });
      }

      const priceUpsertSQL = `
        INSERT INTO variant_store_prices 
          (id, variant_id, store_name, price, url, offer_text, sale_start_date)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (id)
        DO UPDATE SET
          store_name=EXCLUDED.store_name,
          price=EXCLUDED.price,
          url=EXCLUDED.url,
          offer_text=EXCLUDED.offer_text,
          sale_start_date=EXCLUDED.sale_start_date;
      `;

      const insertPriceSQL = `
        INSERT INTO variant_store_prices 
          (variant_id, store_name, price, url, offer_text, sale_start_date)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (variant_id, store_name)
        DO UPDATE SET
          price = EXCLUDED.price,
          url = EXCLUDED.url,
          offer_text = EXCLUDED.offer_text,
          sale_start_date = EXCLUDED.sale_start_date
        RETURNING id;
      `;

      const variantIdMap = req._variantIdMap || [];

      // Load the set of variants that belong to this product (prevents editing
      // store prices for another product's variants and helps resolve indices).
      const pv = await client.query(
        "SELECT id FROM product_variants WHERE product_id = $1",
        [productId],
      );
      const productVariantIds = pv.rows.map((r) => r.id);
      const productVariantIdSet = new Set(productVariantIds);

      const resolveVariantId = (sp) => {
        if (!sp) return null;

        // Prefer explicit index mapping (unambiguous for clients that use it)
        if (sp.variant_index !== undefined && sp.variant_index !== null) {
          const idx = Number(sp.variant_index);
          if (!Number.isNaN(idx) && variantIdMap[idx]) {
            const mapped = Number(variantIdMap[idx]);
            return productVariantIdSet.has(mapped) ? mapped : null;
          }
        }

        // Prefer DB ids when they belong to this product; otherwise fall back to
        // legacy behavior where `variant_id` might actually be an index.
        if (sp.variant_id !== undefined && sp.variant_id !== null) {
          const vnum = Number(sp.variant_id);
          if (Number.isNaN(vnum)) return null;
          if (productVariantIdSet.has(vnum)) return vnum;

          if (variantIdMap[vnum]) {
            const mapped = Number(variantIdMap[vnum]);
            return productVariantIdSet.has(mapped) ? mapped : null;
          }
        }

        return null;
      };

      // Determine which variants should have their store prices replaced:
      // - If `variants` is provided, treat store prices as full replacement for all variants.
      // - Otherwise, replace only for variants referenced by the payload.
      const replaceVariantIds = new Set();
      if (Array.isArray(req.body.variants)) {
        productVariantIds.forEach((id) => replaceVariantIds.add(id));
      } else {
        for (const sp of req.body.variant_store_prices) {
          const vid = resolveVariantId(sp);
          if (vid) replaceVariantIds.add(vid);
        }
      }

      // Delete store prices that are no longer present (by id) so removals persist.
      const keepPriceIdsByVariant = new Map();
      for (const vid of replaceVariantIds)
        keepPriceIdsByVariant.set(vid, new Set());
      for (const sp of req.body.variant_store_prices) {
        const vid = resolveVariantId(sp);
        if (!vid || !replaceVariantIds.has(vid)) continue;
        const pid = Number(sp.id);
        if (Number.isInteger(pid) && pid > 0) {
          keepPriceIdsByVariant.get(vid).add(pid);
        }
      }

      for (const vid of replaceVariantIds) {
        const keepIds = Array.from(keepPriceIdsByVariant.get(vid) || []);
        if (keepIds.length === 0) {
          await client.query(
            "DELETE FROM variant_store_prices WHERE variant_id = $1",
            [vid],
          );
        } else {
          await client.query(
            "DELETE FROM variant_store_prices WHERE variant_id = $1 AND NOT (id = ANY($2::int[]))",
            [vid, keepIds],
          );
        }
      }

      for (const sp of req.body.variant_store_prices) {
        const resolvedVariantId = resolveVariantId(sp);
        if (!resolvedVariantId) continue; // cannot resolve target variant

        const store_name = sp.store_name || sp.store || null;
        if (!store_name) continue;

        const parsedPrice =
          sp.price !== undefined && sp.price !== null && sp.price !== ""
            ? Number(sp.price)
            : null;
        const price = Number.isFinite(parsedPrice) ? parsedPrice : null;

        const url = sp.url || null;
        const offer_text = sp.offer_text || sp.offer || null;
        const sale_start_date = normalizeDateOnlyInput(
          sp.sale_start_date ?? sp.sale_date ?? sp.saleStartDate ?? null,
        );

        if (sp.id) {
          await client.query(priceUpsertSQL, [
            sp.id,
            resolvedVariantId,
            store_name,
            price,
            url,
            offer_text,
            sale_start_date,
          ]);
        } else {
          await client.query(insertPriceSQL, [
            resolvedVariantId,
            store_name,
            price,
            url,
            offer_text,
            sale_start_date,
          ]);
        }
      }
    }

    // If client provided `published` flag, reflect it in product_publish table
    if (req.body.published !== undefined) {
      try {
        const productId = phoneRes.rows[0].product_id || null;
        if (productId) {
          await client.query(
            `
            INSERT INTO product_publish (product_id, is_published, published_by)
            VALUES ($1, $2, $3)
            ON CONFLICT (product_id)
            DO UPDATE SET
              is_published = EXCLUDED.is_published,
              published_by = EXCLUDED.published_by,
              updated_at = now();
            `,
            [
              productId,
              req.body.published,
              req.user && req.user.id ? req.user.id : null,
            ],
          );
        }
      } catch (pubErr) {
        console.error(
          "Failed to update product_publish for smartphone:",
          pubErr,
        );
        // don't fail the whole update for publish tracking issues
      }
    }

    // ---------- UPDATE SPHERE RATINGS (if provided in request body) ----------
    try {
      const productId = phoneRes.rows[0].product_id || null;
      if (productId) {
        await client.query(
          `INSERT INTO product_sphere_ratings
            (product_id, design, display, performance, camera, battery, connectivity, network)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (product_id) DO UPDATE SET
             design = COALESCE(EXCLUDED.design, product_sphere_ratings.design),
             display = COALESCE(EXCLUDED.display, product_sphere_ratings.display),
             performance = COALESCE(EXCLUDED.performance, product_sphere_ratings.performance),
             camera = COALESCE(EXCLUDED.camera, product_sphere_ratings.camera),
             battery = COALESCE(EXCLUDED.battery, product_sphere_ratings.battery),
             connectivity = COALESCE(EXCLUDED.connectivity, product_sphere_ratings.connectivity),
             network = COALESCE(EXCLUDED.network, product_sphere_ratings.network),
             updated_at = CURRENT_TIMESTAMP
          `,
          [
            productId,
            JSON.stringify(req.body.build_design?.sphere_rating || null),
            JSON.stringify(req.body.display?.sphere_rating || null),
            JSON.stringify(req.body.performance?.sphere_rating || null),
            JSON.stringify(req.body.camera?.sphere_rating || null),
            JSON.stringify(req.body.battery?.sphere_rating || null),
            JSON.stringify(req.body.connectivity?.sphere_rating || null),
            JSON.stringify(req.body.network?.sphere_rating || null),
          ],
        );
      }
    } catch (srup) {
      console.error(
        "Sphere ratings upsert error (update):",
        srup.message || srup,
      );
    }

    await client.query("COMMIT");
    return res.json({
      message: "Smartphone updated successfully",
      data: phoneRes.rows[0],
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/smartphone/:id error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Update smartphone (simplified API for ApiTester) - POST /api/smartphone/:id/update
// Payload format similar to /api/smartphones/req but for updating existing device
app.post("/api/smartphone/:id/update", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const rawId = req.params.id;
    const parsedId = Number(rawId);
    if (Number.isNaN(parsedId) || parsedId <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Invalid id" });
    }

    // Find smartphone by product_id or internal id
    let findRes = await client.query(
      "SELECT id, product_id FROM smartphones WHERE product_id = $1 LIMIT 1",
      [parsedId],
    );
    if (!findRes.rows.length) {
      findRes = await client.query(
        "SELECT id, product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [parsedId],
      );
    }
    if (!findRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const sid = findRes.rows[0].id;
    const productId = findRes.rows[0].product_id;
    const b = req.body || {};

    // Prepare simplified payload fields (similar to /req endpoint)
    const safeJSONParse = (raw) => {
      if (raw === null || raw === undefined || raw === "") return null;
      if (typeof raw === "object") return raw;
      try {
        return JSON.parse(String(raw));
      } catch (err) {
        try {
          const alt = String(raw)
            .replace(/\u2018|\u2019|\u201C|\u201D/g, '"')
            .replace(/'/g, '"');
          return JSON.parse(alt);
        } catch (err2) {
          return null;
        }
      }
    };

    const mergeSectionObjects = (...parts) => {
      const out = {};
      for (const part of parts) {
        if (part && typeof part === "object" && !Array.isArray(part)) {
          Object.assign(out, part);
        }
      }
      return out;
    };

    const parseSensors = (raw) => {
      if (raw === null || raw === undefined || raw === "") return null;
      if (Array.isArray(raw))
        return JSON.stringify(raw.map((s) => String(s).trim()));
      if (typeof raw === "object") return JSON.stringify(raw);
      const str = String(raw).trim();
      const parts = str
        .split(/[|,;]+/)
        .map((p) => p.trim())
        .filter(Boolean);
      return parts.length ? JSON.stringify(parts) : null;
    };

    // Parse payload fields
    const category = (b.category || "").trim() || null;
    const brand = (b.brand_name || b.brand || "").trim() || null;
    const model = (b.model || "").trim() || null;
    const launch_date = b.launch_date || null;
    const official_preorder_url =
      b.official_preorder_url || b.officialPreorderUrl || null;

    const launchStatusOverride = normalizeLaunchStatusOverride(
      b.launch_status_override || b.launchStatusOverride,
    );

    const images =
      safeJSONParse(b.images_json) || safeJSONParse(b.images) || [];
    const colors =
      safeJSONParse(b.colors_json) || safeJSONParse(b.colors) || [];
    const build_design =
      safeJSONParse(b.build_design_json) || safeJSONParse(b.build_design) || {};
    const display =
      safeJSONParse(b.display_json) || safeJSONParse(b.display) || {};
    const performance =
      safeJSONParse(b.performance_json) || safeJSONParse(b.performance) || {};
    const camera =
      safeJSONParse(b.camera_json) || safeJSONParse(b.camera) || {};
    const battery =
      safeJSONParse(b.battery_json) || safeJSONParse(b.battery) || {};
    const connectivity = mergeSectionObjects(
      safeJSONParse(b.connectivity_json),
      safeJSONParse(b.network_connectivity_json),
      safeJSONParse(b.connectivity),
    );
    const network = mergeSectionObjects(
      safeJSONParse(b.network_json),
      safeJSONParse(b.navigation_json),
      safeJSONParse(b.network),
    );
    const ports =
      safeJSONParse(b.port_json) ||
      safeJSONParse(b.ports_json) ||
      safeJSONParse(b.ports) ||
      {};
    const audio = safeJSONParse(b.audio_json) || safeJSONParse(b.audio) || {};
    const multimedia =
      safeJSONParse(b.multimedia_json) || safeJSONParse(b.multimedia) || {};
    const sensorsJson = safeJSONParse(b.sensors_json);
    const sensorsInput =
      b.sensors ??
      (Array.isArray(sensorsJson?.sensors)
        ? sensorsJson.sensors
        : sensorsJson || null);
    const sensors = parseSensors(sensorsInput);

    // Update smartphone record
    const updateSQL = `
      UPDATE smartphones SET
        category=$1, brand=$2, model=$3, launch_date=$4,
        official_preorder_url=$5, launch_status_override=$6,
        images=$7, colors=$8, build_design=$9, display=$10, performance=$11,
        camera=$12, battery=$13, connectivity=$14, network=$15, ports=$16,
        audio=$17, multimedia=$18, sensors=$19
      WHERE id=$20
      RETURNING *;
    `;

    const phoneRes = await client.query(updateSQL, [
      category,
      brand,
      model,
      parseDateForImport(launch_date),
      official_preorder_url,
      launchStatusOverride,
      JSON.stringify(images),
      JSON.stringify(colors),
      JSON.stringify(build_design),
      JSON.stringify(display),
      JSON.stringify(performance),
      JSON.stringify(camera),
      JSON.stringify(battery),
      JSON.stringify(connectivity),
      JSON.stringify(network),
      JSON.stringify(ports),
      JSON.stringify(audio),
      JSON.stringify(multimedia),
      sensors,
      sid,
    ]);

    if (!phoneRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    // Update product name if provided
    const product_name = (b.product_name || b.name || "").trim();
    if (product_name && productId) {
      await client.query(`UPDATE products SET name = $1 WHERE id = $2`, [
        product_name,
        productId,
      ]);
    }

    // Handle published flag if provided
    if (b.published !== undefined || b.publish !== undefined) {
      const isPublished = b.published !== undefined ? b.published : b.publish;
      try {
        if (productId) {
          await client.query(
            `
            INSERT INTO product_publish (product_id, is_published, published_by)
            VALUES ($1, $2, $3)
            ON CONFLICT (product_id)
            DO UPDATE SET
              is_published = EXCLUDED.is_published,
              published_by = EXCLUDED.published_by,
              updated_at = now();
            `,
            [
              productId,
              isPublished,
              req.user && req.user.id ? req.user.id : null,
            ],
          );
        }
      } catch (pubErr) {
        console.error("Failed to update publish status:", pubErr);
      }
    }

    await client.query("COMMIT");
    return res.json({
      message: "Smartphone updated successfully",
      data: phoneRes.rows[0],
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("POST /api/smartphone/:id/update error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete smartphone
app.delete("/api/smartphone/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  console.log(req.params.id);
  try {
    const sid = Number(req.params.id);
    if (Number.isNaN(sid) || sid <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    await client.query("BEGIN");

    // resolve product_id from smartphone
    // Accept either internal smartphones.id or the linked products.id (product_id)
    let sres = await client.query(
      "SELECT product_id FROM smartphones WHERE product_id = $1 LIMIT 1",
      [sid],
    );
    if (!sres.rows.length) {
      sres = await client.query(
        "SELECT product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [sid],
      );
    }
    if (!sres.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const productId = sres.rows[0].product_id;

    // check publish status from product_publish table
    const pub = await client.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );

    if (pub.rows.length && pub.rows[0].is_published) {
      await client.query("ROLLBACK");
      return res
        .status(403)
        .json({ message: "Cannot delete: Smartphone is published" });
    }

    // delete any publish record for the product
    await client.query("DELETE FROM product_publish WHERE product_id = $1", [
      productId,
    ]);

    // delete any product comparisons referencing this product (either side)
    await client.query(
      "DELETE FROM product_comparisons WHERE product_id = $1 OR compared_with = $1",
      [productId],
    );

    // delete any sphere ratings associated with this product
    await client.query(
      "DELETE FROM product_sphere_ratings WHERE product_id = $1",
      [productId],
    );

    // delete the product (cascades to smartphones, product_variants, product_images via FK ON DELETE CASCADE)
    await client.query("DELETE FROM products WHERE id = $1", [productId]);

    await client.query("COMMIT");
    return res.json({
      message: "Unpublished smartphone and product deleted successfully",
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("DELETE /api/smartphone/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete laptop
app.delete("/api/laptop/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    const lid = Number(req.params.id);
    if (Number.isNaN(lid) || lid <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    await client.query("BEGIN");

    // Accept either laptop.product_id or laptop.id
    let lres = await client.query(
      "SELECT product_id FROM laptop WHERE product_id = $1 LIMIT 1",
      [lid],
    );
    if (!lres.rows.length) {
      lres = await client.query(
        "SELECT product_id FROM laptop WHERE id = $1 LIMIT 1",
        [lid],
      );
    }
    if (!lres.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Laptop not found" });
    }

    const productId = lres.rows[0].product_id;

    // Prevent deleting published laptops
    const pub = await client.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    if (pub.rows.length && pub.rows[0].is_published) {
      await client.query("ROLLBACK");
      return res
        .status(403)
        .json({ message: "Cannot delete: Laptop is published" });
    }

    await client.query("DELETE FROM product_publish WHERE product_id = $1", [
      productId,
    ]);
    await client.query(
      "DELETE FROM product_comparisons WHERE product_id = $1 OR compared_with = $1",
      [productId],
    );
    await client.query(
      "DELETE FROM product_sphere_ratings WHERE product_id = $1",
      [productId],
    );
    await client.query("DELETE FROM products WHERE id = $1", [productId]);

    await client.query("COMMIT");
    return res.json({
      message: "Unpublished laptop and product deleted successfully",
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("DELETE /api/laptop/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete TV
app.delete("/api/tvs/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    const tid = Number(req.params.id);
    if (Number.isNaN(tid) || tid <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    await client.query("BEGIN");

    const tvRes = await client.query(
      "SELECT product_id FROM tvs WHERE product_id = $1 LIMIT 1",
      [tid],
    );
    if (!tvRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "TV not found" });
    }

    const productId = tvRes.rows[0].product_id;

    const pubRes = await client.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    if (pubRes.rows.length && pubRes.rows[0].is_published) {
      await client.query("ROLLBACK");
      return res
        .status(403)
        .json({ message: "Cannot delete: TV is published" });
    }

    await client.query("DELETE FROM product_publish WHERE product_id = $1", [
      productId,
    ]);
    await client.query(
      "DELETE FROM product_comparisons WHERE product_id = $1 OR compared_with = $1",
      [productId],
    );
    await client.query(
      "DELETE FROM product_sphere_ratings WHERE product_id = $1",
      [productId],
    );
    await client.query("DELETE FROM products WHERE id = $1", [productId]);

    await client.query("COMMIT");
    return res.json({
      message: "Unpublished TV and product deleted successfully",
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("DELETE /api/tvs/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete a color from a smartphone's colors JSONB by index
app.get("/api/laptop", authenticate, async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.meta,
        l.spec_sections,
        l.created_at,

        COALESCE(pub.is_published, false) AS is_published

      FROM products p
      INNER JOIN laptop l
        ON l.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      WHERE p.product_type = 'laptop'

      ORDER BY p.id DESC
    `);

    const laptops = applySpecScoreToRows(
      "laptop",
      (result.rows || []).map((row) =>
        stripScoreRecursively(enrichLaptopResponse(row || {})),
      ),
      profileConfig.profiles,
    );
    res.json({ laptops });
  } catch (err) {
    console.error("GET /api/laptop error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/tv", authenticate, async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        p.brand_id,
        t.category,
        t.model,

        t.key_specs_json,
        t.basic_info_json,
        t.display_json,
        t.video_engine_json,
        t.audio_json,
        t.smart_tv_json,
        t.gaming_json,
        t.ports_json,
        t.connectivity_json,
        t.power_json,
        t.physical_json,
        t.product_details_json,
        t.in_the_box_json,
        t.warranty_json,
        t.images_json,
        t.variants_json,
        t.created_at,

        COALESCE(pub.is_published, false) AS is_published
      FROM products p
      INNER JOIN tvs t
        ON t.product_id = p.id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_publish pub
        ON pub.product_id = p.id
      WHERE p.product_type = 'tv'
      ORDER BY p.id DESC
    `);

    const tvs = applySpecScoreToRows(
      "tv",
      (result.rows || []).map((row) => stripScoreRecursively(row || {})),
      profileConfig.profiles,
    );
    return res.json({ tvs });
  } catch (err) {
    console.error("GET /api/tv error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/tvs/:id", authenticate, async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const rawId = req.params.id;
    const pid = Number(rawId);
    if (!rawId || rawId.trim() === "") {
      return res.status(400).json({ message: "Invalid id" });
    }

    const tvRes = await db.query(
      "SELECT * FROM tvs WHERE product_id = $1 LIMIT 1",
      [pid],
    );
    if (!tvRes.rows.length) {
      return res.status(404).json({ message: "Not found" });
    }

    const tv = tvRes.rows[0];
    const productId = tv.product_id;

    const productRes = await db.query(
      `SELECT p.name, p.brand_id, b.name AS brand_name
       FROM products p
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.id = $1
       LIMIT 1`,
      [productId],
    );

    const product = productRes.rows[0] || {
      name: null,
      brand_id: null,
      brand_name: null,
    };

    const imagesRes = await db.query(
      "SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC, id ASC",
      [productId],
    );
    const imagesJson = imagesRes.rows.map((row) => row.image_url);

    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variantsJson = [];
    for (const variant of variantsRes.rows) {
      const storesRes = await db.query(
        "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY price ASC NULLS LAST, id ASC",
        [variant.id],
      );
      const imagesResByVariant = await db.query(
        "SELECT image_url FROM product_variant_images WHERE variant_id = $1 ORDER BY position ASC NULLS LAST, id ASC",
        [variant.id],
      );
      const attributeObject = toPlainObject(variant.attributes);
      const fallbackVariantImages = normalizeImageArray(
        Array.isArray(attributeObject.images)
          ? attributeObject.images
          : Array.isArray(attributeObject.images_json)
            ? attributeObject.images_json
            : [],
      );
      const variantImages =
        imagesResByVariant.rows.map((row) => row.image_url).filter(Boolean)
          .length > 0
          ? imagesResByVariant.rows.map((row) => row.image_url).filter(Boolean)
          : fallbackVariantImages;

      variantsJson.push({
        ...attributeObject,
        variant_id: variant.id,
        variant_key: variant.variant_key,
        screen_size:
          normalizeNullableText(
            attributeObject.screen_size ||
              attributeObject.size ||
              variant.variant_key,
          ) || null,
        screen_size_value: parseFirstNumeric(
          attributeObject.screen_size ||
            attributeObject.size ||
            variant.variant_key,
        ),
        base_price: variant.base_price,
        images: variantImages,
        store_prices: storesRes.rows,
      });
    }

    const publishRes = await db.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    const published = publishRes.rows[0]
      ? publishRes.rows[0].is_published
      : false;

    const scoredTv = applySpecScoreToRow(
      "tv",
      stripScoreRecursively({
        ...tv,
        name: product.name || null,
        brand_name: product.brand_name || null,
        images: imagesJson,
        images_json: imagesJson,
        variants: variantsJson,
        variants_json: variantsJson,
      }),
      profileConfig.profiles,
    );

    return res.json({
      product,
      tv: scoredTv,
      images_json: imagesJson,
      variants_json: variantsJson,
      published,
    });
  } catch (err) {
    console.error("GET /api/tvs/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.put("/api/tvs/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const rawId = req.params.id;
    const pid = Number(rawId);
    if (!rawId || rawId.trim() === "") {
      return res.status(400).json({ message: "Invalid id" });
    }

    const tvLookup = await db.query(
      "SELECT * FROM tvs WHERE product_id = $1 LIMIT 1",
      [pid],
    );
    if (!tvLookup.rows.length) {
      return res.status(404).json({ message: "Not found" });
    }

    const tvRow = tvLookup.rows[0];
    const productId = tvRow.product_id;

    const payload = normalizeTvPayloadInput(req.body || {});
    const product = toPlainObject(payload.product);

    let productName = normalizeNullableText(
      payload.product_name || payload.name || product.name,
    );

    let brandId =
      payload.brand_id !== undefined && payload.brand_id !== null
        ? Number(payload.brand_id)
        : product.brand_id !== undefined && product.brand_id !== null
          ? Number(product.brand_id)
          : null;

    if ((!Number.isInteger(brandId) || brandId <= 0) && payload.brand_name) {
      brandId = await resolveBrandIdByName(client, payload.brand_name);
    }

    const category = hasOwn(payload, "category")
      ? normalizeNullableText(payload.category)
      : tvRow.category;
    const model = hasOwn(payload, "model")
      ? normalizeNullableText(payload.model)
      : tvRow.model;

    const sectionValues = {};
    for (const key of TV_JSON_OBJECT_SECTIONS) {
      sectionValues[key] = hasOwn(payload, key)
        ? toPlainObject(payload[key])
        : toPlainObject(tvRow[key]);
    }

    const imagesJson = hasOwn(payload, "images_json")
      ? Array.isArray(payload.images_json)
        ? payload.images_json
        : []
      : hasOwn(payload, "images")
        ? Array.isArray(payload.images)
          ? payload.images
          : []
        : Array.isArray(tvRow.images_json)
          ? tvRow.images_json
          : [];

    const variantsJson = normalizeTvVariantsInput(
      hasOwn(payload, "variants_json")
        ? Array.isArray(payload.variants_json)
          ? payload.variants_json
          : []
        : hasOwn(payload, "variants")
          ? Array.isArray(payload.variants)
            ? payload.variants
            : []
          : Array.isArray(tvRow.variants_json)
            ? tvRow.variants_json
            : [],
    );
    const variantsJsonForRow = variantsJson.map((variant) => ({
      variant_key: variant.variant_key,
      screen_size: variant.screen_size,
      screen_size_value: variant.screen_size_value,
      base_price: variant.base_price,
      store_prices: variant.store_prices,
      images: variant.images,
      ...toPlainObject(variant.attributes),
    }));

    const publish = hasOwn(payload, "publish")
      ? Boolean(payload.publish)
      : hasOwn(payload, "published")
        ? Boolean(payload.published)
        : undefined;

    await client.query("BEGIN");

    if (productName || Number.isInteger(brandId)) {
      const existingProduct = await client.query(
        "SELECT name, brand_id FROM products WHERE id = $1 LIMIT 1",
        [productId],
      );

      const currentProduct = existingProduct.rows[0] || {};
      if (!productName) productName = currentProduct.name || null;
      const brandToSave = Number.isInteger(brandId)
        ? brandId
        : currentProduct.brand_id;

      await client.query(
        "UPDATE products SET name = $1, brand_id = $2 WHERE id = $3",
        [productName, brandToSave || null, productId],
      );
    }

    await client.query(
      `
      UPDATE tvs SET
        category = $1,
        model = $2,
        key_specs_json = $3::jsonb,
        basic_info_json = $4::jsonb,
        display_json = $5::jsonb,
        video_engine_json = $6::jsonb,
        audio_json = $7::jsonb,
        smart_tv_json = $8::jsonb,
        gaming_json = $9::jsonb,
        ports_json = $10::jsonb,
        connectivity_json = $11::jsonb,
        power_json = $12::jsonb,
        physical_json = $13::jsonb,
        product_details_json = $14::jsonb,
        in_the_box_json = $15::jsonb,
        warranty_json = $16::jsonb,
        images_json = $17::jsonb,
        variants_json = $18::jsonb
      WHERE product_id = $19
      `,
      [
        category,
        model,
        toJSON(sectionValues.key_specs_json),
        toJSON(sectionValues.basic_info_json),
        toJSON(sectionValues.display_json),
        toJSON(sectionValues.video_engine_json),
        toJSON(sectionValues.audio_json),
        toJSON(sectionValues.smart_tv_json),
        toJSON(sectionValues.gaming_json),
        toJSON(sectionValues.ports_json),
        toJSON(sectionValues.connectivity_json),
        toJSON(sectionValues.power_json),
        toJSON(sectionValues.physical_json),
        toJSON(sectionValues.product_details_json),
        toJSON(sectionValues.in_the_box_json),
        toJSON(sectionValues.warranty_json),
        toJSON(imagesJson),
        toJSON(variantsJsonForRow),
        productId,
      ],
    );

    await client.query("DELETE FROM product_images WHERE product_id = $1", [
      productId,
    ]);
    for (let i = 0; i < imagesJson.length; i++) {
      const imageUrl = normalizeNullableText(imagesJson[i]);
      if (!imageUrl) continue;
      await client.query(
        "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
        [productId, imageUrl, i + 1],
      );
    }

    const oldVariantRes = await client.query(
      "SELECT id FROM product_variants WHERE product_id = $1",
      [productId],
    );
    for (const row of oldVariantRes.rows) {
      await client.query(
        "DELETE FROM variant_store_prices WHERE variant_id = $1",
        [row.id],
      );
    }
    await client.query("DELETE FROM product_variants WHERE product_id = $1", [
      productId,
    ]);

    for (let i = 0; i < variantsJson.length; i++) {
      const variant = variantsJson[i];
      const variantRes = await client.query(
        `
        INSERT INTO product_variants (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [
          productId,
          variant.variant_key,
          JSON.stringify(variant.attributes),
          variant.base_price,
        ],
      );

      const variantId = variantRes.rows[0].id;
      for (const store of variant.store_prices) {
        if (!store.store_name) continue;

        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text, delivery_info)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (variant_id, store_name)
          DO UPDATE SET
            price = EXCLUDED.price,
            url = EXCLUDED.url,
            offer_text = EXCLUDED.offer_text,
            delivery_info = EXCLUDED.delivery_info
          `,
          [
            variantId,
            store.store_name,
            store.price,
            store.url,
            store.offer_text,
            store.delivery_info,
          ],
        );
      }

      for (
        let imageIndex = 0;
        imageIndex < variant.images.length;
        imageIndex++
      ) {
        const imageUrl = variant.images[imageIndex];
        await client.query(
          `
          INSERT INTO product_variant_images (variant_id, image_url, position)
          VALUES ($1,$2,$3)
          ON CONFLICT (variant_id, image_url)
          DO UPDATE SET position = EXCLUDED.position
          `,
          [variantId, imageUrl, imageIndex + 1],
        );
      }
    }

    if (publish !== undefined) {
      const updatePublish = await client.query(
        "UPDATE product_publish SET is_published = $1 WHERE product_id = $2",
        [publish, productId],
      );
      if (updatePublish.rowCount === 0) {
        await client.query(
          "INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)",
          [productId, publish],
        );
      }
    }

    await client.query("COMMIT");

    return res.json({
      message: "TV updated",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/tvs/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});
/* -----------------------
  Ram/Storage/Long API
------------------------*/

// Get all specs (public)
app.get("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const r = await db.query(
      "SELECT * FROM ram_storage_long  ORDER BY id DESC",
    );
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/specs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Create a spec entry (authenticated)
app.post("/api/ram-storage-config", authenticate, async (req, res) => {
  console.log(req.body);
  try {
    const { ram, storage } = req.body;
    // accept multiple possible keys from client: 'product_type', 'long', or 'description'
    const product_type =
      req.body.product_type ||
      req.body.long ||
      req.body.long_term_storage ||
      req.body.description ||
      null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const productTypeVal = product_type ? String(product_type).trim() : null;

    // Check if the same ram+storage combination already exists
    const exists = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 LIMIT 1`,
      [ramVal, storageVal],
    );
    if (exists.rows.length > 0) {
      return res.status(409).json({
        message: "This ram/storage combination already exists",
        id: exists.rows[0].id,
      });
    }

    const r = await db.query(
      `INSERT INTO ram_storage_long (ram, storage, product_type) VALUES ($1, $2, $3) RETURNING *`,
      [ramVal, storageVal, productTypeVal],
    );

    return res.status(201).json({ message: "Spec created", data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/specs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update a spec entry (authenticated)
app.put("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const { ram, storage } = req.body;
    const product_type =
      req.body.product_type ||
      req.body.long ||
      req.body.long_term_storage ||
      req.body.description ||
      null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const productTypeVal = product_type ? String(product_type).trim() : null;

    // Check duplicate combination on other rows
    const dup = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 AND id <> $3 LIMIT 1`,
      [ramVal, storageVal, id],
    );
    if (dup.rows.length > 0) {
      return res.status(409).json({
        message: "Another entry with same ram/storage exists",
        id: dup.rows[0].id,
      });
    }

    const result = await db.query(
      `UPDATE ram_storage_long SET ram = $1, storage = $2, product_type = $3 WHERE id = $4 RETURNING *`,
      [ramVal, storageVal, productTypeVal, id],
    );

    if (result.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });

    return res.json({ message: "Spec updated", data: result.rows[0] });
  } catch (err) {
    console.error("PUT /api/ram-storage-config/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a spec entry (authenticated) - path expected by client
app.delete("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const r = await db.query("DELETE FROM ram_storage_long WHERE id = $1", [
      id,
    ]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });
    return res.json({ message: "Spec deleted" });
  } catch (err) {
    console.error("DELETE /api/ram-storage-config/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Categories CRUD
------------------------*/
// Get all categories (public)
app.get("/api/categories", authenticate, async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM categories ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/categories error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Public categories endpoint (no authentication) - useful for public site
app.get("/api/category", async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM categories ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/category error:", err);
    return res.status(500).json({ error: err.message });
  }
});
// Create category (authenticated)
app.post("/api/categories", authenticate, async (req, res) => {
  console.log(req.body);
  try {
    const { name, type, description } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const nameVal = String(name).trim();
    const typeVal = type ? String(type).trim() : null;
    const descVal = description ? String(description).trim() : null;

    // Only treat duplicate by name (case-insensitive). Multiple categories
    // can share the same product_type.
    const exists = await db.query(
      "SELECT id FROM categories WHERE LOWER(name) = LOWER($1) LIMIT 1",
      [nameVal],
    );
    if (exists.rows.length > 0)
      return res.status(409).json({ message: "Category already exists" });

    const r = await db.query(
      `INSERT INTO categories (name, product_type, description) VALUES ($1,$2,$3) RETURNING *`,
      [nameVal, typeVal, descVal],
    );
    return res.status(201).json({ data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/categories error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update category (authenticated)
app.put("/api/categories/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const { name, type, description } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const nameVal = String(name).trim();
    const typeVal = type ? String(type).trim() : null;
    const descVal = description ? String(description).trim() : null;

    // Only check for another category with the same name (case-insensitive)
    const dup = await db.query(
      "SELECT id FROM categories WHERE LOWER(name) = LOWER($1) AND id <> $2 LIMIT 1",
      [nameVal, id],
    );
    if (dup.rows.length > 0)
      return res
        .status(409)
        .json({ message: "Another category exists with same name/type" });

    const r = await db.query(
      `UPDATE categories SET name=$1, product_type=$2, description=$3 WHERE id=$4 RETURNING *`,
      [nameVal, typeVal, descVal, id],
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Category not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/categories/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete category (authenticated)
app.delete("/api/categories/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const r = await db.query("DELETE FROM categories WHERE id=$1", [id]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Category not found" });
    return res.json({ message: "Category deleted" });
  } catch (err) {
    console.error("DELETE /api/categories/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Online Stores CRUD
------------------------*/
// Get all online stores (public)
app.get("/api/public/online-stores", async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM online_stores ORDER BY id DESC");
    const sanitized = (r.rows || []).map((row) => {
      const { created_at, createdAt, ...rest } = row || {};
      return rest;
    });
    return res.json({ data: sanitized });
  } catch (err) {
    console.error("GET /api/public/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/online-stores", authenticate, async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM online_stores ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Create an online store (authenticated)
app.post("/api/online-stores", authenticate, async (req, res) => {
  try {
    const { name, logo, status } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });
    if (!logo) return res.status(400).json({ message: "logo is required" });

    const r = await db.query(
      `INSERT INTO online_stores (name, logo,status) VALUES ($1,$2,$3) RETURNING *`,
      [String(name).trim(), logo || null, status || "active"],
    );
    return res.status(201).json({ data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update an online store (authenticated)
app.put("/api/online-stores/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const { name, logo, website, description, status } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const r = await db.query(
      `UPDATE online_stores SET name=$1, logo=$2,  status=$3 WHERE id=$4 RETURNING *`,
      [String(name).trim(), logo || null, status || "active", id],
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/online-stores/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete an online store (authenticated)
app.delete("/api/online-stores/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const r = await db.query("DELETE FROM online_stores WHERE id = $1", [id]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ message: "Store deleted" });
  } catch (err) {
    console.error("DELETE /api/online-stores/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Patch status for an online store (authenticated)
app.patch("/api/online-stores/:id/status", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { status } = req.body || {};
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    if (!status) return res.status(400).json({ message: "status is required" });

    const r = await db.query(
      "UPDATE online_stores SET status = $1 WHERE id = $2 RETURNING *",
      [status, id],
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PATCH /api/online-stores/:id/status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a spec entry (authenticated)
app.delete("/api/specs/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const r = await db.query("DELETE FROM ram_storage_long  WHERE id = $1", [
      id,
    ]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });
    return res.json({ message: "Spec deleted" });
  } catch (err) {
    console.error("DELETE /api/specs/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a variant by id (will cascade-delete store prices via FK)
app.delete("/api/variant/:id", authenticate, async (req, res) => {
  try {
    const vid = Number(req.params.id);
    if (!vid || Number.isNaN(vid))
      return res.status(400).json({ message: "Invalid variant id" });

    const result = await db.query(
      "DELETE FROM product_variants WHERE id = $1 RETURNING product_id;",
      [vid],
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Variant not found" });

    return res.json({
      message: "Variant deleted",
      product_id: result.rows[0].product_id,
    });
  } catch (err) {
    console.error("DELETE variant error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a store price entry by id
app.delete("/api/storeprice/:id", authenticate, async (req, res) => {
  try {
    const pid = Number(req.params.id);
    if (!pid || Number.isNaN(pid))
      return res.status(400).json({ message: "Invalid price id" });

    const result = await db.query(
      "DELETE FROM variant_store_prices  WHERE id = $1 RETURNING variant_id;",
      [pid],
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Store price not found" });

    return res.json({
      message: "Store price deleted",
      variant_id: result.rows[0].variant_id,
    });
  } catch (err) {
    console.error("DELETE storeprice error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Variants & Store Price endpoints (single-item helpers) -  
------------------------*/

// Create a single variant store price
/* -----------------------
------------------------ */
app.get("/api/publish/status", async (req, res) => {
  try {
    const r = await db.query(
      "SELECT * FROM smartphone_publish  ORDER BY smartphone_id DESC",
    );
    return res.json({ publish: r.rows });
  } catch (err) {
    console.error("GET /api/publish/status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.patch("/api/products/:id/publish", authenticate, async (req, res) => {
  try {
    const productId = Number(req.params.id);
    if (!productId || Number.isNaN(productId)) {
      return res.status(400).json({ message: "Invalid product id" });
    }

    const { is_published } = req.body;
    if (typeof is_published !== "boolean") {
      return res.status(400).json({ message: "is_published must be boolean" });
    }

    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const product = await db.query("SELECT id FROM products WHERE id = $1", [
      productId,
    ]);

    if (product.rowCount === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    const result = await db.query(
      `
      INSERT INTO product_publish (product_id, is_published, published_by)
      VALUES ($1, $2, $3)
      ON CONFLICT (product_id)
      DO UPDATE SET
        is_published = EXCLUDED.is_published,
        published_by = EXCLUDED.published_by,
        updated_at = now()
      RETURNING *;
      `,
      [productId, is_published, req.user.id],
    );

    return res.json({
      message: "Publish status updated successfully",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("PATCH publish error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  CSV / XLSX Export & Import -  
------------------------*/
// Mount import routers
// Export (CSV) - authenticated
/* -----------------------
  Brands (categories)
------------------------*/
app.get("/api/brands", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        b.id,
        b.name,
        b.logo,
        MAX(to_jsonb(b)->>'website') AS website,
        b.description,
        b.category,
        b.status,
        b.created_at,
        COUNT(DISTINCT p.id)::int AS product_count,
        COUNT(DISTINCT p.id) FILTER (WHERE pp.is_published = true)::int AS published_products
      FROM brands b
      LEFT JOIN products p
        ON p.brand_id = b.id
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
      GROUP BY
        b.id,
        b.name,
        b.logo,
        b.description,
        b.category,
        b.status,
        b.created_at
      ORDER BY b.name ASC
    `);

    res.json({ brands: result.rows });
  } catch (err) {
    console.error("GET /api/brands error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Misc helper endpoints -  
------------------------*/
app.post("/api/brands", authenticate, async (req, res) => {
  try {
    const { name, logo, category, status, description, website } = req.body;
    if (!name) {
      return res.status(400).json({ message: "Brand name required" });
    }

    const r = await db.query(
      `
      INSERT INTO brands (name, logo, category, status, description, website)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (name) DO UPDATE
      SET logo = EXCLUDED.logo,
          description = EXCLUDED.description,
          website = EXCLUDED.website
      RETURNING *;
      `,
      [
        name,
        logo || null,
        category || null,
        status || "active",
        description || null,
        website || null,
      ],
    );

    res.json({ message: "Brand saved", data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/brands error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/brands/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid brand id" });

    const { name, logo, category, status, description, website } = req.body;
    const updates = [];
    const values = [];
    let idx = 1;

    for (const [k, v] of Object.entries({
      name,
      logo,
      category,
      status,
      description,
      website,
    })) {
      if (v !== undefined) {
        updates.push(`${k} = $${idx++}`);
        values.push(v);
      }
    }

    if (!updates.length) {
      return res.status(400).json({ message: "No fields to update" });
    }

    values.push(id);

    const r = await db.query(
      `UPDATE brands SET ${updates.join(", ")} WHERE id = $${idx} RETURNING *`,
      values,
    );

    if (!r.rows.length) {
      return res.status(404).json({ message: "Brand not found" });
    }

    res.json({ message: "Brand updated", data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/brands/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/brands/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid brand id" });

    const r = await db.query("DELETE FROM brands WHERE id = $1", [id]);

    if (!r.rowCount) {
      return res.status(404).json({ message: "Brand not found" });
    }

    res.json({ message: "Brand deleted" });
  } catch (err) {
    console.error("DELETE /api/brands/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

const buildAffiliatePlacementAdminRow = (row) => ({
  ...row,
  source_type: normalizeAffiliateSourceType(row?.source_type, "manual"),
  is_auto: normalizeAffiliateSourceType(row?.source_type, "manual") === "auto",
  total_clicks: Number(row?.total_clicks || 0),
  price:
    row?.price === null || row?.price === undefined ? null : Number(row.price),
  lifecycle_state: getAffiliatePlacementLifecycleState(row),
  effective_unpublish_at: resolveAffiliateEffectiveUnpublishAt(row),
  is_live: isAffiliatePlacementLive(row),
});

const readAffiliatePlacementAdminRowById = async (placementId) => {
  const result = await db.query(
    `
    SELECT
      ap.*,
      p.name AS product_name,
      p.product_type,
      bl.title AS blog_title,
      bl.slug AS blog_slug,
      br.name AS brand_name,
      COALESCE(clicks.total_clicks, 0)::int AS total_clicks,
      clicks.last_clicked_at
    FROM affiliate_placements ap
    LEFT JOIN products p
      ON p.id = ap.product_id
    LEFT JOIN blogs bl
      ON bl.id = ap.blog_id
    LEFT JOIN brands br
      ON br.id = ap.brand_id
    LEFT JOIN (
      SELECT
        placement_id,
        COUNT(*)::int AS total_clicks,
        MAX(created_at) AS last_clicked_at
      FROM affiliate_clicks
      GROUP BY placement_id
    ) clicks
      ON clicks.placement_id = ap.id
    WHERE ap.id = $1
    LIMIT 1
  `,
    [placementId],
  );

  return result.rows[0] ? buildAffiliatePlacementAdminRow(result.rows[0]) : null;
};

const ensureUniqueAffiliatePlacementSlug = async (seed, excludeId = null) => {
  const base = normalizeAffiliateSlug(seed, "affiliate-link");
  let candidate = base;
  let suffix = 2;

  while (true) {
    const query = excludeId
      ? `SELECT id FROM affiliate_placements WHERE slug = $1 AND id <> $2 LIMIT 1`
      : `SELECT id FROM affiliate_placements WHERE slug = $1 LIMIT 1`;
    const params = excludeId ? [candidate, excludeId] : [candidate];
    const existing = await db.query(query, params);

    if (!existing.rows.length) return candidate;
    candidate = `${base}-${suffix}`;
    suffix += 1;
  }
};

const readAffiliateBlogProductIds = async (blogId) => {
  if (!Number.isInteger(Number(blogId)) || Number(blogId) <= 0) return [];

  const result = await db.query(
    `
    SELECT product_id
    FROM (
      SELECT bp.product_id AS product_id, bp.position AS position, bp.id AS source_id
      FROM blog_products bp
      WHERE bp.blog_id = $1

      UNION ALL

      SELECT bl.product_id AS product_id, 999999 AS position, bl.id AS source_id
      FROM blogs bl
      WHERE bl.id = $1 AND bl.product_id IS NOT NULL
    ) source
    WHERE product_id IS NOT NULL
    ORDER BY position ASC, source_id ASC
  `,
    [Number(blogId)],
  );

  return Array.from(
    new Set(
      (result.rows || [])
        .map((row) => Number(row.product_id))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );
};

const readAffiliateProductContexts = async (productIds = []) => {
  const normalizedIds = normalizeAffiliateIdList(productIds);
  if (!normalizedIds.length) return new Map();

  const result = await db.query(
    `
    SELECT
      p.id AS product_id,
      p.product_type,
      p.brand_id,
      p.name AS product_name,
      b.name AS brand_name,
      LOWER(TRIM(COALESCE(s.category, ''))) AS category_name
    FROM products p
    LEFT JOIN brands b
      ON b.id = p.brand_id
    LEFT JOIN smartphones s
      ON s.product_id = p.id
    WHERE p.id = ANY($1::int[])
  `,
    [normalizedIds],
  );

  return new Map(
    (result.rows || []).map((row) => [
      Number(row.product_id),
      {
        product_id: Number(row.product_id),
        product_type: String(row.product_type || "").trim().toLowerCase(),
        brand_id: Number(row.brand_id) || null,
        product_name: row.product_name || "",
        brand_name: row.brand_name || "",
        category_name: String(row.category_name || "").trim().toLowerCase(),
      },
    ]),
  );
};

const readAutoAffiliateOfferSources = async ({
  productIds = [],
  latestLimit = 250,
} = {}) => {
  const normalizedIds = normalizeAffiliateIdList(productIds);
  const params = [];
  let whereSql = `
    WHERE p.product_type = 'smartphone'
      AND COALESCE(pp.is_published, false) = true
      AND sp.url IS NOT NULL
      AND BTRIM(sp.url) <> ''
  `;

  if (normalizedIds.length) {
    params.push(normalizedIds);
    whereSql += ` AND p.id = ANY($${params.length}::int[])`;
  }

  const limitSql =
    normalizedIds.length || !Number.isFinite(Number(latestLimit))
      ? ""
      : ` LIMIT ${Math.max(1, Math.min(500, Math.floor(Number(latestLimit))))}`;

  const result = await db.query(
    `
    WITH ranked_store_rows AS (
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        p.brand_id,
        p.created_at AS product_created_at,
        b.name AS brand_name,
        LOWER(TRIM(COALESCE(s.category, ''))) AS category_name,
        s.launch_date,
        v.id AS variant_id,
        sp.id AS store_price_id,
        sp.store_name,
        sp.price,
        sp.url,
        sp.offer_text,
        sp.sale_start_date,
        os.logo AS store_logo_url,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image_url,
        ROW_NUMBER() OVER (
          PARTITION BY p.id
          ORDER BY
            CASE
              WHEN sp.price IS NOT NULL AND sp.price > 0 THEN 0
              ELSE 1
            END ASC,
            sp.price ASC NULLS LAST,
            CASE
              WHEN sp.url IS NOT NULL AND BTRIM(sp.url) <> '' THEN 0
              ELSE 1
            END ASC,
            sp.sale_start_date DESC NULLS LAST,
            sp.id ASC
        ) AS row_rank
      FROM products p
      INNER JOIN smartphones s
        ON s.product_id = p.id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
      INNER JOIN product_variants v
        ON v.product_id = p.id
      INNER JOIN variant_store_prices sp
        ON sp.variant_id = v.id
      LEFT JOIN online_stores os
        ON LOWER(TRIM(os.name)) = LOWER(TRIM(sp.store_name))
      ${whereSql}
    )
    SELECT *
    FROM ranked_store_rows
    WHERE row_rank = 1
    ORDER BY COALESCE(sale_start_date, launch_date, product_created_at) DESC NULLS LAST, product_id DESC
    ${limitSql}
  `,
    params,
  );

  return result.rows || [];
};

const buildAutoAffiliatePlacementPayloadFromOffer = (offer = {}) => {
  const productId = Number(offer.product_id);
  const productName = String(offer.product_name || "").trim();
  const storeName = String(offer.store_name || "").trim();
  const targetUrl = String(offer.url || "").trim();
  const autoKey = `auto:product:${productId}:best-offer`;
  const publishAt =
    offer.sale_start_date ||
    offer.launch_date ||
    offer.product_created_at ||
    new Date().toISOString();
  const priceValue = Number(offer.price);
  const price =
    Number.isFinite(priceValue) && priceValue > 0 ? priceValue : null;
  const safeProductName = productName || `Product ${productId}`;
  const safeStoreName = storeName || "Online Store";

  return {
    name: `${safeProductName} Auto Offer`,
    slug: normalizeAffiliateSlug(`auto-product-${productId}-best-offer`),
    source_type: "auto",
    auto_key: autoKey,
    auto_variant_id: Number(offer.variant_id) || null,
    auto_store_price_id: Number(offer.store_price_id) || null,
    title: `${safeProductName} latest price on ${safeStoreName}`,
    description:
      String(offer.offer_text || "").trim() ||
      `Auto-generated from live store pricing data for ${safeProductName}.`,
    cta_text: "Check price",
    cta_subtext: "Auto-generated from store data",
    badge_text: "Latest Offer",
    disclosure_text: "Affiliate link",
    store_name: safeStoreName,
    store_logo_url: String(offer.store_logo_url || "").trim() || null,
    image_url: String(offer.image_url || "").trim() || null,
    destination_url: targetUrl || null,
    affiliate_url: targetUrl || null,
    price,
    currency_code: "INR",
    priority: 0,
    status: "published",
    publish_at: publishAt,
    unpublish_at: null,
    duration_days: null,
    allow_product_list: true,
    allow_product_detail: true,
    allow_news: true,
    scope_type: "product",
    product_id: productId || null,
    blog_id: null,
    brand_id: Number(offer.brand_id) || null,
    category_name: String(offer.category_name || "").trim() || null,
    list_slot: "product_card",
    detail_slot: "detail_highlight",
    news_slot: "inline_after_intro",
  };
};

const syncAutoAffiliatePlacementsForProducts = async ({
  productIds = [],
  includeLatest = false,
  latestLimit = 250,
} = {}) => {
  const normalizedIds = normalizeAffiliateIdList(productIds);
  if (!normalizedIds.length && !includeLatest) return [];

  const sourceRows = await readAutoAffiliateOfferSources({
    productIds: normalizedIds,
    latestLimit,
  });
  const sourceProductIds = Array.from(
    new Set(
      sourceRows
        .map((row) => Number(row.product_id))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );

  const staleProductIds = normalizedIds.filter(
    (productId) => !sourceProductIds.includes(productId),
  );
  if (staleProductIds.length) {
    await db.query(
      `
      DELETE FROM affiliate_placements
      WHERE source_type = 'auto'
        AND product_id = ANY($1::int[])
    `,
      [staleProductIds],
    );
  }

  const syncedIds = [];
  for (const row of sourceRows) {
    const payload = buildAutoAffiliatePlacementPayloadFromOffer(row);
    const upsertResult = await db.query(
      `
      INSERT INTO affiliate_placements (
        name,
        slug,
        source_type,
        auto_key,
        auto_variant_id,
        auto_store_price_id,
        title,
        description,
        cta_text,
        cta_subtext,
        badge_text,
        disclosure_text,
        store_name,
        store_logo_url,
        image_url,
        destination_url,
        affiliate_url,
        price,
        currency_code,
        priority,
        status,
        publish_at,
        unpublish_at,
        duration_days,
        allow_product_list,
        allow_product_detail,
        allow_news,
        scope_type,
        product_id,
        blog_id,
        brand_id,
        category_name,
        list_slot,
        detail_slot,
        news_slot,
        created_by,
        updated_by
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37
      )
      ON CONFLICT (auto_key)
      WHERE auto_key IS NOT NULL
      DO UPDATE SET
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        source_type = EXCLUDED.source_type,
        auto_variant_id = EXCLUDED.auto_variant_id,
        auto_store_price_id = EXCLUDED.auto_store_price_id,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        cta_text = EXCLUDED.cta_text,
        cta_subtext = EXCLUDED.cta_subtext,
        badge_text = EXCLUDED.badge_text,
        disclosure_text = EXCLUDED.disclosure_text,
        store_name = EXCLUDED.store_name,
        store_logo_url = EXCLUDED.store_logo_url,
        image_url = EXCLUDED.image_url,
        destination_url = EXCLUDED.destination_url,
        affiliate_url = EXCLUDED.affiliate_url,
        price = EXCLUDED.price,
        currency_code = EXCLUDED.currency_code,
        priority = EXCLUDED.priority,
        status = EXCLUDED.status,
        publish_at = EXCLUDED.publish_at,
        unpublish_at = EXCLUDED.unpublish_at,
        duration_days = EXCLUDED.duration_days,
        allow_product_list = EXCLUDED.allow_product_list,
        allow_product_detail = EXCLUDED.allow_product_detail,
        allow_news = EXCLUDED.allow_news,
        scope_type = EXCLUDED.scope_type,
        product_id = EXCLUDED.product_id,
        blog_id = EXCLUDED.blog_id,
        brand_id = EXCLUDED.brand_id,
        category_name = EXCLUDED.category_name,
        list_slot = EXCLUDED.list_slot,
        detail_slot = EXCLUDED.detail_slot,
        news_slot = EXCLUDED.news_slot,
        updated_by = NULL,
        updated_at = NOW()
      RETURNING id
    `,
      [
        payload.name,
        payload.slug,
        payload.source_type,
        payload.auto_key,
        payload.auto_variant_id,
        payload.auto_store_price_id,
        payload.title,
        payload.description,
        payload.cta_text,
        payload.cta_subtext,
        payload.badge_text,
        payload.disclosure_text,
        payload.store_name,
        payload.store_logo_url,
        payload.image_url,
        payload.destination_url,
        payload.affiliate_url,
        payload.price,
        payload.currency_code,
        payload.priority,
        payload.status,
        payload.publish_at,
        payload.unpublish_at,
        payload.duration_days,
        payload.allow_product_list,
        payload.allow_product_detail,
        payload.allow_news,
        payload.scope_type,
        payload.product_id,
        payload.blog_id,
        payload.brand_id,
        payload.category_name,
        payload.list_slot,
        payload.detail_slot,
        payload.news_slot,
        null,
        null,
      ],
    );
    if (upsertResult.rows[0]?.id) syncedIds.push(Number(upsertResult.rows[0].id));
  }

  return syncedIds;
};

const safeSyncAutoAffiliatePlacementsForProducts = async (
  options = {},
  label = "Affiliate auto-sync failed",
) => {
  try {
    const ids = await syncAutoAffiliatePlacementsForProducts(options);
    return { ok: true, ids };
  } catch (err) {
    console.error(label, err);
    return { ok: false, ids: [], error: err };
  }
};

const resolveAffiliateDuplicateTargetProductIds = async (payload = {}) => {
  const scopeType = normalizeAffiliateScopeType(payload.scope_type, "global");
  if (scopeType === "product") {
    return normalizeAffiliateIdList(payload.product_id ? [payload.product_id] : []);
  }
  if (scopeType === "blog") {
    return payload.blog_id ? readAffiliateBlogProductIds(payload.blog_id) : [];
  }
  return [];
};

const hasAffiliatePageOverlap = (left = {}, right = {}) =>
  Boolean(
    (Boolean(left.allow_product_list) && Boolean(right.allow_product_list)) ||
      (Boolean(left.allow_product_detail) &&
        Boolean(right.allow_product_detail)) ||
      (Boolean(left.allow_news) && Boolean(right.allow_news)),
  );

const findAutomaticAffiliateDuplicate = async (
  payload = {},
  { excludeId = null } = {},
) => {
  const targetProductIds = await resolveAffiliateDuplicateTargetProductIds(payload);
  if (!targetProductIds.length) return null;

  await safeSyncAutoAffiliatePlacementsForProducts(
    { productIds: targetProductIds },
    "Automatic affiliate duplicate sync failed:",
  );

  const comparisonUrl =
    normalizeAffiliateComparisonUrl(payload.affiliate_url) ||
    normalizeAffiliateComparisonUrl(payload.destination_url);
  const comparisonStore = normalizeAffiliateComparisonText(payload.store_name);

  const params = [targetProductIds];
  let excludeSql = "";
  if (excludeId) {
    params.push(excludeId);
    excludeSql = ` AND ap.id <> $${params.length}`;
  }

  const result = await db.query(
    `
    SELECT
      ap.*,
      p.name AS product_name,
      b.name AS brand_name
    FROM affiliate_placements ap
    LEFT JOIN products p
      ON p.id = ap.product_id
    LEFT JOIN brands b
      ON b.id = ap.brand_id
    WHERE ap.source_type = 'auto'
      AND ap.product_id = ANY($1::int[])
      ${excludeSql}
    ORDER BY ap.updated_at DESC, ap.id DESC
  `,
    params,
  );

  for (const row of result.rows || []) {
    if (!hasAffiliatePageOverlap(payload, row)) continue;

    const rowUrl =
      normalizeAffiliateComparisonUrl(row.affiliate_url) ||
      normalizeAffiliateComparisonUrl(row.destination_url);
    const rowStore = normalizeAffiliateComparisonText(row.store_name);
    const urlMatches = comparisonUrl && rowUrl && comparisonUrl === rowUrl;
    const storeMatches = comparisonStore && rowStore && comparisonStore === rowStore;

    if (urlMatches || storeMatches) {
      return {
        id: Number(row.id),
        source_type: row.source_type,
        product_id: Number(row.product_id) || null,
        product_name: row.product_name || "",
        brand_name: row.brand_name || "",
        store_name: row.store_name || "",
        title: row.title || row.name || "",
        affiliate_url: row.affiliate_url || "",
        destination_url: row.destination_url || "",
      };
    }
  }

  return null;
};

const resolveAffiliatePlacementMatches = (placement, context = {}) => {
  const scopeType = normalizeAffiliateScopeType(placement?.scope_type, "global");
  const blogId = Number(context.blogId) || null;
  const primaryProductId = Number(context.primaryProductId) || null;
  const productIds = normalizeAffiliateIdList(context.productIds);
  const productContextById = context.productContextById || new Map();
  const productContexts = productIds
    .map((productId) => productContextById.get(productId))
    .filter(Boolean);

  if (scopeType === "global") {
    return [
      {
        match_type: "global",
        matched_product_id:
          context.pageType === "product_list" ? null : primaryProductId,
        matched_blog_id: blogId,
      },
    ];
  }

  if (scopeType === "product") {
    const placementProductId = Number(placement?.product_id);
    if (!Number.isInteger(placementProductId) || placementProductId <= 0) {
      return [];
    }
    if (!productIds.includes(placementProductId)) return [];

    return [
      {
        match_type: "product",
        matched_product_id: placementProductId,
        matched_blog_id: blogId,
      },
    ];
  }

  if (scopeType === "blog") {
    const placementBlogId = Number(placement?.blog_id);
    if (!Number.isInteger(placementBlogId) || placementBlogId <= 0) return [];
    if (!blogId || placementBlogId !== blogId) return [];

    return [
      {
        match_type: "blog",
        matched_product_id: primaryProductId,
        matched_blog_id: blogId,
      },
    ];
  }

  if (scopeType === "brand") {
    const brandId = Number(placement?.brand_id);
    if (!Number.isInteger(brandId) || brandId <= 0) return [];

    return productContexts
      .filter((row) => Number(row.brand_id) === brandId)
      .map((row) => ({
        match_type: "brand",
        matched_product_id: row.product_id,
        matched_blog_id: blogId,
      }));
  }

  if (scopeType === "category") {
    const categoryName = String(placement?.category_name || "")
      .trim()
      .toLowerCase();
    if (!categoryName) return [];

    return productContexts
      .filter((row) => row.category_name === categoryName)
      .map((row) => ({
        match_type: "category",
        matched_product_id: row.product_id,
        matched_blog_id: blogId,
      }));
  }

  return [];
};

const serializeAffiliatePlacementForPublic = (
  placement,
  match,
  pageType,
  now = new Date(),
) => ({
  id: Number(placement.id),
  name: placement.name || "",
  slug: placement.slug || "",
  title: placement.title || placement.name || "",
  description: placement.description || "",
  cta_text: placement.cta_text || "View offer",
  cta_subtext: placement.cta_subtext || "",
  badge_text: placement.badge_text || "",
  disclosure_text: placement.disclosure_text || "Affiliate",
  store_name: placement.store_name || "",
  store_logo_url: placement.store_logo_url || "",
  image_url: placement.image_url || "",
  destination_url: placement.destination_url || "",
  affiliate_url: placement.affiliate_url || "",
  target_url: placement.affiliate_url || placement.destination_url || "",
  price:
    placement.price === null || placement.price === undefined
      ? null
      : Number(placement.price),
  currency_code: placement.currency_code || "INR",
  source_type: normalizeAffiliateSourceType(placement.source_type, "manual"),
  priority: Number(placement.priority || 0),
  scope_type: placement.scope_type || "global",
  slot: resolveAffiliateCurrentSlot(placement, pageType),
  match_type: match.match_type || "global",
  match_score: buildAffiliatePlacementScore(placement, match.match_type, now),
  matched_product_id: match.matched_product_id || null,
  matched_blog_id: match.matched_blog_id || null,
  total_clicks: Number(placement.total_clicks || 0),
  lifecycle_state: getAffiliatePlacementLifecycleState(placement, now),
  effective_unpublish_at: resolveAffiliateEffectiveUnpublishAt(placement),
  product_name: placement.product_name || "",
  product_type: placement.product_type || "",
  blog_title: placement.blog_title || "",
  blog_slug: placement.blog_slug || "",
  brand_name: placement.brand_name || "",
});

/* -----------------------
  Affiliate placements
------------------------*/
app.get("/api/admin/affiliate-placements/options", authenticate, async (req, res) => {
  try {
    const [productsRes, blogsRes, brandsRes, categoriesRes] = await Promise.all([
      db.query(`
        SELECT
          p.id,
          p.name,
          p.product_type,
          b.name AS brand_name,
          s.category,
          COALESCE(pp.is_published, false) AS is_published
        FROM products p
        INNER JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_publish pp
          ON pp.product_id = p.id
        ORDER BY COALESCE(pp.is_published, false) DESC, p.id DESC
        LIMIT 250
      `),
      db.query(`
        SELECT id, title, slug, status, updated_at
        FROM blogs
        WHERE status IN ('draft', 'published')
        ORDER BY updated_at DESC, id DESC
        LIMIT 150
      `),
      db.query(`
        SELECT id, name, logo
        FROM brands
        ORDER BY name ASC
      `),
      db.query(`
        SELECT id, name, product_type
        FROM categories
        ORDER BY name ASC
      `),
    ]);

    return res.json({
      products: productsRes.rows || [],
      blogs: blogsRes.rows || [],
      brands: brandsRes.rows || [],
      categories: categoriesRes.rows || [],
    });
  } catch (err) {
    console.error("GET /api/admin/affiliate-placements/options error:", err);
    return res.status(500).json({ message: "Failed to load affiliate options" });
  }
});

app.get("/api/admin/affiliate-placements", authenticate, async (req, res) => {
  try {
    const syncState = await safeSyncAutoAffiliatePlacementsForProducts(
      {
        includeLatest: true,
        latestLimit: 250,
      },
      "GET /api/admin/affiliate-placements auto-sync error:",
    );

    const result = await db.query(`
      SELECT
        ap.*,
        p.name AS product_name,
        p.product_type,
        bl.title AS blog_title,
        bl.slug AS blog_slug,
        br.name AS brand_name,
        COALESCE(clicks.total_clicks, 0)::int AS total_clicks,
        clicks.last_clicked_at
      FROM affiliate_placements ap
      LEFT JOIN products p
        ON p.id = ap.product_id
      LEFT JOIN blogs bl
        ON bl.id = ap.blog_id
      LEFT JOIN brands br
        ON br.id = ap.brand_id
      LEFT JOIN (
        SELECT
          placement_id,
          COUNT(*)::int AS total_clicks,
          MAX(created_at) AS last_clicked_at
        FROM affiliate_clicks
        GROUP BY placement_id
      ) clicks
        ON clicks.placement_id = ap.id
      ORDER BY ap.updated_at DESC, ap.id DESC
    `);

    return res.json({
      rows: (result.rows || []).map(buildAffiliatePlacementAdminRow),
      warnings: syncState.ok
        ? []
        : ["Automatic affiliate sync failed. Showing existing placements only."],
    });
  } catch (err) {
    console.error("GET /api/admin/affiliate-placements error:", err);
    return res
      .status(500)
      .json({ message: "Failed to fetch affiliate placements" });
  }
});

app.post("/api/admin/affiliate-placements", authenticate, async (req, res) => {
  try {
    const { payload, errors } = normalizeAffiliatePlacementInput(req.body || {});
    if (payload.scope_type !== "product") payload.product_id = null;
    if (payload.scope_type !== "blog") payload.blog_id = null;
    if (payload.scope_type !== "brand") payload.brand_id = null;
    if (payload.scope_type !== "category") payload.category_name = null;

    if (errors.length) {
      return res.status(400).json({ message: errors[0], errors });
    }

    const duplicateAuto = await findAutomaticAffiliateDuplicate(payload);
    if (duplicateAuto) {
      return res.status(409).json({
        message:
          "An automatic affiliate placement already exists for this product/store combination.",
        duplicate: duplicateAuto,
      });
    }

    const slug = await ensureUniqueAffiliatePlacementSlug(
      payload.slug || payload.name || payload.title || "affiliate-link",
    );

    const result = await db.query(
      `
      INSERT INTO affiliate_placements (
        name,
        slug,
        title,
        description,
        cta_text,
        cta_subtext,
        badge_text,
        disclosure_text,
        store_name,
        store_logo_url,
        image_url,
        destination_url,
        affiliate_url,
        price,
        currency_code,
        priority,
        status,
        publish_at,
        unpublish_at,
        duration_days,
        allow_product_list,
        allow_product_detail,
        allow_news,
        scope_type,
        product_id,
        blog_id,
        brand_id,
        category_name,
        list_slot,
        detail_slot,
        news_slot,
        created_by,
        updated_by
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33
      )
      RETURNING *
    `,
      [
        payload.name,
        slug,
        payload.title,
        payload.description,
        payload.cta_text,
        payload.cta_subtext,
        payload.badge_text,
        payload.disclosure_text,
        payload.store_name,
        payload.store_logo_url,
        payload.image_url,
        payload.destination_url,
        payload.affiliate_url,
        payload.price,
        payload.currency_code,
        payload.priority,
        payload.status,
        payload.publish_at,
        payload.unpublish_at,
        payload.duration_days,
        payload.allow_product_list,
        payload.allow_product_detail,
        payload.allow_news,
        payload.scope_type,
        payload.product_id,
        payload.blog_id,
        payload.brand_id,
        payload.category_name,
        payload.list_slot,
        payload.detail_slot,
        payload.news_slot,
        req.user?.id ?? null,
        req.user?.id ?? null,
      ],
    );

    const adminRow = await readAffiliatePlacementAdminRowById(result.rows[0].id);

    return res.status(201).json({
      message: "Affiliate placement created",
      data: adminRow || buildAffiliatePlacementAdminRow(result.rows[0]),
    });
  } catch (err) {
    console.error("POST /api/admin/affiliate-placements error:", err);
    return res
      .status(500)
      .json({ message: "Failed to create affiliate placement" });
  }
});

app.put("/api/admin/affiliate-placements/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid affiliate placement id" });
    }

    const existing = await db.query(
      `SELECT * FROM affiliate_placements WHERE id = $1 LIMIT 1`,
      [id],
    );
    if (!existing.rows.length) {
      return res.status(404).json({ message: "Affiliate placement not found" });
    }
    if (normalizeAffiliateSourceType(existing.rows[0].source_type) === "auto") {
      return res.status(403).json({
        message:
          "Automatic affiliate placements are generated from product store data and cannot be edited manually.",
      });
    }

    const { payload, errors } = normalizeAffiliatePlacementInput(req.body || {}, {
      existing: existing.rows[0],
    });
    if (payload.scope_type !== "product") payload.product_id = null;
    if (payload.scope_type !== "blog") payload.blog_id = null;
    if (payload.scope_type !== "brand") payload.brand_id = null;
    if (payload.scope_type !== "category") payload.category_name = null;

    if (errors.length) {
      return res.status(400).json({ message: errors[0], errors });
    }

    const duplicateAuto = await findAutomaticAffiliateDuplicate(payload, {
      excludeId: id,
    });
    if (duplicateAuto) {
      return res.status(409).json({
        message:
          "An automatic affiliate placement already exists for this product/store combination.",
        duplicate: duplicateAuto,
      });
    }

    const slug = await ensureUniqueAffiliatePlacementSlug(
      payload.slug || payload.name || payload.title || existing.rows[0].slug,
      id,
    );

    const result = await db.query(
      `
      UPDATE affiliate_placements
      SET
        name = $1,
        slug = $2,
        title = $3,
        description = $4,
        cta_text = $5,
        cta_subtext = $6,
        badge_text = $7,
        disclosure_text = $8,
        store_name = $9,
        store_logo_url = $10,
        image_url = $11,
        destination_url = $12,
        affiliate_url = $13,
        price = $14,
        currency_code = $15,
        priority = $16,
        status = $17,
        publish_at = $18,
        unpublish_at = $19,
        duration_days = $20,
        allow_product_list = $21,
        allow_product_detail = $22,
        allow_news = $23,
        scope_type = $24,
        product_id = $25,
        blog_id = $26,
        brand_id = $27,
        category_name = $28,
        list_slot = $29,
        detail_slot = $30,
        news_slot = $31,
        updated_by = $32,
        updated_at = NOW()
      WHERE id = $33
      RETURNING *
    `,
      [
        payload.name,
        slug,
        payload.title,
        payload.description,
        payload.cta_text,
        payload.cta_subtext,
        payload.badge_text,
        payload.disclosure_text,
        payload.store_name,
        payload.store_logo_url,
        payload.image_url,
        payload.destination_url,
        payload.affiliate_url,
        payload.price,
        payload.currency_code,
        payload.priority,
        payload.status,
        payload.publish_at,
        payload.unpublish_at,
        payload.duration_days,
        payload.allow_product_list,
        payload.allow_product_detail,
        payload.allow_news,
        payload.scope_type,
        payload.product_id,
        payload.blog_id,
        payload.brand_id,
        payload.category_name,
        payload.list_slot,
        payload.detail_slot,
        payload.news_slot,
        req.user?.id ?? null,
        id,
      ],
    );

    const adminRow = await readAffiliatePlacementAdminRowById(result.rows[0].id);

    return res.json({
      message: "Affiliate placement updated",
      data: adminRow || buildAffiliatePlacementAdminRow(result.rows[0]),
    });
  } catch (err) {
    console.error("PUT /api/admin/affiliate-placements/:id error:", err);
    return res
      .status(500)
      .json({ message: "Failed to update affiliate placement" });
  }
});

app.delete("/api/admin/affiliate-placements/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid affiliate placement id" });
    }

    const existing = await db.query(
      `SELECT id, source_type FROM affiliate_placements WHERE id = $1 LIMIT 1`,
      [id],
    );
    if (!existing.rows.length) {
      return res.status(404).json({ message: "Affiliate placement not found" });
    }
    if (normalizeAffiliateSourceType(existing.rows[0].source_type) === "auto") {
      return res.status(403).json({
        message:
          "Automatic affiliate placements are generated from product store data and cannot be deleted manually.",
      });
    }

    const result = await db.query(
      `DELETE FROM affiliate_placements WHERE id = $1`,
      [id],
    );

    return res.json({ message: "Affiliate placement deleted" });
  } catch (err) {
    console.error("DELETE /api/admin/affiliate-placements/:id error:", err);
    return res
      .status(500)
      .json({ message: "Failed to delete affiliate placement" });
  }
});

app.get("/api/public/affiliate-placements", async (req, res) => {
  try {
    const pageType = normalizeAffiliatePageType(
      req.query.pageType ?? req.query.page_type,
    );
    if (!pageType) {
      return res.status(400).json({ message: "pageType is required" });
    }

    const pageColumn =
      pageType === "product_list"
        ? "allow_product_list"
        : pageType === "product_detail"
          ? "allow_product_detail"
          : "allow_news";
    const primaryProductId = toPositiveInt(
      req.query.productId ?? req.query.product_id,
      null,
    );
    const blogId = toPositiveInt(req.query.blogId ?? req.query.blog_id, null);
    const requestedProductIds = Array.from(
      new Set([
        ...normalizeAffiliateIdList(
          req.query.productIds ?? req.query.product_ids ?? [],
        ),
        ...(primaryProductId ? [primaryProductId] : []),
      ]),
    );
    const blogProductIds = blogId ? await readAffiliateBlogProductIds(blogId) : [];
    const candidateProductIds = Array.from(
      new Set([...requestedProductIds, ...blogProductIds]),
    );
    const productContextById = await readAffiliateProductContexts(
      candidateProductIds,
    );
    const context = {
      pageType,
      blogId,
      primaryProductId: primaryProductId || candidateProductIds[0] || null,
      productIds: candidateProductIds,
      productContextById,
    };
    const now = new Date();

    if (candidateProductIds.length) {
      await safeSyncAutoAffiliatePlacementsForProducts(
        {
          productIds: candidateProductIds,
        },
        "GET /api/public/affiliate-placements auto-sync error:",
      );
    }

    const result = await db.query(
      `
      SELECT
        ap.*,
        p.name AS product_name,
        p.product_type,
        bl.title AS blog_title,
        bl.slug AS blog_slug,
        br.name AS brand_name,
        COALESCE(clicks.total_clicks, 0)::int AS total_clicks
      FROM affiliate_placements ap
      LEFT JOIN products p
        ON p.id = ap.product_id
      LEFT JOIN blogs bl
        ON bl.id = ap.blog_id
      LEFT JOIN brands br
        ON br.id = ap.brand_id
      LEFT JOIN (
        SELECT placement_id, COUNT(*)::int AS total_clicks
        FROM affiliate_clicks
        GROUP BY placement_id
      ) clicks
        ON clicks.placement_id = ap.id
      WHERE ap.status = 'published'
        AND ap.${pageColumn} = true
      ORDER BY ap.priority DESC, ap.updated_at DESC, ap.id DESC
    `,
    );

    const placements = [];
    for (const row of result.rows || []) {
      if (!isAffiliatePlacementLive(row, now)) continue;
      const matches = resolveAffiliatePlacementMatches(row, context);
      for (const match of matches) {
        placements.push(
          serializeAffiliatePlacementForPublic(row, match, pageType, now),
        );
      }
    }

    placements.sort((left, right) => {
      if (right.match_score !== left.match_score) {
        return right.match_score - left.match_score;
      }
      return Number(right.id || 0) - Number(left.id || 0);
    });

    return res.json({
      pageType,
      placements,
    });
  } catch (err) {
    console.error("GET /api/public/affiliate-placements error:", err);
    return res
      .status(500)
      .json({ message: "Failed to fetch affiliate placements" });
  }
});

app.get("/api/public/affiliate-redirect/:id", async (req, res) => {
  try {
    const placementId = Number(req.params.id);
    if (!Number.isInteger(placementId) || placementId <= 0) {
      return res.status(400).send("Invalid affiliate placement");
    }

    const result = await db.query(
      `SELECT * FROM affiliate_placements WHERE id = $1 LIMIT 1`,
      [placementId],
    );
    const placement = result.rows[0];
    if (!placement) {
      return res.status(404).send("Affiliate placement not found");
    }

    const targetUrl =
      String(placement.affiliate_url || "").trim() ||
      String(placement.destination_url || "").trim();
    if (!targetUrl) {
      return res.status(404).send("Affiliate destination not available");
    }

    const pageType =
      normalizeAffiliatePageType(req.query.pageType ?? req.query.page_type) ||
      null;
    const slot = toNullableTrimmedText(req.query.slot, 80);
    const productId = toPositiveInt(
      req.query.productId ?? req.query.product_id,
      null,
    );
    const blogId = toPositiveInt(req.query.blogId ?? req.query.blog_id, null);
    const deviceType = normalizeAffiliateDeviceType(
      req.query.deviceType ?? req.query.device_type,
      req.get("user-agent") || "",
    );

    await db.query(
      `
      INSERT INTO affiliate_clicks (
        placement_id,
        page_type,
        slot,
        product_id,
        blog_id,
        device_type,
        referer,
        user_agent,
        ip_address,
        target_url,
        was_live
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    `,
      [
        placementId,
        pageType,
        slot,
        productId,
        blogId,
        deviceType,
        req.get("referer") || null,
        req.get("user-agent") || null,
        req.ip || null,
        targetUrl,
        isAffiliatePlacementLive(placement),
      ],
    );

    return res.redirect(302, targetUrl);
  } catch (err) {
    console.error("GET /api/public/affiliate-redirect/:id error:", err);
    return res.status(500).send("Unable to open affiliate destination");
  }
});

/* -----------------------
  Banners (marketing)
------------------------*/
app.get("/api/admin/banners", authenticate, async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        b.*,
        (
          b.is_published = true
          AND (b.start_at IS NULL OR b.start_at <= NOW())
          AND (b.end_at IS NULL OR b.end_at >= NOW())
        ) AS is_active,
        (b.end_at IS NOT NULL AND b.end_at < NOW()) AS is_expired
      FROM banners b
      ORDER BY b.placement ASC, b.priority DESC, b.created_at DESC
    `);

    return res.json({ banners: result.rows });
  } catch (err) {
    console.error("GET /api/admin/banners error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.post("/api/admin/banners", authenticate, async (req, res) => {
  try {
    const body = req.body || {};
    const placement = String(body.placement || "").trim();
    const media_url = String(body.media_url || body.mediaUrl || "").trim();

    if (!placement) {
      return res.status(400).json({ message: "placement is required" });
    }
    if (!media_url) {
      return res.status(400).json({ message: "media_url is required" });
    }

    const allowedPlacements = new Set([
      "top_leaderboard",
      "right_sidebar",
      "in_content",
      "footer_leaderboard",
      "mobile_sticky",
    ]);
    if (!allowedPlacements.has(placement)) {
      return res.status(400).json({ message: "Invalid placement" });
    }

    const parseBool = (v) => {
      if (v === true || v === false) return v;
      if (v === 1 || v === 0) return Boolean(v);
      if (v === null || v === undefined) return false;
      const s = String(v).trim().toLowerCase();
      if (["true", "1", "yes", "y", "on"].includes(s)) return true;
      if (["false", "0", "no", "n", "off"].includes(s)) return false;
      return false;
    };

    const title =
      body.title === null || body.title === undefined
        ? null
        : String(body.title).trim();
    const size_desktop =
      body.size_desktop ?? body.sizeDesktop ?? body.desktop_size ?? null;
    const size_tablet =
      body.size_tablet ?? body.sizeTablet ?? body.tablet_size ?? null;
    const size_mobile =
      body.size_mobile ?? body.sizeMobile ?? body.mobile_size ?? null;
    const media_type = body.media_type ?? body.mediaType ?? body.format ?? null;
    const link_url = body.link_url ?? body.linkUrl ?? body.url ?? null;
    const start_at = body.start_at ?? body.startAt ?? null;
    const end_at = body.end_at ?? body.endAt ?? null;
    const is_published = parseBool(
      body.is_published ?? body.isPublished ?? body.publish,
    );
    const priorityRaw = Number(body.priority ?? 0);
    const priority = Number.isFinite(priorityRaw)
      ? Math.max(0, Math.floor(priorityRaw))
      : 0;

    if (start_at && end_at) {
      const startTime = new Date(start_at);
      const endTime = new Date(end_at);
      if (
        !Number.isNaN(startTime.getTime()) &&
        !Number.isNaN(endTime.getTime())
      ) {
        if (endTime < startTime) {
          return res
            .status(400)
            .json({ message: "end_at must be after start_at" });
        }
      }
    }

    const result = await db.query(
      `
      INSERT INTO banners (
        title,
        placement,
        size_desktop,
        size_tablet,
        size_mobile,
        media_url,
        media_type,
        link_url,
        start_at,
        end_at,
        is_published,
        priority
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      RETURNING *;
      `,
      [
        title,
        placement,
        size_desktop,
        size_tablet,
        size_mobile,
        media_url,
        media_type,
        link_url,
        start_at,
        end_at,
        is_published,
        priority,
      ],
    );

    return res.json({ message: "Banner created", data: result.rows[0] });
  } catch (err) {
    console.error("POST /api/admin/banners error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.put("/api/admin/banners/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid banner id" });

    const body = req.body || {};
    const updates = [];
    const values = [];
    let idx = 1;

    const allowedPlacements = new Set([
      "top_leaderboard",
      "right_sidebar",
      "in_content",
      "footer_leaderboard",
      "mobile_sticky",
    ]);

    const pushUpdate = (field, value) => {
      updates.push(`${field} = $${idx++}`);
      values.push(value);
    };

    if (body.title !== undefined) pushUpdate("title", body.title);
    if (body.placement !== undefined) {
      const placement = String(body.placement || "").trim();
      if (!allowedPlacements.has(placement)) {
        return res.status(400).json({ message: "Invalid placement" });
      }
      pushUpdate("placement", placement);
    }
    if (body.size_desktop !== undefined)
      pushUpdate("size_desktop", body.size_desktop);
    if (body.size_tablet !== undefined)
      pushUpdate("size_tablet", body.size_tablet);
    if (body.size_mobile !== undefined)
      pushUpdate("size_mobile", body.size_mobile);
    if (body.media_url !== undefined) pushUpdate("media_url", body.media_url);
    if (body.media_type !== undefined)
      pushUpdate("media_type", body.media_type);
    if (body.link_url !== undefined) pushUpdate("link_url", body.link_url);
    if (body.start_at !== undefined) pushUpdate("start_at", body.start_at);
    if (body.end_at !== undefined) pushUpdate("end_at", body.end_at);
    if (body.priority !== undefined) {
      const priorityRaw = Number(body.priority ?? 0);
      const priority = Number.isFinite(priorityRaw)
        ? Math.max(0, Math.floor(priorityRaw))
        : 0;
      pushUpdate("priority", priority);
    }
    if (body.is_published !== undefined) {
      const v = body.is_published;
      const parseBool = (val) => {
        if (val === true || val === false) return val;
        if (val === 1 || val === 0) return Boolean(val);
        if (val === null || val === undefined) return false;
        const s = String(val).trim().toLowerCase();
        if (["true", "1", "yes", "y", "on"].includes(s)) return true;
        if (["false", "0", "no", "n", "off"].includes(s)) return false;
        return false;
      };
      pushUpdate("is_published", parseBool(v));
    }

    if (!updates.length) {
      return res.status(400).json({ message: "No fields to update" });
    }

    updates.push(`updated_at = NOW()`);
    values.push(id);

    const result = await db.query(
      `UPDATE banners SET ${updates.join(", ")} WHERE id = $${idx} RETURNING *`,
      values,
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Banner not found" });
    }

    return res.json({ message: "Banner updated", data: result.rows[0] });
  } catch (err) {
    console.error("PUT /api/admin/banners/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.delete("/api/admin/banners/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid banner id" });

    const result = await db.query("DELETE FROM banners WHERE id = $1", [id]);
    if (!result.rowCount) {
      return res.status(404).json({ message: "Banner not found" });
    }

    return res.json({ message: "Banner deleted" });
  } catch (err) {
    console.error("DELETE /api/admin/banners/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/public/banners", async (req, res) => {
  try {
    const placement = req.query.placement
      ? String(req.query.placement).trim()
      : null;
    const limitRaw = Number(req.query.limit ?? 0);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : null;

    const params = [];
    let where = `
      WHERE b.is_published = true
        AND (b.start_at IS NULL OR b.start_at <= NOW())
        AND (b.end_at IS NULL OR b.end_at >= NOW())
    `;
    if (placement) {
      params.push(placement);
      where += ` AND b.placement = $${params.length}`;
    }

    const limitSql = limit ? ` LIMIT ${Math.min(50, Math.floor(limit))}` : "";

    const result = await db.query(
      `
      SELECT
        b.id,
        b.title,
        b.placement,
        b.size_desktop,
        b.size_tablet,
        b.size_mobile,
        b.media_url,
        b.media_type,
        b.link_url,
        b.start_at,
        b.end_at,
        b.priority
      FROM banners b
      ${where}
      ORDER BY b.priority DESC, b.created_at DESC
      ${limitSql}
      `,
      params,
    );

    return res.json({ banners: result.rows });
  } catch (err) {
    console.error("GET /api/public/banners error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Reports
------------------------*/
// Products grouped by category (smartphone categories + totals by product_type)
app.get("/api/reports/products-by-category", authenticate, async (req, res) => {
  try {
    const catRes = await db.query(`
      SELECT COALESCE(s.category, 'Uncategorized') AS category, COUNT(*) AS count
      FROM smartphones s
      GROUP BY COALESCE(s.category, 'Uncategorized')
      ORDER BY count DESC
    `);

    const totalsRes = await db.query(`
      SELECT p.product_type, COUNT(*) AS count
      FROM products p
      GROUP BY p.product_type
      ORDER BY p.product_type
    `);

    return res.json({ categories: catRes.rows, totals: totalsRes.rows });
  } catch (err) {
    console.error("GET /api/reports/products-by-category error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Launch timing analytics across device families
app.get("/api/reports/launch-timing", authenticate, async (req, res) => {
  try {
    const laptopBrandSql = `
      COALESCE(
        b.name,
        NULLIF(TRIM(l.meta->>'brand'), ''),
        NULLIF(TRIM(l.spec_sections#>>'{basic_info_json,brand_name}'), ''),
        NULLIF(TRIM(l.spec_sections#>>'{basic_info_json,brand}'), ''),
        'Unknown'
      )
    `;

    const laptopCategorySql = `
      COALESCE(
        NULLIF(TRIM(l.meta->>'category'), ''),
        NULLIF(TRIM(l.spec_sections#>>'{basic_info_json,category}'), ''),
        'Laptop'
      )
    `;

    const laptopLaunchDateSql = `
      COALESCE(
        CASE
          WHEN NULLIF(TRIM(l.meta->>'launch_date'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (l.meta->>'launch_date')::date
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(l.spec_sections#>>'{basic_info_json,launch_date}'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (l.spec_sections#>>'{basic_info_json,launch_date}')::date
          ELSE NULL
        END,
        l.created_at::date,
        p.created_at::date
      )
    `;

    const tvCategorySql = `
      COALESCE(
        NULLIF(TRIM(t.category), ''),
        NULLIF(TRIM(t.basic_info_json->>'category'), ''),
        'TV'
      )
    `;

    const tvLaunchDateSql = `
      COALESCE(
        CASE
          WHEN NULLIF(TRIM(t.basic_info_json->>'launch_date'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (t.basic_info_json->>'launch_date')::date
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(t.product_details_json->>'launch_date'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (t.product_details_json->>'launch_date')::date
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(t.product_details_json->>'launch_year'), '') ~ '^[0-9]{4}$'
            THEN make_date((t.product_details_json->>'launch_year')::int, 1, 1)
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(t.basic_info_json->>'launch_year'), '') ~ '^[0-9]{4}$'
            THEN make_date((t.basic_info_json->>'launch_year')::int, 1, 1)
          ELSE NULL
        END,
        t.created_at::date,
        p.created_at::date
      )
    `;

    const [smartphoneRes, laptopRes, tvRes] = await Promise.all([
      db.query(`
        SELECT
          p.id AS product_id,
          p.product_type,
          p.name AS product_name,
          COALESCE(b.name, NULLIF(TRIM(s.brand), ''), 'Unknown') AS brand_name,
          COALESCE(NULLIF(TRIM(s.category), ''), 'Uncategorized') AS category,
          s.launch_date,
          MIN(sp.sale_start_date) AS sale_start_date
        FROM products p
        INNER JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_variants v
          ON v.product_id = p.id
        LEFT JOIN variant_store_prices sp
          ON sp.variant_id = v.id
        WHERE p.product_type = 'smartphone'
        GROUP BY
          p.id,
          p.product_type,
          p.name,
          b.name,
          s.brand,
          s.category,
          s.launch_date
      `),
      db.query(`
        SELECT
          p.id AS product_id,
          p.product_type,
          p.name AS product_name,
          ${laptopBrandSql} AS brand_name,
          ${laptopCategorySql} AS category,
          ${laptopLaunchDateSql} AS launch_date,
          MIN(sp.sale_start_date) AS sale_start_date
        FROM products p
        INNER JOIN laptop l
          ON l.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_variants v
          ON v.product_id = p.id
        LEFT JOIN variant_store_prices sp
          ON sp.variant_id = v.id
        WHERE p.product_type = 'laptop'
        GROUP BY
          p.id,
          p.product_type,
          p.name,
          ${laptopBrandSql},
          ${laptopCategorySql},
          ${laptopLaunchDateSql}
      `),
      db.query(`
        SELECT
          p.id AS product_id,
          p.product_type,
          p.name AS product_name,
          COALESCE(b.name, 'Unknown') AS brand_name,
          ${tvCategorySql} AS category,
          ${tvLaunchDateSql} AS launch_date,
          MIN(sp.sale_start_date) AS sale_start_date
        FROM products p
        INNER JOIN tvs t
          ON t.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_variants v
          ON v.product_id = p.id
        LEFT JOIN variant_store_prices sp
          ON sp.variant_id = v.id
        WHERE p.product_type = 'tv'
        GROUP BY
          p.id,
          p.product_type,
          p.name,
          b.name,
          ${tvCategorySql},
          ${tvLaunchDateSql}
      `),
    ]);

    const normalizeTimingRow = (row) => {
      const launchDate = normalizeDateOnlyInput(row?.launch_date);
      const saleStartDate = normalizeDateOnlyInput(row?.sale_start_date);

      return {
        product_id: Number(row?.product_id),
        product_type: String(row?.product_type || ""),
        product_name: String(row?.product_name || "Unnamed device"),
        brand_name: row?.brand_name ? String(row.brand_name) : null,
        category: row?.category ? String(row.category) : "Uncategorized",
        launch_date: launchDate,
        sale_start_date: saleStartDate,
        sale_gap_days: diffDateOnlyDays(launchDate, saleStartDate),
      };
    };

    const devices = [
      ...(smartphoneRes.rows || []),
      ...(laptopRes.rows || []),
      ...(tvRes.rows || []),
    ]
      .map(normalizeTimingRow)
      .sort((left, right) => {
        const launchDiff =
          (toDateOnlyUtcMillis(right?.launch_date) || 0) -
          (toDateOnlyUtcMillis(left?.launch_date) || 0);
        if (launchDiff !== 0) return launchDiff;
        return Number(right?.product_id || 0) - Number(left?.product_id || 0);
      });

    return res.json({
      devices,
      totals: {
        total_devices: devices.length,
        devices_with_sale_date: devices.filter((item) => item.sale_start_date)
          .length,
        devices_with_gap: devices.filter((item) =>
          Number.isFinite(item.sale_gap_days),
        ).length,
      },
    });
  } catch (err) {
    console.error("GET /api/reports/launch-timing error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Publish status grouped by product_type
app.get("/api/reports/publish-status", authenticate, async (req, res) => {
  try {
    const r = await db.query(`
      SELECT p.product_type,
             COUNT(*) AS total,
             COALESCE(SUM(CASE WHEN pp.is_published THEN 1 ELSE 0 END),0) AS published,
             COALESCE(SUM(CASE WHEN pp.is_published THEN 0 ELSE 1 END),0) AS drafts
      FROM products p
      LEFT JOIN product_publish pp ON pp.product_id = p.id
      GROUP BY p.product_type
      ORDER BY p.product_type
    `);

    return res.json({ publish_by_type: r.rows });
  } catch (err) {
    console.error("GET /api/reports/publish-status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Number of published products by user
app.get("/api/reports/published-by-user", authenticate, async (req, res) => {
  try {
    const r = await db.query(`
      SELECT u.id, u.user_name, u.email, COUNT(*) AS published_count
      FROM product_publish pp
      JOIN "user" u ON u.id = pp.published_by
      WHERE pp.is_published = true
      GROUP BY u.id, u.user_name, u.email
      ORDER BY published_count DESC
      LIMIT 100
    `);

    return res.json({ published_by_user: r.rows });
  } catch (err) {
    console.error("GET /api/reports/published-by-user error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Recent publish activity (who published what)
app.get(
  "/api/reports/recent-publish-activity",
  authenticate,
  async (req, res) => {
    try {
      const r = await db.query(`
      SELECT pp.product_id, pp.is_published, pp.published_by, pp.updated_at,
             p.name AS product_name, p.product_type, u.user_name, u.email
      FROM product_publish pp
      LEFT JOIN products p ON p.id = pp.product_id
      LEFT JOIN "user" u ON u.id = pp.published_by
      ORDER BY pp.updated_at DESC
      LIMIT 100
    `);

      return res.json({ recent_publish_activity: r.rows });
    } catch (err) {
      console.error("GET /api/reports/recent-publish-activity error:", err);
      return res.status(500).json({ error: err.message });
    }
  },
);

// Record a product view (public)

app.post("/api/public/product/:id/view", async (req, res) => {
  const rawId = req.params.id;
  const productId = Number(rawId);

  if (!rawId || !Number.isInteger(productId) || productId <= 0) {
    console.warn("Invalid product id in view request:", rawId);
    return res.status(400).json({ message: "Invalid product id" });
  }

  console.log("Recording view for product id:", productId);
  try {
    const b = req.body || {};
    const visitorIdRaw =
      b.visitor_id ?? b.visitorId ?? req.headers["x-visitor-id"] ?? "";
    const ipRaw =
      (req.headers["x-forwarded-for"] || "").split(",")[0].trim() ||
      req.ip ||
      "";
    const userAgent = req.headers["user-agent"] || "";

    const keySource = visitorIdRaw
      ? `vid:${String(visitorIdRaw).trim()}`
      : `ip:${String(ipRaw)}|ua:${String(userAgent)}`;

    const visitor_key = crypto
      .createHash("sha256")
      .update(keySource)
      .digest("hex")
      .slice(0, 32);

    try {
      await db.query(
        `INSERT INTO product_views (product_id, visitor_key) VALUES ($1, $2)`,
        [productId, visitor_key],
      );
    } catch (err) {
      // Backward-compatible fallback if the DB hasn't added visitor_key yet
      if (err && err.code === "42703") {
        await db.query(`INSERT INTO product_views (product_id) VALUES ($1)`, [
          productId,
        ]);
      } else {
        throw err;
      }
    }
    return res.json({ message: "View recorded" });
  } catch (err) {
    console.error("Error recording product view:", err);
    return res.status(500).json({ message: "Failed to record view" });
  }
});

app.post("/api/public/page-engagement", async (req, res) => {
  try {
    const body = req.body || {};
    const productId = Number(body.product_id ?? body.productId);
    const durationRaw = Number(body.duration_ms ?? body.durationMs ?? 0);
    const durationMs = Number.isFinite(durationRaw)
      ? Math.min(30 * 60 * 1000, Math.max(0, Math.floor(durationRaw)))
      : 0;
    const pagePath =
      cleanText(body.page_path ?? body.pagePath ?? "", 255) || null;
    const source = cleanToken(body.source, 48) || "detail";

    if (!Number.isInteger(productId) || productId <= 0) {
      return res.status(400).json({ message: "Invalid product id" });
    }

    await db.query(
      `
      INSERT INTO page_engagement_events (
        product_id,
        page_path,
        source,
        duration_ms
      )
      VALUES ($1, $2, $3, $4)
      `,
      [productId, pagePath, source, durationMs],
    );

    return res.json({ success: true });
  } catch (err) {
    console.error("POST /api/public/page-engagement error:", err);
    return res.status(500).json({ success: false });
  }
});

// Recompute Hook Dynamic Score (admin) - use external cron to call this endpoint,
// or run `npm run recompute:hookscore` on a schedule.
app.post("/api/admin/hook-score/recompute", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const smartphones = await recomputeProductDynamicScoreSmartphones(db);
    const laptops = await recomputeProductDynamicScoreLaptops(db);
    const tvs = await recomputeProductDynamicScoreTVs(db);

    return res.json({
      ok: true,
      updated:
        (smartphones.updated || 0) +
        (laptops.updated || 0) +
        (tvs.updated || 0),
      results: {
        smartphones,
        laptops,
        tvs,
      },
    });
  } catch (err) {
    console.error("POST /api/admin/hook-score/recompute error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Recompute Trending Scores (admin) - run on a schedule via cron/CI or call this endpoint.
app.post("/api/admin/trending/recompute", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const result = await recomputeProductTrendingScores(db);
    return res.json(result);
  } catch (err) {
    console.error("POST /api/admin/trending/recompute error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Recompute competitor analysis (admin) - designed for daily cron runs.
app.post("/api/admin/competitors/recompute", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const body = req.body || {};
    const limitRaw = Number(body.limit ?? req.query?.limit);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
      : 3;

    const rawIds = Array.isArray(body.product_ids)
      ? body.product_ids
      : Array.isArray(body.productIds)
        ? body.productIds
        : [];

    const productIds = Array.from(
      new Set(
        rawIds
          .map((value) => Number(value))
          .filter((value) => Number.isInteger(value) && value > 0),
      ),
    );

    const result = await recomputeSmartphoneCompetitorAnalysis(db, {
      limit,
      productIds,
    });

    return res.json({
      ok: true,
      ...result,
    });
  } catch (err) {
    console.error("POST /api/admin/competitors/recompute error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Admin: Inspect trending scores + signals (for debugging)
app.get("/api/admin/trending", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const q = req.query || {};
    const typeRaw = String(q.type ?? q.product_type ?? "").trim();
    const limitRaw = Number(q.limit ?? 50);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(200, Math.max(1, Math.floor(limitRaw)))
      : 50;

    const allowedTypes = [
      "smartphone",
      "laptop",
      "networking",
      "tv",
      "accessories",
    ];

    const type =
      typeRaw && allowedTypes.includes(typeRaw) ? typeRaw : typeRaw ? null : "";

    if (type === null) {
      return res.status(400).json({ message: "Invalid type" });
    }

    const params = [];
    let where = "";
    if (typeRaw) {
      params.push(typeRaw);
      where = `WHERE p.product_type = $${params.length}`;
    }
    params.push(limit);

    const result = await db.query(
      `
      WITH views_total AS (
        SELECT
          product_id,
          COUNT(*)::int AS views_total,
          COUNT(DISTINCT COALESCE(visitor_key, id::text))::int AS unique_visitors_total
        FROM product_views
        GROUP BY product_id
      ),
      compares_total AS (
        SELECT product_id, COUNT(*)::int AS compares_total
        FROM (
          SELECT product_id
          FROM product_comparisons
          UNION ALL
          SELECT compared_with AS product_id
          FROM product_comparisons
        ) t
        GROUP BY product_id
      )
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand,
        ts.views_7d,
        ts.compares_7d,
        ts.views_prev_7d,
        ts.velocity,
        ts.trending_score,
        to_char(
          ts.calculated_at AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
        ) AS calculated_at,
        to_char(
          MAX(ts.calculated_at) OVER () AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
        ) AS updated_at,
        ts.manual_boost,
        ts.manual_priority,
        ts.manual_badge,
        COALESCE(vt.views_total, 0) AS views_total,
        COALESCE(vt.unique_visitors_total, 0) AS unique_visitors_total,
        COALESCE(ct.compares_total, 0) AS compares_total
      FROM product_trending_score ts
      INNER JOIN products p
        ON p.id = ts.product_id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN views_total vt
        ON vt.product_id = p.id
      LEFT JOIN compares_total ct
        ON ct.product_id = p.id
      ${where}
      ORDER BY
        ts.manual_priority DESC,
        ts.manual_boost DESC,
        ts.trending_score DESC,
        ts.calculated_at DESC,
        p.id DESC
      LIMIT $${params.length}
      `,
      params,
    );

    return res.json({
      success: true,
      type: typeRaw || "all",
      period: "7d",
      updated_at: result.rows?.[0]?.updated_at || null,
      results: result.rows || [],
    });
  } catch (err) {
    console.error("GET /api/admin/trending error:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
});

// Admin: Manual boosts (editorial/campaign overrides)
app.post("/api/admin/trending/boost", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const body = req.body || {};
    const productId = Number(body.product_id ?? body.productId ?? body.id);
    if (!Number.isInteger(productId) || productId <= 0) {
      return res.status(400).json({ message: "Invalid product_id" });
    }

    const manualBoost =
      body.manual_boost ?? body.manualBoost ?? body.manual ?? body.boost;
    const manualPriorityRaw = Number(
      body.manual_priority ?? body.manualPriority ?? body.priority ?? 0,
    );
    const manualPriority = Number.isFinite(manualPriorityRaw)
      ? Math.max(0, Math.floor(manualPriorityRaw))
      : 0;
    const manualBadgeRaw = body.manual_badge ?? body.manualBadge ?? body.badge;
    const manualBadge =
      manualBadgeRaw === null || manualBadgeRaw === undefined
        ? null
        : String(manualBadgeRaw).trim().slice(0, 64);

    const parseBool = (v) => {
      if (v === true || v === false) return v;
      if (v === 1 || v === 0) return Boolean(v);
      if (v === null || v === undefined) return false;
      const s = String(v).trim().toLowerCase();
      if (["true", "1", "yes", "y", "on"].includes(s)) return true;
      if (["false", "0", "no", "n", "off"].includes(s)) return false;
      return false;
    };

    const manual_boost = parseBool(manualBoost);

    await db.query(
      `
      INSERT INTO product_trending_score (product_id, manual_boost, manual_priority, manual_badge)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (product_id)
      DO UPDATE SET
        manual_boost = EXCLUDED.manual_boost,
        manual_priority = EXCLUDED.manual_priority,
        manual_badge = EXCLUDED.manual_badge
      `,
      [productId, manual_boost, manualPriority, manualBadge],
    );

    return res.json({
      success: true,
      product_id: productId,
      manual_boost,
      manual_priority: manualPriority,
      manual_badge: manualBadge,
    });
  } catch (err) {
    console.error("POST /api/admin/trending/boost error:", err);
    if (err && err.code === "23503") {
      return res.status(400).json({ message: "Invalid product_id" });
    }
    return res.status(500).json({ success: false, message: err.message });
  }
});

app.get("/api/admin/compare-scoring", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const config = await readCompareScoringConfig();
    return res.json(toCompareScoringAdminResponse(config));
  } catch (err) {
    console.error("GET /api/admin/compare-scoring error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load compare scoring config" });
  }
});

app.get("/api/public/device-field-profiles", async (_req, res) => {
  try {
    const config = await readDeviceFieldProfilesConfig();
    return res.json(toDeviceFieldProfilesResponse(config));
  } catch (err) {
    console.error("GET /api/public/device-field-profiles error:", err);
    return res.status(500).json({
      message: "Failed to load device field profiles",
      profiles: normalizeDeviceFieldProfilesConfig(
        DEFAULT_DEVICE_FIELD_PROFILES,
      ),
      updated_at: null,
    });
  }
});

app.get("/api/admin/device-field-profiles", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const config = await readDeviceFieldProfilesConfig();
    return res.json(toDeviceFieldProfilesResponse(config));
  } catch (err) {
    console.error("GET /api/admin/device-field-profiles error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load device field profiles" });
  }
});

app.put("/api/admin/device-field-profiles", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const body = req.body || {};
    const normalizedProfiles = normalizeDeviceFieldProfilesConfig(
      body.profiles || body,
    );

    await db.query(
      `
      INSERT INTO device_field_profiles_config (id, profiles, updated_by, updated_at)
      VALUES (1, $1::jsonb, $2, now())
      ON CONFLICT (id)
      DO UPDATE SET
        profiles = EXCLUDED.profiles,
        updated_by = EXCLUDED.updated_by,
        updated_at = now()
      `,
      [JSON.stringify(normalizedProfiles), req.user?.id ?? null],
    );

    const updated = await readDeviceFieldProfilesConfig();
    return res.json({
      success: true,
      ...toDeviceFieldProfilesResponse(updated),
    });
  } catch (err) {
    console.error("PUT /api/admin/device-field-profiles error:", err);
    return res
      .status(500)
      .json({ message: "Failed to update device field profiles" });
  }
});

app.put("/api/admin/compare-scoring", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const body = req.body || {};
    const normalized = normalizeCompareScoreConfig({
      weights: body.weights || body,
      chipset_rules: body.chipset_rules ?? body.chipsetRules ?? [],
    });

    await db.query(
      `
      INSERT INTO compare_scoring_config (id, weights, chipset_rules, updated_by, updated_at)
      VALUES (1, $1::jsonb, $2::jsonb, $3, now())
      ON CONFLICT (id)
      DO UPDATE SET
        weights = EXCLUDED.weights,
        chipset_rules = EXCLUDED.chipset_rules,
        updated_by = EXCLUDED.updated_by,
        updated_at = now()
      `,
      [
        JSON.stringify(normalized.weights),
        JSON.stringify(normalized.chipsetRules),
        req.user?.id ?? null,
      ],
    );

    const updated = await readCompareScoringConfig();
    return res.json({
      success: true,
      ...toCompareScoringAdminResponse(updated),
    });
  } catch (err) {
    console.error("PUT /api/admin/compare-scoring error:", err);
    return res
      .status(500)
      .json({ message: "Failed to update compare scoring config" });
  }
});

app.get("/api/admin/compare-pages", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const pages = await readPublishedComparePages({
      limit: Math.min(100, Math.max(1, Number(req.query?.limit) || 100)),
    });
    return res.json({ pages });
  } catch (err) {
    console.error("GET /api/admin/compare-pages error:", err);
    return res.status(500).json({ message: "Failed to load compare pages" });
  }
});

app.get(
  "/api/admin/compare-pages/suggestions/:productId",
  authenticate,
  async (req, res) => {
    try {
      if (req.user?.role !== "admin") {
        return res.status(403).json({ message: "Admin access required" });
      }

      const result = await readSmartphoneCompareSuggestions(
        req.params.productId,
        2,
      );
      return res.json(result);
    } catch (err) {
      console.error(
        "GET /api/admin/compare-pages/suggestions/:productId error:",
        err,
      );
      return res.status(500).json({ message: "Failed to load suggestions" });
    }
  },
);

app.get("/api/admin/compare-pages/:id", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const pageId = Number(req.params.id);
    if (!Number.isInteger(pageId) || pageId <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const pages = await readPublishedComparePages({ id: pageId, limit: 1 });
    if (!pages.length) {
      return res.status(404).json({ message: "Compare page not found" });
    }

    return res.json({ page: pages[0] });
  } catch (err) {
    console.error("GET /api/admin/compare-pages/:id error:", err);
    return res.status(500).json({ message: "Failed to load compare page" });
  }
});

app.post("/api/admin/compare-pages", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const page = await savePublishedComparePage({
      payload: req.body || {},
      userId: req.user?.id ?? null,
    });
    return res.status(201).json({ success: true, page });
  } catch (err) {
    console.error("POST /api/admin/compare-pages error:", err);
    return res
      .status(err.statusCode || 500)
      .json({
        message: err.message || "Failed to create compare page",
        existingPage: err.existingPage || null,
      });
  }
});

app.put("/api/admin/compare-pages/:id", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const pageId = Number(req.params.id);
    if (!Number.isInteger(pageId) || pageId <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const page = await savePublishedComparePage({
      pageId,
      payload: req.body || {},
      userId: req.user?.id ?? null,
    });
    return res.json({ success: true, page });
  } catch (err) {
    console.error("PUT /api/admin/compare-pages/:id error:", err);
    return res
      .status(err.statusCode || 500)
      .json({
        message: err.message || "Failed to update compare page",
        existingPage: err.existingPage || null,
      });
  }
});

app.post(
  "/api/admin/compare-pages/auto-sync",
  authenticate,
  async (req, res) => {
    try {
      if (req.user?.role !== "admin") {
        return res.status(403).json({ message: "Admin access required" });
      }

      const result = await syncAutomaticSmartphoneComparePages({
        userId: req.user?.id ?? null,
        recomputeIfMissing: true,
      });
      return res.json({ success: true, result });
    } catch (err) {
      console.error("POST /api/admin/compare-pages/auto-sync error:", err);
      return res.status(500).json({
        message: "Failed to sync automatic compare pages",
      });
    }
  },
);

app.post("/api/admin/blogs/context", authenticate, async (req, res) => {
  try {
    if (!(await ensureBlogManagerAccess(req, res, "view"))) return;

    const productIds = orderBlogProductIds(
      req.body?.product_ids ?? req.body?.productIds ?? req.body?.products,
      req.body?.primary_product_id ?? req.body?.primaryProductId ?? req.body?.product_id,
    );
    if (!productIds.length) {
      return res.status(400).json({ message: "At least one product is required" });
    }

    const profileConfig = await readDeviceFieldProfilesConfig();
    const snapshotResult = await fetchBlogSnapshotsByProductIds(
      productIds,
      profileConfig.profiles,
    );
    if (snapshotResult.missingIds.length) {
      return res.status(404).json({
        message: `Products not found: ${snapshotResult.missingIds.join(", ")}`,
      });
    }

    const selectionContext = buildBlogSelectionContext(
      snapshotResult.snapshots,
      req.body?.token_map,
    );
    const suggestions = buildBlogSuggestionsForSelection(
      snapshotResult.snapshots,
      selectionContext.tokenMap,
    );
    const shouldMatchExisting = req.body?.match_existing !== false;
    const existingMatch = shouldMatchExisting
      ? await findExistingBlogByOrderedProductSet(selectionContext.productIds)
      : null;
    const existing = existingMatch?.id
      ? await db.query(
          `
            SELECT
              id,
              product_id,
              category,
              title,
              slug,
              excerpt,
              author_name,
              author_user_id,
              content_template,
              content_rendered,
              status,
              blog_eligible,
              meta_title,
              meta_description,
              hero_image_source,
              hero_image_alt,
              hero_image_caption,
              tags,
              featured,
              trending,
              pinned,
              CASE
                WHEN hero_image_source = 'none' THEN NULL
                ELSE COALESCE(
                  hero_image,
                  (
                    SELECT pi.image_url
                    FROM product_images pi
                    WHERE pi.product_id = blogs.product_id
                    ORDER BY pi.position ASC NULLS LAST, pi.id ASC
                    LIMIT 1
                  )
                )
              END AS hero_image,
              published_at,
              created_at,
              updated_at
            FROM blogs
            WHERE id = $1
            LIMIT 1
          `,
          [existingMatch.id],
        )
      : { rows: [] };

    return res.json({
      primary_product_id: selectionContext.productIds[0] || null,
      primary_product_type:
        selectionContext.primarySnapshot?.product_type || null,
      product_ids: selectionContext.productIds,
      products: selectionContext.products,
      token_map: selectionContext.tokenMap,
      token_keys: selectionContext.tokenKeys,
      suggestions,
      existing_blog: existing.rows[0] || null,
    });
  } catch (err) {
    console.error("POST /api/admin/blogs/context error:", err);
    return res.status(500).json({ message: "Failed to load blog context" });
  }
});

app.delete("/api/admin/compare-pages/:id", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const pageId = Number(req.params.id);
    if (!Number.isInteger(pageId) || pageId <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const result = await db.query(
      `DELETE FROM published_compare_pages WHERE id = $1 RETURNING id`,
      [pageId],
    );
    if (!result.rows.length) {
      return res.status(404).json({ message: "Compare page not found" });
    }

    return res.json({ success: true, id: pageId });
  } catch (err) {
    console.error("DELETE /api/admin/compare-pages/:id error:", err);
    return res.status(500).json({ message: "Failed to delete compare page" });
  }
});

const FEATURE_CLICK_DEVICE_TYPE_LABELS = {
  smartphone: "Smartphone",
  laptop: "Laptop",
  tv: "TV",
  "home-appliance": "Home Appliance",
  networking: "Networking",
};

const FEATURE_CLICK_DEVICE_TYPE_ALIASES = {
  mobile: "smartphone",
  mobiles: "smartphone",
  phone: "smartphone",
  phones: "smartphone",
  notebook: "laptop",
  notebooks: "laptop",
  television: "tv",
  televisions: "tv",
  appliance: "home-appliance",
  appliances: "home-appliance",
  "home-appliances": "home-appliance",
};

const FEATURE_CLICK_META = {
  "ai-features": { label: "AI Features", category: "Smart Features" },
  "high-camera": { label: "High MP Camera", category: "Camera" },
  "long-battery": { label: "Long Battery", category: "Battery" },
  "fast-charging": { label: "Fast Charging", category: "Battery" },
  "wireless-charging": { label: "Wireless Charging", category: "Battery" },
  amoled: { label: "AMOLED Display", category: "Display" },
  "high-refresh-rate": { label: "120Hz+ Refresh Rate", category: "Display" },
  "5g": { label: "5G Connectivity", category: "Connectivity" },
  "wifi-7": { label: "Wi-Fi 7", category: "Connectivity" },
  "ip-rating": { label: "IP Rating", category: "Security" },
  "high-ram": { label: "High RAM", category: "Performance" },
  gaming: { label: "Gaming Ready", category: "Performance" },
  esim: { label: "eSIM", category: "Connectivity" },
  nfc: { label: "NFC", category: "Connectivity" },
  ois: { label: "OIS Camera", category: "Camera" },
  periscope: { label: "Periscope Lens", category: "Camera" },
  "ufs-4": { label: "UFS 4.x Storage", category: "Performance" },
  lpddr5x: { label: "LPDDR5X Memory", category: "Performance" },
  fingerprint: { label: "Fingerprint Security", category: "Security" },
  "high-storage": { label: "High Storage", category: "Performance" },
  lightweight: { label: "Lightweight Design", category: "Design" },
  "oled-display": { label: "OLED Display", category: "Display" },
  touchscreen: { label: "Touchscreen", category: "Display" },
  intel: { label: "Intel Powered", category: "Performance" },
  amd: { label: "AMD Powered", category: "Performance" },
  "large-screen": { label: "Large Screen", category: "Display" },
  "ultra-hd-4k": { label: "4K Ultra HD", category: "Display" },
  "oled-qled": { label: "OLED / QLED", category: "Display" },
  "smart-tv": { label: "Smart TV", category: "Smart Features" },
  hdr: { label: "HDR", category: "Display" },
  "dolby-audio": { label: "Dolby Audio", category: "Audio" },
  wifi: { label: "Wi-Fi", category: "Connectivity" },
  "voice-assistant": { label: "Voice Assistant", category: "Smart Features" },
};

const normalizeFeatureClickToken = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");

const normalizeFeatureClickDeviceType = (value, allowAll = false) => {
  const normalized = normalizeFeatureClickToken(value);
  if (allowAll && (!normalized || normalized === "all")) {
    return "all";
  }
  return FEATURE_CLICK_DEVICE_TYPE_ALIASES[normalized] || normalized;
};

const isSafeFeatureClickId = (value) =>
  /^[a-z0-9][a-z0-9-]{0,63}$/.test(String(value || ""));

const toFeatureClickLabel = (value) =>
  String(value || "")
    .split("-")
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");

const getFeatureClickMeta = (featureId) => {
  const normalized = normalizeFeatureClickToken(featureId);
  const matched = FEATURE_CLICK_META[normalized] || null;
  return {
    feature_id: normalized,
    feature_label: matched?.label || toFeatureClickLabel(normalized) || "Feature",
    category: matched?.category || "Other",
  };
};

const getFeatureClickDeviceLabel = (deviceType) => {
  const normalized = normalizeFeatureClickDeviceType(deviceType);
  return (
    FEATURE_CLICK_DEVICE_TYPE_LABELS[normalized] ||
    toFeatureClickLabel(normalized) ||
    "Unknown"
  );
};

const roundFeatureMetric = (value, digits = 1) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Number(parsed.toFixed(digits));
};

const computeFeatureChangePercent = (currentValue, previousValue) => {
  const current = Number(currentValue) || 0;
  const previous = Number(previousValue) || 0;
  if (current === 0 && previous === 0) return 0;
  if (previous <= 0) return current > 0 ? 100 : 0;
  return roundFeatureMetric(((current - previous) / previous) * 100, 1);
};

const shiftFeatureDateOnly = (value, offsetDays) => {
  const base = new Date(`${String(value || "").slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(base.getTime())) return null;
  base.setUTCDate(base.getUTCDate() + Number(offsetDays || 0));
  return base.toISOString().slice(0, 10);
};

// Popular feature clicks (public) - aggregated per day
app.post("/api/public/feature-click", async (req, res) => {
  try {
    const b = req.body || {};
    const deviceType = normalizeFeatureClickDeviceType(
      b.device_type ?? b.deviceType ?? "",
    );
    const featureId = normalizeFeatureClickToken(
      b.feature_id ?? b.featureId ?? b.id ?? "",
    );

    if (!deviceType || !featureId) {
      return res
        .status(400)
        .json({ message: "device_type and feature_id are required" });
    }
    if (!isSafeFeatureClickId(deviceType) || !isSafeFeatureClickId(featureId)) {
      return res
        .status(400)
        .json({ message: "Invalid device_type/feature_id" });
    }

    await db.query(
      `
      INSERT INTO feature_click_stats (device_type, feature_id, day, clicks, last_clicked_at)
      VALUES ($1, $2, CURRENT_DATE, 1, now())
      ON CONFLICT (device_type, feature_id, day)
      DO UPDATE SET
        clicks = feature_click_stats.clicks + 1,
        last_clicked_at = now()
      `,
      [deviceType, featureId],
    );

    return res.json({ success: true });
  } catch (err) {
    console.error("POST /api/public/feature-click error:", err);
    return res.status(500).json({ success: false });
  }
});

app.post("/api/public/search-interest", async (req, res) => {
  try {
    const body = req.body || {};
    const query = cleanText(body.query, 180) || null;
    const rawProductType = body.product_type ?? body.productType ?? "";
    const rawDeviceType =
      body.device_type ?? body.deviceType ?? rawProductType ?? "";
    const normalizedProductType =
      rawProductType === "" ||
      rawProductType === null ||
      rawProductType === undefined
        ? null
        : normalizeProductType(rawProductType);
    const normalizedDeviceType =
      rawDeviceType === "" ||
      rawDeviceType === null ||
      rawDeviceType === undefined
        ? null
        : normalizeProductType(rawDeviceType);

    if (rawProductType && normalizedProductType === undefined) {
      return res.status(400).json({ message: "Invalid product_type" });
    }

    if (rawDeviceType && normalizedDeviceType === undefined) {
      return res.status(400).json({ message: "Invalid device_type" });
    }

    const resolvedProduct = await resolveSearchInterestProduct(db, {
      productId: body.product_id ?? body.productId,
      productType: normalizedProductType || normalizedDeviceType || null,
      query,
    });

    if (!query && !resolvedProduct?.product_id) {
      return res
        .status(400)
        .json({ message: "query or product_id is required" });
    }

    const eventId = cleanToken(body.event_id ?? body.eventId, 96) || null;
    const source = cleanToken(body.source, 48) || "search";

    await db.query(
      `
      INSERT INTO search_interest_events (
        event_id,
        query,
        normalized_query,
        product_id,
        product_type,
        device_type,
        source
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (event_id) DO NOTHING
      `,
      [
        eventId,
        query,
        query ? normalizeSearchQuery(query) : null,
        resolvedProduct?.product_id || null,
        resolvedProduct?.product_type || normalizedProductType || null,
        normalizedDeviceType || normalizedProductType || null,
        source,
      ],
    );

    return res.json({
      success: true,
      product_id: resolvedProduct?.product_id || null,
    });
  } catch (err) {
    console.error("POST /api/public/search-interest error:", err);
    return res.status(500).json({ success: false });
  }
});

// Popular feature ordering (public) - last N days
app.get("/api/public/popular-features", async (req, res) => {
  try {
    const q = req.query || {};
    const deviceType = normalizeFeatureClickDeviceType(
      q.deviceType ?? q.device_type ?? "smartphone",
    );
    if (!isSafeFeatureClickId(deviceType)) {
      return res.status(400).json({ message: "Invalid deviceType" });
    }

    const daysRaw = Number(q.days ?? 7);
    const limitRaw = Number(q.limit ?? 16);
    const days = Number.isFinite(daysRaw)
      ? Math.min(30, Math.max(1, Math.floor(daysRaw)))
      : 7;
    const limit = Number.isFinite(limitRaw)
      ? Math.min(50, Math.max(1, Math.floor(limitRaw)))
      : 16;

    const result = await db.query(
      `
      SELECT
        feature_id,
        SUM(clicks)::int AS clicks,
        MAX(last_clicked_at) AS last_clicked_at
      FROM feature_click_stats
      WHERE device_type = $1
        AND day >= (CURRENT_DATE - (($2::int) - 1))
      GROUP BY feature_id
      ORDER BY clicks DESC, last_clicked_at DESC
      LIMIT $3
      `,
      [deviceType, days, limit],
    );

    return res.json({
      device_type: deviceType,
      days,
      results: result.rows || [],
    });
  } catch (err) {
    console.error("GET /api/public/popular-features error:", err);
    return res.status(500).json({ message: "Failed to load popular features" });
  }
});

app.get("/api/reports/feature-clicks", authenticate, async (req, res) => {
  try {
    const q = req.query || {};
    const deviceType = normalizeFeatureClickDeviceType(
      q.deviceType ?? q.device_type ?? "all",
      true,
    );
    const daysRaw = Number(q.days ?? 7);
    const days = Number.isFinite(daysRaw)
      ? Math.min(90, Math.max(1, Math.floor(daysRaw)))
      : 7;

    if (deviceType !== "all" && !isSafeFeatureClickId(deviceType)) {
      return res.status(400).json({ message: "Invalid deviceType" });
    }

    const currentRangeSql =
      deviceType === "all"
        ? "day >= (CURRENT_DATE - (($1::int) - 1))"
        : "device_type = $1 AND day >= (CURRENT_DATE - (($2::int) - 1))";
    const previousRangeSql =
      deviceType === "all"
        ? "day >= (CURRENT_DATE - ((($1::int) * 2) - 1)) AND day < (CURRENT_DATE - (($1::int) - 1))"
        : "device_type = $1 AND day >= (CURRENT_DATE - ((($2::int) * 2) - 1)) AND day < (CURRENT_DATE - (($2::int) - 1))";
    const scopedParams = deviceType === "all" ? [days] : [deviceType, days];

    const [
      todayRes,
      dailyRes,
      previousDailyRes,
      featureRes,
      previousFeatureRes,
      deviceRes,
    ] =
      await Promise.all([
        db.query(`SELECT CURRENT_DATE::text AS current_date`),
        db.query(
          `
          SELECT day::text AS day, SUM(clicks)::int AS clicks
          FROM feature_click_stats
          WHERE ${currentRangeSql}
          GROUP BY day
          ORDER BY day ASC
          `,
          scopedParams,
        ),
        db.query(
          `
          SELECT day::text AS day, SUM(clicks)::int AS clicks
          FROM feature_click_stats
          WHERE ${previousRangeSql}
          GROUP BY day
          ORDER BY day ASC
          `,
          scopedParams,
        ),
        db.query(
          `
          SELECT
            feature_id,
            ARRAY_AGG(DISTINCT device_type ORDER BY device_type) AS device_types,
            SUM(clicks)::int AS clicks,
            MAX(last_clicked_at) AS last_clicked_at
          FROM feature_click_stats
          WHERE ${currentRangeSql}
          GROUP BY feature_id
          ORDER BY clicks DESC, last_clicked_at DESC
          `,
          scopedParams,
        ),
        db.query(
          `
          SELECT
            feature_id,
            SUM(clicks)::int AS clicks
          FROM feature_click_stats
          WHERE ${previousRangeSql}
          GROUP BY feature_id
          `,
          scopedParams,
        ),
        db.query(
          `
          SELECT device_type, SUM(clicks)::int AS clicks
          FROM feature_click_stats
          WHERE ${currentRangeSql}
          GROUP BY device_type
          ORDER BY clicks DESC, device_type ASC
          `,
          scopedParams,
        ),
      ]);

    const currentDate =
      todayRes.rows?.[0]?.current_date ||
      new Date().toISOString().slice(0, 10);
    const rangeEnd = currentDate;
    const rangeStart = shiftFeatureDateOnly(currentDate, -(days - 1)) || currentDate;
    const previousRangeEnd = shiftFeatureDateOnly(rangeStart, -1) || rangeStart;
    const previousRangeStart =
      shiftFeatureDateOnly(previousRangeEnd, -(days - 1)) || previousRangeEnd;

    const dailyMap = new Map(
      (dailyRes.rows || []).map((row) => [
        String(row.day || "").slice(0, 10),
        Number(row.clicks) || 0,
      ]),
    );
    const previousDailyMap = new Map(
      (previousDailyRes.rows || []).map((row) => [
        String(row.day || "").slice(0, 10),
        Number(row.clicks) || 0,
      ]),
    );

    const series = [];
    for (let index = 0; index < days; index += 1) {
      const dateValue = shiftFeatureDateOnly(rangeStart, index) || rangeStart;
      series.push({
        date: dateValue,
        clicks: dailyMap.get(dateValue) || 0,
      });
    }

    const activeDays = series.filter((item) => Number(item.clicks) > 0).length;
    const previousSeries = [];
    for (let index = 0; index < days; index += 1) {
      const dateValue =
        shiftFeatureDateOnly(previousRangeStart, index) || previousRangeStart;
      previousSeries.push({
        date: dateValue,
        clicks: previousDailyMap.get(dateValue) || 0,
      });
    }
    const previousActiveDays = previousSeries.filter(
      (item) => Number(item.clicks) > 0,
    ).length;
    const previousFeatureMap = new Map(
      (previousFeatureRes.rows || []).map((row) => [
        normalizeFeatureClickToken(row.feature_id),
        Number(row.clicks) || 0,
      ]),
    );

    const currentFeatureRows = (featureRes.rows || []).map((row) => {
      const featureId = normalizeFeatureClickToken(row.feature_id);
      const meta = getFeatureClickMeta(featureId);
      const clicks = Number(row.clicks) || 0;
      const previousClicks = previousFeatureMap.get(featureId) || 0;
      const normalizedDevices = Array.isArray(row.device_types)
        ? row.device_types
            .map((item) => normalizeFeatureClickDeviceType(item))
            .filter(Boolean)
        : [];

      return {
        feature_id: featureId,
        feature_label: meta.feature_label,
        category: meta.category,
        clicks,
        change_pct: computeFeatureChangePercent(clicks, previousClicks),
        last_clicked_at: row.last_clicked_at || null,
        device_types: normalizedDevices,
        device_labels: normalizedDevices.map(getFeatureClickDeviceLabel),
      };
    });

    const totalClicks = currentFeatureRows.reduce(
      (sum, row) => sum + Number(row.clicks || 0),
      0,
    );
    const previousTotalClicks = Array.from(previousFeatureMap.values()).reduce(
      (sum, value) => sum + Number(value || 0),
      0,
    );

    const categoryMap = new Map();
    const previousCategoryMap = new Map();

    for (const row of currentFeatureRows) {
      const existing = categoryMap.get(row.category) || 0;
      categoryMap.set(row.category, existing + Number(row.clicks || 0));
    }

    for (const [featureId, clicks] of previousFeatureMap.entries()) {
      const meta = getFeatureClickMeta(featureId);
      const existing = previousCategoryMap.get(meta.category) || 0;
      previousCategoryMap.set(meta.category, existing + Number(clicks || 0));
    }

    const categoryBreakdown = Array.from(categoryMap.entries())
      .map(([label, clicks]) => {
        const previousClicks = previousCategoryMap.get(label) || 0;
        const percent = totalClicks > 0 ? (Number(clicks) / totalClicks) * 100 : 0;
        return {
          label,
          clicks: Number(clicks) || 0,
          percent: roundFeatureMetric(percent, 1),
          change_pct: computeFeatureChangePercent(clicks, previousClicks),
        };
      })
      .sort((left, right) => right.clicks - left.clicks);

    const previousCategoryBreakdown = Array.from(previousCategoryMap.entries())
      .map(([label, clicks]) => {
        const percent =
          previousTotalClicks > 0
            ? (Number(clicks) / previousTotalClicks) * 100
            : 0;
        return {
          label,
          clicks: Number(clicks) || 0,
          percent: roundFeatureMetric(percent, 1),
        };
      })
      .sort((left, right) => right.clicks - left.clicks);

    const topCategory = categoryBreakdown[0] || null;
    const previousTopCategory = previousCategoryBreakdown[0] || null;
    const uniqueFeatures = currentFeatureRows.length;
    const previousUniqueFeatures = previousFeatureMap.size;
    const avgDailyClicks = days > 0 ? totalClicks / days : 0;
    const previousAvgDailyClicks = days > 0 ? previousTotalClicks / days : 0;
    const avgClicksPerFeature =
      uniqueFeatures > 0 ? totalClicks / uniqueFeatures : 0;
    const previousAvgClicksPerFeature =
      previousUniqueFeatures > 0
        ? previousTotalClicks / previousUniqueFeatures
        : 0;

    const deviceBreakdown = (deviceRes.rows || []).map((row) => {
      const clicks = Number(row.clicks) || 0;
      return {
        key: normalizeFeatureClickDeviceType(row.device_type),
        label: getFeatureClickDeviceLabel(row.device_type),
        clicks,
        percent:
          totalClicks > 0 ? roundFeatureMetric((clicks / totalClicks) * 100, 1) : 0,
      };
    });

    const topFeatures = currentFeatureRows.slice(0, 8).map((row, index) => ({
      ...row,
      rank: index + 1,
      share_pct:
        totalClicks > 0
          ? roundFeatureMetric((Number(row.clicks || 0) / totalClicks) * 100, 1)
          : 0,
    }));

    return res.json({
      success: true,
      generated_at: new Date().toISOString(),
      filters: {
        days,
        device_type: deviceType,
        range_start: rangeStart,
        range_end: rangeEnd,
        previous_range_start: previousRangeStart,
        previous_range_end: previousRangeEnd,
      },
      summary: {
        total_clicks: totalClicks,
        total_clicks_change_pct: computeFeatureChangePercent(
          totalClicks,
          previousTotalClicks,
        ),
        avg_daily_clicks: roundFeatureMetric(avgDailyClicks, 1),
        avg_daily_clicks_change_pct: computeFeatureChangePercent(
          avgDailyClicks,
          previousAvgDailyClicks,
        ),
        avg_clicks_per_feature: roundFeatureMetric(avgClicksPerFeature, 1),
        avg_clicks_per_feature_change_pct: computeFeatureChangePercent(
          avgClicksPerFeature,
          previousAvgClicksPerFeature,
        ),
        features_clicked: uniqueFeatures,
        features_clicked_change_pct: computeFeatureChangePercent(
          uniqueFeatures,
          previousUniqueFeatures,
        ),
        top_category_label: topCategory?.label || "None",
        top_category_share_pct: roundFeatureMetric(topCategory?.percent || 0, 1),
        top_category_share_change_pct: computeFeatureChangePercent(
          topCategory?.percent || 0,
          previousTopCategory?.percent || 0,
        ),
        active_days: activeDays,
        active_days_change_pct: computeFeatureChangePercent(
          activeDays,
          previousActiveDays,
        ),
      },
      series,
      top_features: topFeatures,
      device_breakdown: deviceBreakdown,
      category_breakdown: categoryBreakdown,
    });
  } catch (err) {
    console.error("GET /api/reports/feature-clicks error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load feature clicks report" });
  }
});

app.get("/api/public/search-popularity", async (req, res) => {
  try {
    const query = req.query || {};
    const rawType = String(
      query.productType ?? query.product_type ?? query.type ?? "",
    ).trim();

    if (rawType && normalizeProductType(rawType) === undefined) {
      return res.status(400).json({ message: "Invalid productType" });
    }

    const result = await getSearchPopularityDevices(db, {
      productType: rawType,
      days: query.days,
      limit: query.limit ?? 5,
    });

    return res.json({
      success: true,
      product_type: result.productType || "all",
      days: result.days,
      generated_at: new Date().toISOString(),
      devices: result.devices,
    });
  } catch (err) {
    console.error("GET /api/public/search-popularity error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load search popularity" });
  }
});

app.get("/api/admin/search-popularity", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const query = req.query || {};
    const rawType = String(
      query.productType ?? query.product_type ?? query.type ?? "",
    ).trim();

    if (rawType && normalizeProductType(rawType) === undefined) {
      return res.status(400).json({ message: "Invalid productType" });
    }

    const result = await getSearchPopularityDevices(db, {
      productType: rawType,
      days: query.days,
      limit: query.limit ?? 100,
    });

    return res.json({
      success: true,
      product_type: result.productType || "all",
      days: result.days,
      generated_at: new Date().toISOString(),
      devices: result.devices,
    });
  } catch (err) {
    console.error("GET /api/admin/search-popularity error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load search popularity report" });
  }
});

app.get("/api/public/trending-products", async (req, res) => {
  try {
    const q = req.query || {};
    const typeRaw = String(q.type ?? q.product_type ?? "smartphone").trim();
    const limitRaw = Number(q.limit ?? 10);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(50, Math.max(1, Math.floor(limitRaw)))
      : 10;

    const allowedTypes = [
      "smartphone",
      "laptop",
      "networking",
      "tv",
      "accessories",
    ];

    if (!allowedTypes.includes(typeRaw)) {
      return res.status(400).json({ message: "Invalid type" });
    }

    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand,
        ts.trending_score,
        ts.manual_boost,
        ts.manual_priority,
        ts.manual_badge,
        to_char(
          MAX(ts.calculated_at) OVER () AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
        ) AS updated_at,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image,
        COALESCE(
          (
            SELECT MIN(sp.price)
            FROM product_variants v
            LEFT JOIN variant_store_prices sp
              ON sp.variant_id = v.id
            WHERE v.product_id = p.id
              AND sp.price IS NOT NULL
          ),
          (
            SELECT MIN(v.base_price)
            FROM product_variants v
            WHERE v.product_id = p.id
              AND v.base_price IS NOT NULL
          )
        ) AS price
      FROM product_trending_score ts
      INNER JOIN products p
        ON p.id = ts.product_id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE p.product_type = $1
      ORDER BY
        ts.manual_priority DESC,
        ts.manual_boost DESC,
        ts.trending_score DESC,
        p.id DESC
      LIMIT $2
      `,
      [typeRaw, limit],
    );

    const slugify = (name, id) => {
      const s = name
        ? String(name)
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "-")
            .replace(/(^-|-$)/g, "")
        : "";
      return s || `product-${id}`;
    };

    const badgeForScore = (score) => {
      const s = Number(score);
      if (!Number.isFinite(s)) return "👀 Gaining Attention";
      if (s >= 80) return "🔥 Trending Now";
      if (s >= 60) return "📈 Popular This Week";
      return "👀 Gaining Attention";
    };

    const rows = result.rows || [];
    const updatedAt = rows?.[0]?.updated_at || null;

    const trending = rows.map((r) => {
      const manualBoost = Boolean(r.manual_boost);
      const manualBadge = r.manual_badge ? String(r.manual_badge).trim() : "";
      const badge =
        manualBoost && manualBadge
          ? manualBadge
          : manualBoost
            ? "🚀 Editor Pick"
            : badgeForScore(r.trending_score);

      return {
        id: r.product_id,
        name: r.name,
        slug: slugify(r.name, r.product_id),
        image: r.image || null,
        price: r.price ?? null,
        brand: r.brand || null,
        product_type: r.product_type,
        badge,
      };
    });

    return res.json({
      type: typeRaw,
      period: "7d",
      updated_at: updatedAt,
      trending,
    });
  } catch (err) {
    console.error("GET /api/public/trending-products error:", err);
    return res
      .status(500)
      .json({ message: "Failed to fetch trending products" });
  }
});

const normalizeMemoryValue = (value) => {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;

  const parsed = text.match(/(\d+(?:\.\d+)?)\s*(TB|GB|MB)/i);
  if (parsed) {
    const amount = Number(parsed[1]);
    const unit = parsed[2].toUpperCase();
    return `${Number.isFinite(amount) ? amount : parsed[1]} ${unit}`;
  }

  const cleaned = text
    .replace(/\b(ram|rom|storage|internal|memory)\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  return cleaned || text;
};

const memoryToMb = (value) => {
  if (value === null || value === undefined) return Number.MAX_SAFE_INTEGER;
  const text = String(value).trim();
  if (!text) return Number.MAX_SAFE_INTEGER;
  const parsed = text.match(/(\d+(?:\.\d+)?)\s*(TB|GB|MB)/i);
  if (!parsed) return Number.MAX_SAFE_INTEGER;

  const amount = Number(parsed[1]);
  if (!Number.isFinite(amount)) return Number.MAX_SAFE_INTEGER;
  const unit = parsed[2].toUpperCase();

  if (unit === "TB") return amount * 1024 * 1024;
  if (unit === "GB") return amount * 1024;
  return amount;
};

const combineMemoryValues = (values) => {
  if (!values || values.length === 0) return null;
  const normalized = values
    .map(normalizeMemoryValue)
    .filter((val) => val && String(val).trim().length > 0);
  if (normalized.length === 0) return null;

  const unique = Array.from(new Set(normalized));
  unique.sort((a, b) => {
    const diff = memoryToMb(a) - memoryToMb(b);
    if (diff !== 0) return diff;
    return String(a).localeCompare(String(b));
  });

  return unique.join(" / ");
};

const handleTrendingSmartphones = async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const limitRaw = Number(req.query?.limit ?? 20);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(50, Math.max(1, Math.floor(limitRaw)))
      : 20;

    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        b.name AS brand,
        b.logo AS brand_logo,
        MAX(to_jsonb(b)->>'website') AS brand_website,
        s.model,
        s.launch_date,
        s.official_preorder_url,
        s.launch_status_override,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity,
        s.network,
        s.build_design,
        s.audio,
        s.multimedia,
        s.sensors,
        MAX(ts.trending_score) AS trending_score,
        MAX(ts.views_7d) AS views_7d,
        MAX(ts.views_prev_7d) AS views_prev_7d,
        MAX(ts.velocity) AS velocity,
        MAX((ts.manual_boost)::int) AS manual_boost,
        MAX(ts.manual_priority) AS manual_priority,
        MAX(ts.manual_badge) AS manual_badge,
        MAX(ts.calculated_at) AS trending_calculated_at,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image_url,
        COALESCE(
          (
            SELECT MIN(sp.price)
            FROM product_variants v
            LEFT JOIN variant_store_prices sp
              ON sp.variant_id = v.id
            WHERE v.product_id = p.id
              AND sp.price IS NOT NULL
          ),
          (
            SELECT MIN(v.base_price)
            FROM product_variants v
            WHERE v.product_id = p.id
              AND v.base_price IS NOT NULL
          )
        ) AS starting_price,
        ARRAY_AGG(
          DISTINCT NULLIF(
            COALESCE(
              v.attributes->>'ram',
              v.attributes->>'RAM',
              v.attributes->>'memory'
            ),
            ''
          )
        ) FILTER (
          WHERE COALESCE(
            v.attributes->>'ram',
            v.attributes->>'RAM',
            v.attributes->>'memory'
          ) IS NOT NULL
            AND COALESCE(
              v.attributes->>'ram',
              v.attributes->>'RAM',
              v.attributes->>'memory'
            ) <> ''
        ) AS ram_values,
        ARRAY_AGG(
          DISTINCT NULLIF(
            COALESCE(
              v.attributes->>'storage',
              v.attributes->>'rom',
              v.attributes->>'ROM_storage',
              v.attributes->>'internal_storage'
            ),
            ''
          )
        ) FILTER (
          WHERE COALESCE(
            v.attributes->>'storage',
            v.attributes->>'rom',
            v.attributes->>'ROM_storage',
            v.attributes->>'internal_storage'
          ) IS NOT NULL
            AND COALESCE(
              v.attributes->>'storage',
              v.attributes->>'rom',
              v.attributes->>'ROM_storage',
              v.attributes->>'internal_storage'
            ) <> ''
        ) AS storage_values
      FROM products p
      JOIN smartphones s ON s.product_id = p.id
      JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN product_variants v ON v.product_id = p.id
      LEFT JOIN product_trending_score ts ON ts.product_id = p.id
      WHERE p.product_type = 'smartphone'
      GROUP BY
        p.id,
        p.name,
        b.name,
        b.logo,
        s.model,
        s.launch_date,
        s.official_preorder_url,
        s.launch_status_override,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity,
        s.network,
        s.build_design,
        s.audio,
        s.multimedia,
        s.sensors
      ORDER BY
        COALESCE(MAX(ts.manual_priority), 0) DESC,
        COALESCE(MAX((ts.manual_boost)::int), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC
      LIMIT $1;
      `,
      [limit],
    );

    const rows = result.rows || [];
    const trending = applySpecScoreToRows(
      "smartphone",
      rows.map((row) => ({
        id: row.product_id,
        product_id: row.product_id,
        name: row.name,
        brand: row.brand || null,
        brand_name: row.brand || null,
        brand_logo: row.brand_logo || null,
        brand_website: row.brand_website || null,
        official_preorder_url: row.official_preorder_url || null,
        launch_status_override: row.launch_status_override || null,
        model: row.model || null,
        launch_date: row.launch_date || null,
        display: row.display || null,
        performance: row.performance || null,
        camera: row.camera || null,
        battery: row.battery || null,
        connectivity: row.connectivity || null,
        network: row.network || null,
        build_design: row.build_design || null,
        audio: row.audio || null,
        multimedia: row.multimedia || null,
        sensors: row.sensors || null,
        images: row.image_url ? [row.image_url] : [],
        image_url: row.image_url || null,
        variants: [],
        price: row.starting_price ?? null,
        starting_price: row.starting_price ?? null,
        ram: combineMemoryValues(row.ram_values || []),
        storage: combineMemoryValues(row.storage_values || []),
        trend_score:
          Number.isFinite(Number(row?.trending_score)) &&
          row?.trending_score !== null
            ? Number(row.trending_score)
            : null,
        trend_views_7d: Number.isFinite(Number(row?.views_7d))
          ? Number(row.views_7d)
          : 0,
        trend_views_prev_7d: Number.isFinite(Number(row?.views_prev_7d))
          ? Number(row.views_prev_7d)
          : 0,
        trend_velocity:
          Number.isFinite(Number(row?.velocity)) && row?.velocity !== null
            ? Number(row.velocity)
            : null,
        trend_manual_boost: Boolean(Number(row?.manual_boost ?? 0)),
        trend_manual_priority: Number.isFinite(Number(row?.manual_priority))
          ? Number(row.manual_priority)
          : 0,
        trend_manual_badge: row?.manual_badge || null,
        trend_calculated_at: row?.trending_calculated_at ?? null,
      })),
      profileConfig.profiles,
    );

    const todayIndia = getIndiaDateOnly();
    for (const item of trending) {
      const variantsRes = await db.query(
        `SELECT id, variant_key, attributes->>'ram' AS ram, attributes->>'storage' AS storage, base_price
         FROM product_variants
         WHERE product_id = $1
         ORDER BY id ASC`,
        [item.product_id],
      );

      const variants = [];
      for (const variant of variantsRes.rows) {
        const storesRes = await db.query(
          "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY price ASC NULLS LAST, id ASC",
          [variant.id],
        );
        variants.push({
          ...variant,
          variant_id: variant.id,
          store_prices: decorateStorePriceList(storesRes.rows, todayIndia),
        });
      }

      const effectivePrice = resolveEffectiveSmartphonePrice(
        variants,
        item.starting_price ?? item.price ?? null,
      );
      item.variants = variants;
      item.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
      item.price = effectivePrice;
      item.starting_price = effectivePrice;
      const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
      item.launch_status = launchStage;
      item.launchStatus = launchStage;
      applySmartphoneLaunchPolicy(item, launchStage);
    }

    return res.json({
      success: true,
      period: "7d",
      updated_at:
        (rows || []).find((r) => r?.trending_calculated_at)
          ?.trending_calculated_at ?? null,
      trending,
      smartphones: trending,
    });
  } catch (err) {
    console.error("Trending smartphones error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch trending smartphones",
    });
  }
};

// Trending Smartphones (grouped by product, combined RAM/ROM)
app.get("/api/public/trending/smartphones", handleTrendingSmartphones);

// Trending Laptops
app.get("/api/public/trending/laptops", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const limitRaw = Number(req.query?.limit ?? 50);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(100, Math.max(1, Math.floor(limitRaw)))
      : 50;

    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.meta,
        l.spec_sections,
        l.created_at,

        MAX(ts.trending_score) AS trending_score,
        MAX(ts.views_7d) AS views_7d,
        MAX(ts.views_prev_7d) AS views_prev_7d,
        MAX(ts.velocity) AS velocity,
        MAX((ts.manual_boost)::int) AS manual_boost,
        MAX(ts.manual_priority) AS manual_priority,
        MAX(ts.manual_badge) AS manual_badge,
        MAX(ts.calculated_at) AS trending_calculated_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'ram', v.attributes->>'ram',
              'storage', v.attributes->>'storage',
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN laptop l
        ON l.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN product_variants v
        ON v.product_id = p.id
      LEFT JOIN product_trending_score ts
        ON ts.product_id = p.id

      WHERE p.product_type = 'laptop'

      GROUP BY
        p.id,
        b.name,
        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.meta,
        l.spec_sections,
        l.created_at

      ORDER BY
        COALESCE(MAX(ts.manual_priority), 0) DESC,
        COALESCE(MAX((ts.manual_boost)::int), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC

      LIMIT $1;
    `,
      [limit],
    );

    const badgeForScore = (score) => {
      const s = Number(score);
      if (!Number.isFinite(s)) return "Gaining Attention";
      if (s >= 80) return "Trending Now";
      if (s >= 60) return "Popular This Week";
      return "Gaining Attention";
    };

    const laptops = applySpecScoreToRows(
      "laptop",
      (result.rows || []).map((row) => {
        const trendScoreRaw = Number(row?.trending_score);
        const trendScore = Number.isFinite(trendScoreRaw)
          ? trendScoreRaw
          : null;
        const views7dRaw = Number(row?.views_7d);
        const viewsPrevRaw = Number(row?.views_prev_7d);
        const views7d = Number.isFinite(views7dRaw) ? views7dRaw : 0;
        const viewsPrev7d = Number.isFinite(viewsPrevRaw) ? viewsPrevRaw : 0;
        const manualBoost = Boolean(Number(row?.manual_boost ?? 0));
        const manualBadge = row?.manual_badge
          ? String(row.manual_badge).trim()
          : "";

        return {
          ...toCanonicalLaptopProductResponse(row),
          trend_score: trendScore,
          trend_views_7d: views7d,
          trend_views_prev_7d: viewsPrev7d,
          trend_delta: views7d - viewsPrev7d,
          trend_velocity: row?.velocity ?? null,
          trend_manual_boost: manualBoost,
          trend_manual_priority: row?.manual_priority ?? 0,
          trend_manual_badge: manualBadge || null,
          trend_badge:
            manualBoost && manualBadge
              ? manualBadge
              : manualBoost
                ? "Editor Pick"
                : badgeForScore(trendScore),
          trend_calculated_at: row?.trending_calculated_at ?? null,
        };
      }),
      profileConfig.profiles,
    ).map(toPublicLaptopResponseRow);

    return res.json({
      period: "7d",
      updated_at:
        (result.rows || []).find((r) => r?.trending_calculated_at)
          ?.trending_calculated_at ?? null,
      laptops,
      trending: laptops,
    });
  } catch (err) {
    console.error("GET /api/public/trending/laptops error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending TVs
app.get("/api/public/trending/tvs", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const limitRaw = Number(req.query?.limit ?? 50);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(100, Math.max(1, Math.floor(limitRaw)))
      : 50;

    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name AS name,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        b.name AS brand,
        COALESCE(t.model, p.name) AS model,
        t.key_specs_json,
        t.basic_info_json,
        t.display_json,
        t.video_engine_json,
        t.audio_json,
        t.smart_tv_json,
        t.gaming_json,
        t.connectivity_json,
        t.ports_json,
        t.power_json,
        t.physical_json,
        t.product_details_json,
        t.in_the_box_json,
        t.warranty_json,

        MAX(ts.trending_score) AS trending_score,
        MAX(ts.views_7d) AS views_7d,
        MAX(ts.views_prev_7d) AS views_prev_7d,
        MAX(ts.velocity) AS velocity,
        MAX((ts.manual_boost)::int) AS manual_boost,
        MAX(ts.manual_priority) AS manual_priority,
        MAX(ts.manual_badge) AS manual_badge,
        MAX(ts.calculated_at) AS trending_calculated_at,

        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT COALESCE(MIN(sp.price), MIN(v.base_price))
          FROM product_variants v
          LEFT JOIN variant_store_prices sp
            ON sp.variant_id = v.id
          WHERE v.product_id = p.id
        ) AS price,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image,
        COALESCE(
          (
            SELECT json_agg(pi.image_url ORDER BY pi.position ASC NULLS LAST, pi.id ASC)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,
        COALESCE(
          (
            SELECT json_agg(
              jsonb_build_object(
                'variant_id', v.id,
                'variant_key', v.variant_key,
                'screen_size', COALESCE(v.attributes->>'screen_size', v.attributes->>'size'),
                'screen_size_value', NULLIF(
                  regexp_replace(
                    COALESCE(v.attributes->>'screen_size', v.attributes->>'size', v.variant_key, ''),
                    '[^0-9.]',
                    '',
                    'g'
                  ),
                  ''
                )::numeric,
                'base_price', v.base_price,
                'images', (
                  CASE
                    WHEN EXISTS (
                      SELECT 1 FROM product_variant_images pvi0 WHERE pvi0.variant_id = v.id
                    )
                    THEN (
                      SELECT COALESCE(
                        json_agg(pvi.image_url ORDER BY pvi.position ASC NULLS LAST, pvi.id ASC),
                        '[]'::json
                      )
                      FROM product_variant_images pvi
                      WHERE pvi.variant_id = v.id
                    )
                    ELSE COALESCE(v.attributes->'images', v.attributes->'images_json', '[]'::jsonb)::json
                  END
                ),
                'store_prices', (
                  SELECT COALESCE(
                    json_agg(
                      jsonb_build_object(
                        'id', sp.id,
                        'store_name', sp.store_name,
                        'price', sp.price,
                        'url', sp.url,
                        'offer_text', sp.offer_text,
                        'delivery_info', sp.delivery_info
                      )
                      ORDER BY sp.price ASC NULLS LAST, sp.id ASC
                    ),
                    '[]'::json
                  )
                  FROM variant_store_prices sp
                  WHERE sp.variant_id = v.id
                )
              )
              ORDER BY v.id ASC
            )
            FROM product_variants v
            WHERE v.product_id = p.id
          ),
          '[]'::json
        ) AS variants
      FROM products p
      INNER JOIN tvs t
        ON t.product_id = p.id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_trending_score ts
        ON ts.product_id = p.id
      WHERE p.product_type = 'tv'
      GROUP BY
        p.id,
        p.name,
        p.product_type,
        b.name,
        t.model,
        t.key_specs_json,
        t.basic_info_json,
        t.display_json,
        t.video_engine_json,
        t.audio_json,
        t.smart_tv_json,
        t.gaming_json,
        t.connectivity_json,
        t.ports_json,
        t.power_json,
        t.physical_json,
        t.product_details_json,
        t.in_the_box_json,
        t.warranty_json
      ORDER BY
        COALESCE(MAX(ts.manual_priority), 0) DESC,
        COALESCE(MAX((ts.manual_boost)::int), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC
      LIMIT $1;
      `,
      [limit],
    );

    const badgeForScore = (score) => {
      const s = Number(score);
      if (!Number.isFinite(s)) return "Gaining Attention";
      if (s >= 80) return "Trending Now";
      if (s >= 60) return "Popular This Week";
      return "Gaining Attention";
    };

    const tvs = applySpecScoreToRows(
      "tv",
      (result.rows || []).map((row) => {
        const trendScoreRaw = Number(row?.trending_score);
        const trendScore = Number.isFinite(trendScoreRaw)
          ? trendScoreRaw
          : null;
        const views7dRaw = Number(row?.views_7d);
        const viewsPrevRaw = Number(row?.views_prev_7d);
        const views7d = Number.isFinite(views7dRaw) ? views7dRaw : 0;
        const viewsPrev7d = Number.isFinite(viewsPrevRaw) ? viewsPrevRaw : 0;
        const manualBoost = Boolean(Number(row?.manual_boost ?? 0));
        const manualBadge = row?.manual_badge
          ? String(row.manual_badge).trim()
          : "";

        return {
          ...row,
          trend_score: trendScore,
          trend_views_7d: views7d,
          trend_views_prev_7d: viewsPrev7d,
          trend_delta: views7d - viewsPrev7d,
          trend_velocity: row?.velocity ?? null,
          trend_manual_boost: manualBoost,
          trend_manual_priority: row?.manual_priority ?? 0,
          trend_manual_badge: manualBadge || null,
          trend_badge:
            manualBoost && manualBadge
              ? manualBadge
              : manualBoost
                ? "Editor Pick"
                : badgeForScore(trendScore),
          trend_calculated_at: row?.trending_calculated_at ?? null,
        };
      }),
      profileConfig.profiles,
    ).map(toPublicTvResponseRow);

    return res.json({
      period: "7d",
      updated_at:
        (result.rows || []).find((r) => r?.trending_calculated_at)
          ?.trending_calculated_at ?? null,
      tvs,
      trending: tvs,
    });
  } catch (err) {
    console.error("GET /api/public/trending/tvs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending Networking
app.get("/api/public/trending/networking", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
          AND p.product_type = 'networking'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 12
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        n.model_number AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        COALESCE(pi.image_url, NULL) AS image,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN networking n ON n.product_id = p.id
      LEFT JOIN product_images pi ON pi.product_id = p.id AND pi.position = 1
      ORDER BY tp.views DESC, price ASC NULLS LAST
      LIMIT 50;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/networking error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Smartphones
app.get("/api/public/new/smartphones", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.name AS name,
        p.product_type,
        b.name AS brand,
        b.name AS brand_name,
        b.logo AS brand_logo,
        (to_jsonb(b)->>'website') AS brand_website,
        s.model AS model,
        s.launch_date,
        s.official_preorder_url,
        s.launch_status_override,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity,
        s.network,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN smartphones s ON s.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'smartphone'
      ORDER BY COALESCE(s.launch_date, p.created_at) DESC
      LIMIT 20;
    `);

    const launches = applySpecScoreToRows(
      "smartphone",
      (result.rows || []).map((row) => ({
        ...row,
        images: row?.image ? [row.image] : [],
        variants: [],
      })),
      profileConfig.profiles,
    );

    const todayIndia = getIndiaDateOnly();
    for (const item of launches) {
      const variantsRes = await db.query(
        `SELECT id, variant_key, attributes->>'ram' AS ram, attributes->>'storage' AS storage, base_price
         FROM product_variants
         WHERE product_id = $1
         ORDER BY id ASC`,
        [item.product_id],
      );

      const variants = [];
      for (const variant of variantsRes.rows) {
        const storesRes = await db.query(
          "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY price ASC NULLS LAST, id ASC",
          [variant.id],
        );
        variants.push({
          ...variant,
          variant_id: variant.id,
          store_prices: decorateStorePriceList(storesRes.rows, todayIndia),
        });
      }

      const effectivePrice = resolveEffectiveSmartphonePrice(
        variants,
        item.price ?? null,
      );
      item.variants = variants;
      item.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
      item.price = effectivePrice;
      item.starting_price = effectivePrice;
      const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
      item.launch_status = launchStage;
      item.launchStatus = launchStage;
      applySmartphoneLaunchPolicy(item, launchStage);
    }

    return res.json({ new: launches });
  } catch (err) {
    console.error("GET /api/public/new/smartphones error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Latest Entries - Laptops
app.get("/api/public/new/laptops", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.name,
        p.product_type,
        b.name AS brand,
        b.name AS brand_name,
        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.software,
        l.physical,
        l.meta,
        l.spec_sections,
        COALESCE(l.created_at, p.created_at) AS created_at,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN laptop l ON l.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'laptop'
      ORDER BY COALESCE(l.created_at, p.created_at) DESC, p.id DESC
      LIMIT 20;
    `);

    const launches = applySpecScoreToRows(
      "laptop",
      (result.rows || []).map((row) => ({
        ...row,
        images: row?.image ? [row.image] : [],
        variants: [],
      })),
      profileConfig.profiles,
    ).map(toPublicLaptopResponseRow);

    return res.json({ new: launches });
  } catch (err) {
    console.error("GET /api/public/new/laptops error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - TVs
app.get("/api/public/new/tvs", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        b.name AS brand,
        t.category,
        t.model,
        COALESCE(t.created_at, p.created_at) AS launch_date,
        t.key_specs_json,
        t.basic_info_json,
        t.display_json,
        t.audio_json,
        t.smart_tv_json,
        t.connectivity_json,
        t.ports_json,
        t.power_json,
        t.warranty_json,
        COALESCE(
          (
            SELECT json_agg(pi.image_url ORDER BY pi.position ASC NULLS LAST, pi.id ASC)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          COALESCE(t.images_json, '[]'::jsonb)::json
        ) AS images_json,
        COALESCE(
          (
            SELECT json_agg(
              jsonb_build_object(
                'variant_id', v.id,
                'variant_key', v.variant_key,
                'screen_size', COALESCE(v.attributes->>'screen_size', v.attributes->>'size'),
                'screen_size_value', NULLIF(
                  regexp_replace(
                    COALESCE(v.attributes->>'screen_size', v.attributes->>'size', v.variant_key, ''),
                    '[^0-9.]',
                    '',
                    'g'
                  ),
                  ''
                )::numeric,
                'base_price', v.base_price,
                'images', (
                  CASE
                    WHEN EXISTS (
                      SELECT 1 FROM product_variant_images pvi0 WHERE pvi0.variant_id = v.id
                    )
                    THEN (
                      SELECT COALESCE(
                        json_agg(pvi.image_url ORDER BY pvi.position ASC NULLS LAST, pvi.id ASC),
                        '[]'::json
                      )
                      FROM product_variant_images pvi
                      WHERE pvi.variant_id = v.id
                    )
                    ELSE COALESCE(v.attributes->'images', v.attributes->'images_json', '[]'::jsonb)::json
                  END
                ),
                'store_prices', (
                  SELECT COALESCE(
                    json_agg(
                      jsonb_build_object(
                        'id', sp.id,
                        'store_name', sp.store_name,
                        'price', sp.price,
                        'url', sp.url,
                        'offer_text', sp.offer_text,
                        'delivery_info', sp.delivery_info
                      )
                      ORDER BY sp.price ASC NULLS LAST, sp.id ASC
                    ),
                    '[]'::json
                  )
                  FROM variant_store_prices sp
                  WHERE sp.variant_id = v.id
                )
              )
              ORDER BY v.id ASC
            )
            FROM product_variants v
            WHERE v.product_id = p.id
          ),
          COALESCE(t.variants_json, '[]'::jsonb)::json
        ) AS variants_json,
        (
          SELECT COALESCE(MIN(sp.price), MIN(v.base_price))
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN tvs t ON t.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'tv'
      ORDER BY COALESCE(t.created_at, p.created_at) DESC
      LIMIT 20;
    `);

    const launches = applySpecScoreToRows(
      "tv",
      result.rows || [],
      profileConfig.profiles,
    ).map(toPublicTvResponseRow);

    return res.json({ new: launches });
  } catch (err) {
    console.error("GET /api/public/new/tvs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Networking
app.get("/api/public/new/networking", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        n.created_at AS launch_date,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN networking n ON n.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'networking'
      ORDER BY COALESCE(n.created_at, p.created_at) DESC
      LIMIT 20;
    `);

    return res.json({ new: result.rows });
  } catch (err) {
    console.error("GET /api/public/new/networking error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending All Types (smartphones, laptops, networking, tvs, etc.)
app.get("/api/public/trending/all", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 50
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand,
        COALESCE(s.model, t.model, n.model_number, p.name) AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN smartphones s ON s.product_id = p.id
      LEFT JOIN tvs t ON t.product_id = p.id
      LEFT JOIN networking n ON n.product_id = p.id
      ORDER BY tp.views DESC, price ASC NULLS LAST;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/all error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/public/compare-pages/routes", async (_req, res) => {
  try {
    const pages = await readPublishedComparePages({
      publishedOnly: true,
      limit: 400,
    });

    return res.json({
      routes: pages.map((page) => ({
        slug: page.slug,
        route_path: page.route_path,
        title: page.title,
        meta_description: page.meta_description,
        updated_at: page.updated_at,
      })),
    });
  } catch (err) {
    console.error("GET /api/public/compare-pages/routes error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load compare page routes" });
  }
});

app.get("/api/public/compare-pages/resolve", async (req, res) => {
  try {
    const slug = normalizeComparePageSlugInput(req.query?.slug);
    if (!slug) {
      return res.status(400).json({ message: "slug is required" });
    }

    const pages = await readPublishedComparePages({
      slug,
      publishedOnly: true,
      limit: 1,
    });
    const page = pages[0] || null;
    if (!page) {
      return res.status(404).json({ message: "Compare page not found" });
    }

    return res.json({
      matched: true,
      page,
      compare_path: page.route_path,
    });
  } catch (err) {
    console.error("GET /api/public/compare-pages/resolve error:", err);
    return res.status(500).json({ message: "Failed to resolve compare page" });
  }
});

app.get("/api/public/compare/resolve", async (req, res) => {
  try {
    const normalizeCompareSlug = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/-price-in-india$/g, "")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "");

    const leftSlug = normalizeCompareSlug(req.query?.left);
    const rightSlug = normalizeCompareSlug(req.query?.right);
    if (!leftSlug || !rightSlug) {
      return res
        .status(400)
        .json({ message: "left and right slugs are required" });
    }

    const requestedType = String(req.query?.type || "")
      .trim()
      .toLowerCase();
    const allowedTypes = new Set(["smartphone", "laptop", "tv", "networking"]);
    const typeFilter = allowedTypes.has(requestedType) ? requestedType : null;

    const slugList = Array.from(new Set([leftSlug, rightSlug]));
    const params = [slugList];
    let typeWhere = "";
    if (typeFilter) {
      params.push(typeFilter);
      typeWhere = "AND p.product_type = $2";
    }

    const result = await db.query(
      `
      SELECT q.product_id, q.product_name, q.product_type, q.slug
      FROM (
        SELECT
          p.id AS product_id,
          p.name AS product_name,
          p.product_type,
          regexp_replace(
            regexp_replace(lower(coalesce(p.name, '')), '[^a-z0-9]+', '-', 'g'),
            '(^-|-$)',
            '',
            'g'
          ) AS slug
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        WHERE p.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
          ${typeWhere}
      ) q
      WHERE q.slug = ANY($1::text[])
      ORDER BY q.product_id DESC
      `,
      params,
    );

    const bySlug = new Map();
    for (const row of result.rows || []) {
      const key = String(row.slug || "").trim();
      if (!key || bySlug.has(key)) continue;
      bySlug.set(key, row);
    }

    const left = bySlug.get(leftSlug) || null;
    const right = bySlug.get(rightSlug) || null;
    const matched = Boolean(
      left &&
      right &&
      Number(left.product_id) !== Number(right.product_id) &&
      String(left.product_type || "") === String(right.product_type || ""),
    );

    return res.json({
      matched,
      left,
      right,
      compare_path: matched
        ? getComparePageRoutePath(
            buildComparePageSlug([left.product_name, right.product_name]),
          )
        : null,
    });
  } catch (err) {
    console.error("GET /api/public/compare/resolve error:", err);
    return res.status(500).json({ message: "Failed to resolve compare slugs" });
  }
});

app.post("/api/public/compare/scores", async (req, res) => {
  try {
    const body = req.body || {};
    const rawDevices = Array.isArray(body.devices)
      ? body.devices
      : Array.isArray(body.products)
        ? body.products
        : [];

    const normalizedDevices = [];
    const seenProductIds = new Set();

    for (const item of rawDevices) {
      const productIdRaw =
        typeof item === "number"
          ? item
          : (item?.product_id ?? item?.productId ?? item?.id);
      const productId = Number(productIdRaw);
      if (!Number.isInteger(productId) || productId <= 0) continue;
      if (seenProductIds.has(productId)) continue;

      const variantId = Number(item?.variant_id ?? item?.variantId);
      const variantIndex = Number(item?.variant_index ?? item?.variantIndex);

      const entry = { product_id: productId };
      if (Number.isInteger(variantId) && variantId > 0) {
        entry.variant_id = variantId;
      } else if (Number.isInteger(variantIndex) && variantIndex >= 0) {
        entry.variant_index = variantIndex;
      }

      normalizedDevices.push(entry);
      seenProductIds.add(productId);
    }

    if (normalizedDevices.length < 2 || normalizedDevices.length > 4) {
      return res.status(400).json({
        message: "Select minimum 2 and maximum 4 valid products",
      });
    }

    const productIds = normalizedDevices.map((entry) => entry.product_id);
    const productResult = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        COALESCE(s.performance, n.performance, l.cpu, t.video_engine_json, '{}'::jsonb) AS performance,
        COALESCE(s.display, l.display, t.display_json, '{}'::jsonb) AS display,
        COALESCE(s.camera, '{}'::jsonb) AS camera,
        COALESCE(s.battery, l.battery, t.power_json, '{}'::jsonb) AS battery,
        COALESCE(s.connectivity, l.connectivity, n.connectivity, t.connectivity_json, '{}'::jsonb) AS connectivity,
        COALESCE(s.network, '{}'::jsonb) AS network,
        COALESCE(s.build_design, '{}'::jsonb) AS build_design,
        COALESCE(s.audio, t.audio_json, '{}'::jsonb) AS audio,
        COALESCE(s.multimedia, '{}'::jsonb) AS multimedia,
        COALESCE(s.sensors, '{}'::jsonb) AS sensors,
        COALESCE(l.memory, '{}'::jsonb) AS memory,
        COALESCE(l.storage, '{}'::jsonb) AS storage,
        COALESCE(l.physical, t.physical_json, n.physical_details, '{}'::jsonb) AS physical,
        COALESCE(l.software, '{}'::jsonb) AS software,
        COALESCE(l.features, '{}'::jsonb) AS features,
        COALESCE(t.smart_tv_json, '{}'::jsonb) AS smart_features,
        COALESCE(t.gaming_json, '{}'::jsonb) AS gaming,
        COALESCE(t.ports_json, '{}'::jsonb) AS ports,
        COALESCE(n.specifications, '{}'::jsonb) AS specifications,
        COALESCE(n.features, '{}'::jsonb) AS networking_features,
        COALESCE(n.performance, '{}'::jsonb) AS networking_performance,
        (
          SELECT MIN(v.base_price)
          FROM product_variants v
          WHERE v.product_id = p.id
            AND v.base_price IS NOT NULL
        ) AS min_price,
        COALESCE(
          (
            SELECT json_agg(
              jsonb_build_object(
                'id', v.id,
                'base_price', v.base_price,
                'price', v.base_price,
                'attributes', v.attributes,
                'ram', v.attributes->>'ram',
                'storage', v.attributes->>'storage',
                'storage_type', v.attributes->>'storage_type'
              )
              ORDER BY v.id ASC
            )
            FROM product_variants v
            WHERE v.product_id = p.id
          ),
          '[]'::json
        ) AS variants
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN smartphones s
        ON s.product_id = p.id
      LEFT JOIN laptop l
        ON l.product_id = p.id
      LEFT JOIN networking n
        ON n.product_id = p.id
      LEFT JOIN tvs t
        ON t.product_id = p.id
      WHERE p.id = ANY($1::int[])
      `,
      [productIds],
    );

    if (productResult.rows.length < 2) {
      return res.status(404).json({ message: "Products not found" });
    }

    const productTypes = [
      ...new Set(
        (productResult.rows || [])
          .map((row) => String(row?.product_type || "").trim().toLowerCase())
          .filter(Boolean),
      ),
    ];

    if (productTypes.length > 1) {
      return res.status(400).json({
        message:
          "Comparison scoring is available only for devices from the same product type.",
      });
    }

    const compareConfig = await readCompareScoringConfig();
    const variantSelection = Object.fromEntries(
      normalizedDevices.map((entry) => [String(entry.product_id), entry]),
    );
    const analysis = buildCompareRanking(
      productResult.rows,
      variantSelection,
      compareConfig,
    );

    if (!analysis.ranking.length && analysis.warnings?.length) {
      return res.status(400).json({
        message: analysis.warnings[0],
        warnings: analysis.warnings,
      });
    }

    return res.json({
      score_version: "compare_v2",
      product_type: analysis.productType,
      overall_winner: analysis.overallWinner,
      category_winners: analysis.categoryWinners,
      warnings: analysis.warnings || [],
      scores: analysis.ranking.map((row) => ({
        product_id: Number(row.productId),
        overall_score: row.overallScore,
        rank: row.rank,
        confidence: row.confidence,
        price: row.price,
        reasons: row.reasons || [],
        breakdown: row.breakdown || {},
        details: row.details || {},
      })),
    });
  } catch (err) {
    console.error("POST /api/public/compare/scores error:", err);
    return res.status(500).json({ message: "Failed to score comparison" });
  }
});

app.post("/api/public/compare", async (req, res) => {
  try {
    // Support two payload shapes:
    // 1) { products: [1,2,3] } -> record pairwise comparisons (existing behavior)
    // 2) { left_product_id: 1, right_product_id: 2, product_type: 'smartphone' }
    const body = req.body || {};
    console.log("Comparison payload:", body);

    if (body.left_product_id && body.right_product_id) {
      const left = Number(body.left_product_id);
      const right = Number(body.right_product_id);
      if (
        !Number.isInteger(left) ||
        !Number.isInteger(right) ||
        left <= 0 ||
        right <= 0
      ) {
        return res.status(400).json({ message: "Invalid product ids" });
      }
      if (left === right) {
        return res
          .status(400)
          .json({ message: "Please compare two different products" });
      }

      const publishedPair = await db.query(
        `
        SELECT p.id
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        WHERE p.id = ANY($1::int[])
        `,
        [[left, right]],
      );
      if ((publishedPair.rows || []).length < 2) {
        return res.status(400).json({
          message: "Only published products can be compared",
        });
      }

      // normalize order so A vs B == B vs A
      const [l, r] = [left, right].sort((a, b) => a - b);

      try {
        await db.query(
          `INSERT INTO product_comparisons (product_id, compared_with)
           VALUES ($1, $2)`,
          [l, r],
        );
        return res.json({ message: "Comparison recorded" });
      } catch (err) {
        console.error("POST /api/public/compare insert failed:", err);
        return res.status(500).json({ message: "Failed to record comparison" });
      }
    }

    // Backwards-compatible: accept { products: [1,2,3...] }
    const raw = Array.isArray(body.products) ? body.products : [];
    // sanitize and normalize product ids
    const nums = raw
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n) && n > 0);
    const unique = Array.from(new Set(nums));

    if (unique.length < 2 || unique.length > 4) {
      return res.status(400).json({
        message: "Select minimum 2 and maximum 4 valid products",
      });
    }

    const publishedProducts = await db.query(
      `
      SELECT p.id
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      WHERE p.id = ANY($1::int[])
      `,
      [unique],
    );
    const publishedIds = new Set(
      (publishedProducts.rows || []).map((row) => Number(row.id)),
    );
    const filtered = unique.filter((id) => publishedIds.has(Number(id)));

    if (filtered.length !== unique.length) {
      return res.status(400).json({
        message: "Only published products can be compared",
      });
    }

    for (let i = 0; i < filtered.length; i++) {
      for (let j = i + 1; j < filtered.length; j++) {
        await db.query(
          `INSERT INTO product_comparisons (product_id, compared_with)
           VALUES ($1, $2)`,
          [filtered[i], filtered[j]],
        );
      }
    }

    return res.json({ message: "Comparison recorded" });
  } catch (err) {
    console.error("POST /api/public/compare error:", err);
    return res.status(500).json({ message: "Failed to record comparison" });
  }
});

app.get("/api/public/trending/most-compared", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p1.id AS product_id,
        p1.name AS product_name,
        p1.product_type AS product_type,
        (
          SELECT image_url
          FROM product_images
          WHERE product_id = p1.id
          ORDER BY position ASC NULLS LAST, id ASC
          LIMIT 1
        ) AS product_image,
        p2.id AS compared_product_id,
        p2.name AS compared_product_name,
        p2.product_type AS compared_product_type,
        (
          SELECT image_url
          FROM product_images
          WHERE product_id = p2.id
          ORDER BY position ASC NULLS LAST, id ASC
          LIMIT 1
        ) AS compared_product_image,
        COUNT(pc.id) AS compare_count
      FROM product_comparisons pc
      JOIN products p1 ON p1.id = pc.product_id
      JOIN products p2 ON p2.id = pc.compared_with
      INNER JOIN product_publish pub1
        ON pub1.product_id = p1.id
       AND pub1.is_published = true
      INNER JOIN product_publish pub2
        ON pub2.product_id = p2.id
       AND pub2.is_published = true
      WHERE pc.compared_at >= now() - INTERVAL '7 days'
        AND p1.product_type IN ('smartphone', 'laptop', 'tv')
        AND p2.product_type IN ('smartphone', 'laptop', 'tv')
      GROUP BY p1.id, p1.name, p1.product_type, p2.id, p2.name, p2.product_type
      ORDER BY compare_count DESC
    `);

    res.json({
      mostCompared: result.rows,
    });
  } catch (err) {
    console.error("Most compared error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get variants for a smartphone
app.get("/api/smartphone/:id/variants", async (req, res) => {
  try {
    const sid = Number(req.params.id);
    if (!sid) return res.status(400).json({ message: "Invalid id" });
    // Resolve product_id from smartphone id then fetch product_variants
    const sres = await db.query(
      "SELECT product_id FROM smartphones WHERE id = $1",
      [sid],
    );
    if (!sres.rows.length)
      return res.status(404).json({ message: "Smartphone not found" });
    const productId = sres.rows[0].product_id;
    const r = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );
    return res.json(r.rows);
  } catch (err) {
    console.error("GET variants error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Get store prices for variant
app.get("/api/variant/:id/store-prices", async (req, res) => {
  try {
    const vid = Number(req.params.id);
    if (!vid) return res.status(400).json({ message: "Invalid id" });
    const r = await db.query(
      "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
      [vid],
    );
    return res.json(decorateStorePriceList(r.rows));
  } catch (err) {
    console.error("GET variant store prices error:", err);
    return res.status(500).json({ error: err.message });
  }
});

const SMARTPHONE_DISCOVERY_BUDGET_SEGMENTS = [
  {
    key: "under_10000",
    label: "Under ₹10,000",
    path: "/smartphones/filter/under-10000",
  },
  {
    key: "under_15000",
    label: "Under ₹15,000",
    path: "/smartphones/filter/under-15000",
  },
  {
    key: "under_20000",
    label: "Under ₹20,000",
    path: "/smartphones/filter/under-20000",
  },
  {
    key: "under_25000",
    label: "Under ₹25,000",
    path: "/smartphones/filter/under-25000",
  },
  {
    key: "under_30000",
    label: "Under ₹30,000",
    path: "/smartphones/filter/under-30000",
  },
  {
    key: "under_40000",
    label: "Under ₹40,000",
    path: "/smartphones/filter/under-40000",
  },
  {
    key: "under_50000",
    label: "Under ₹50,000",
    path: "/smartphones/filter/under-50000",
  },
  {
    key: "above_50000",
    label: "Above ₹50,000",
    path: "/smartphones/filter/above-50000",
  },
];

const toSafeNumeric = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toProductSlug = (name, id) => {
  const slug = String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  return slug || `product-${id}`;
};

const toIsoDateOrNull = (value) => {
  if (!value) return null;
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return null;
  return dt.toISOString();
};

const mapDiscoveryProductRow = (row) => ({
  id: Number(row.product_id),
  name: row.name || row.product_name || "Device",
  slug: toProductSlug(row.name || row.product_name, row.product_id),
  brand_name: row.brand_name || null,
  image_url: row.image_url || null,
  price: toSafeNumeric(row.price),
  launch_date: toIsoDateOrNull(row.launch_date || row.created_at),
});

// PUBLIC: Product discovery sections (rule-based and cached-friendly)
app.get("/api/public/product/:id/discovery", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const brandLimitRaw = Number(req.query?.brand_limit);
    const latestLimitRaw = Number(req.query?.latest_limit);
    const brandHubLimitRaw = Number(req.query?.brand_hub_limit);
    const smartLinksLimitRaw = Number(req.query?.smart_links_limit);

    const brandLimit = Number.isFinite(brandLimitRaw)
      ? Math.min(12, Math.max(1, Math.floor(brandLimitRaw)))
      : 6;
    const latestLimit = Number.isFinite(latestLimitRaw)
      ? Math.min(12, Math.max(1, Math.floor(latestLimitRaw)))
      : 8;
    const brandHubLimit = Number.isFinite(brandHubLimitRaw)
      ? Math.min(20, Math.max(4, Math.floor(brandHubLimitRaw)))
      : 10;
    const smartLinksLimit = Number.isFinite(smartLinksLimitRaw)
      ? Math.min(12, Math.max(4, Math.floor(smartLinksLimitRaw)))
      : 8;

    const baseRes = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.created_at,
        p.brand_id,
        b.name AS brand_name,
        s.category,
        s.launch_date,
        COALESCE(
          (
            SELECT MIN(vsp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices vsp
              ON vsp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND vsp.price IS NOT NULL
          ),
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p.id
              AND pv.base_price IS NOT NULL
          )
        ) AS price
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN smartphones s
        ON s.product_id = p.id
      WHERE p.id = $1
        AND p.product_type = 'smartphone'
      LIMIT 1
      `,
      [id],
    );

    if (!baseRes.rows.length) {
      return res.status(404).json({ message: "Product not found" });
    }

    const current = baseRes.rows[0];
    const currentBrandId = Number(current.brand_id);
    const currentBrandName = String(current.brand_name || "").trim();
    const currentPrice = toSafeNumeric(current.price);

    let newFromBrandRows = [];
    if (Number.isInteger(currentBrandId) && currentBrandId > 0) {
      const brandRecentRes = await db.query(
        `
        SELECT
          p.id AS product_id,
          p.name,
          b.name AS brand_name,
          s.launch_date,
          p.created_at,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = p.id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          ) AS image_url,
          COALESCE(
            (
              SELECT MIN(vsp.price)::numeric
              FROM product_variants pv
              INNER JOIN variant_store_prices vsp
                ON vsp.variant_id = pv.id
              WHERE pv.product_id = p.id
                AND vsp.price IS NOT NULL
            ),
            (
              SELECT MIN(pv.base_price)::numeric
              FROM product_variants pv
              WHERE pv.product_id = p.id
                AND pv.base_price IS NOT NULL
            )
          ) AS price
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN smartphones s
          ON s.product_id = p.id
        WHERE p.product_type = 'smartphone'
          AND p.brand_id = $2
          AND p.id <> $1
          AND COALESCE(s.launch_date, p.created_at::date) >= CURRENT_DATE - INTERVAL '18 months'
        ORDER BY COALESCE(s.launch_date, p.created_at::date) DESC, p.id DESC
        LIMIT $3
        `,
        [id, currentBrandId, brandLimit],
      );

      newFromBrandRows = brandRecentRes.rows || [];
      if (!newFromBrandRows.length) {
        const brandFallbackRes = await db.query(
          `
          SELECT
            p.id AS product_id,
            p.name,
            b.name AS brand_name,
            s.launch_date,
            p.created_at,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = p.id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            ) AS image_url,
            COALESCE(
              (
                SELECT MIN(vsp.price)::numeric
                FROM product_variants pv
                INNER JOIN variant_store_prices vsp
                  ON vsp.variant_id = pv.id
                WHERE pv.product_id = p.id
                  AND vsp.price IS NOT NULL
              ),
              (
                SELECT MIN(pv.base_price)::numeric
                FROM product_variants pv
                WHERE pv.product_id = p.id
                  AND pv.base_price IS NOT NULL
              )
            ) AS price
          FROM products p
          INNER JOIN product_publish pub
            ON pub.product_id = p.id
           AND pub.is_published = true
          LEFT JOIN brands b
            ON b.id = p.brand_id
          LEFT JOIN smartphones s
            ON s.product_id = p.id
          WHERE p.product_type = 'smartphone'
            AND p.brand_id = $2
            AND p.id <> $1
          ORDER BY COALESCE(s.launch_date, p.created_at::date) DESC, p.id DESC
          LIMIT $3
          `,
          [id, currentBrandId, brandLimit],
        );
        newFromBrandRows = brandFallbackRes.rows || [];
      }
    }

    const latestRes = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        b.name AS brand_name,
        s.launch_date,
        p.created_at,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image_url,
        COALESCE(
          (
            SELECT MIN(vsp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices vsp
              ON vsp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND vsp.price IS NOT NULL
          ),
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p.id
              AND pv.base_price IS NOT NULL
          )
        ) AS price
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN smartphones s
        ON s.product_id = p.id
      WHERE p.product_type = 'smartphone'
        AND p.id <> $1
      ORDER BY COALESCE(s.launch_date, p.created_at::date) DESC, p.id DESC
      LIMIT $2
      `,
      [id, latestLimit],
    );

    const budgetCountsRes = await db.query(
      `
      WITH priced AS (
        SELECT
          p.id,
          COALESCE(
            (
              SELECT MIN(vsp.price)::numeric
              FROM product_variants pv
              INNER JOIN variant_store_prices vsp
                ON vsp.variant_id = pv.id
              WHERE pv.product_id = p.id
                AND vsp.price IS NOT NULL
            ),
            (
              SELECT MIN(pv.base_price)::numeric
              FROM product_variants pv
              WHERE pv.product_id = p.id
                AND pv.base_price IS NOT NULL
            )
          ) AS price
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        WHERE p.product_type = 'smartphone'
      )
      SELECT
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 10000)::int AS under_10000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 15000)::int AS under_15000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 20000)::int AS under_20000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 25000)::int AS under_25000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 30000)::int AS under_30000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 40000)::int AS under_40000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price <= 50000)::int AS under_50000,
        COUNT(*) FILTER (WHERE price IS NOT NULL AND price > 50000)::int AS above_50000
      FROM priced
      `,
    );

    const budgetCounts = budgetCountsRes.rows?.[0] || {};
    const budgetSegments = SMARTPHONE_DISCOVERY_BUDGET_SEGMENTS.map(
      (segment) => ({
        key: segment.key,
        label: segment.label,
        path: segment.path,
        product_count: Number(budgetCounts[segment.key]) || 0,
        active_for_current:
          currentPrice != null
            ? segment.key === "above_50000"
              ? currentPrice > 50000
              : currentPrice <= Number(segment.key.replace("under_", ""))
            : false,
      }),
    ).filter((item) => item.product_count > 0);

    const brandHubRes = await db.query(
      `
      WITH view_counts AS (
        SELECT product_id, COUNT(*)::int AS views_30d
        FROM product_views
        WHERE viewed_at >= now() - INTERVAL '30 days'
        GROUP BY product_id
      ),
      brand_rollup AS (
        SELECT
          b.id AS brand_id,
          b.name AS brand_name,
          b.logo AS logo_url,
          COUNT(DISTINCT p.id)::int AS product_count,
          COALESCE(SUM(vc.views_30d), 0)::int AS views_30d
        FROM brands b
        INNER JOIN products p
          ON p.brand_id = b.id
         AND p.product_type = 'smartphone'
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN view_counts vc
          ON vc.product_id = p.id
        GROUP BY b.id, b.name, b.logo
      )
      SELECT
        brand_id,
        brand_name,
        logo_url,
        product_count,
        views_30d,
        (views_30d * 2 + product_count)::int AS popularity_score
      FROM brand_rollup
      ORDER BY views_30d DESC, product_count DESC, brand_name ASC
      LIMIT $1
      `,
      [brandHubLimit],
    );

    const brandHub = (brandHubRes.rows || []).map((row) => ({
      brand_id: Number(row.brand_id),
      brand_name: row.brand_name,
      logo_url: row.logo_url || null,
      slug: String(row.brand_name || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, ""),
      product_count: Number(row.product_count) || 0,
      views_30d: Number(row.views_30d) || 0,
      popularity_score: Number(row.popularity_score) || 0,
      is_current_brand:
        currentBrandName &&
        String(row.brand_name || "").toLowerCase() ===
          currentBrandName.toLowerCase(),
    }));

    const nearestBudget = (() => {
      if (currentPrice == null) return null;
      if (currentPrice > 50000) {
        return SMARTPHONE_DISCOVERY_BUDGET_SEGMENTS.find(
          (segment) => segment.key === "above_50000",
        );
      }
      for (const segment of SMARTPHONE_DISCOVERY_BUDGET_SEGMENTS) {
        if (!segment.key.startsWith("under_")) continue;
        const max = Number(segment.key.replace("under_", ""));
        if (currentPrice <= max) return segment;
      }
      return null;
    })();

    const smartDiscoveriesRaw = [];
    if (currentBrandName) {
      const brandQuery = encodeURIComponent(currentBrandName);
      smartDiscoveriesRaw.push({
        key: "brand-latest",
        label: `Latest ${currentBrandName} Phones`,
        path: `/smartphones?brand=${brandQuery}&sort=newest`,
      });
      smartDiscoveriesRaw.push({
        key: "brand-5g",
        label: `${currentBrandName} 5G Phones`,
        path: `/smartphones?brand=${brandQuery}&network=5G`,
      });
      if (nearestBudget) {
        smartDiscoveriesRaw.push({
          key: "brand-budget",
          label: `${currentBrandName} ${nearestBudget.label}`,
          path: `${nearestBudget.path}?brand=${brandQuery}`,
        });
      }
    }
    if (nearestBudget) {
      smartDiscoveriesRaw.push({
        key: "budget-nearby",
        label: `Top Picks ${nearestBudget.label}`,
        path: nearestBudget.path,
      });
    }
    smartDiscoveriesRaw.push({
      key: "trending",
      label: "Trending Smartphones",
      path: "/trending/smartphones",
    });
    smartDiscoveriesRaw.push({
      key: "new-launches",
      label: "Latest Smartphone Launches",
      path: "/smartphones?filter=new",
    });
    smartDiscoveriesRaw.push({
      key: "compare",
      label: "Compare Smartphones",
      path: "/compare",
    });
    smartDiscoveriesRaw.push({
      key: "all-smartphones",
      label: "Explore All Smartphones",
      path: "/smartphones",
    });

    const seenSmartLinks = new Set();
    const smartDiscoveries = [];
    for (const item of smartDiscoveriesRaw) {
      const key = `${item.label}|${item.path}`;
      if (seenSmartLinks.has(key)) continue;
      seenSmartLinks.add(key);
      smartDiscoveries.push(item);
      if (smartDiscoveries.length >= smartLinksLimit) break;
    }

    return res.json({
      product_id: id,
      product_name: current.name,
      brand_name: currentBrandName || null,
      generated_at: new Date().toISOString(),
      sections: {
        new_from_brand: newFromBrandRows.map(mapDiscoveryProductRow),
        latest_releases: (latestRes.rows || []).map(mapDiscoveryProductRow),
        budget_segments: budgetSegments,
        brand_hub: brandHub,
        smart_discoveries: smartDiscoveries,
      },
    });
  } catch (err) {
    console.error("GET /api/public/product/:id/discovery error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load discovery sections" });
  }
});

// PUBLIC: Product competitor cards (precomputed competitor_analysis)
app.get("/api/public/product/:id/competitors", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid id" });

    const limitRaw = Number(req.query?.limit);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
      : 3;

    const productRes = await db.query(
      `
      SELECT p.id, p.name, p.product_type
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      WHERE p.id = $1
      LIMIT 1
      `,
      [id],
    );

    if (!productRes.rows.length) {
      return res.status(404).json({ message: "Product not found" });
    }

    const product = productRes.rows[0];
    if (product.product_type !== "smartphone") {
      return res.status(400).json({
        message:
          "Competitor cards are currently available for smartphones only",
      });
    }

    const fetchRows = async () => {
      const result = await db.query(
        `
        SELECT
          ca.product_id,
          ca.competitor_id,
          ca.competition_score,
          ca.spec_similarity_score,
          ca.price_proximity_score,
          ca.compare_frequency_score,
          ca.reason,
          ca.analysis_json,
          ca.computed_at,
          p.name,
          b.name AS brand_name,
          COALESCE(ds.hook_score, 0) AS hook_score,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = p.id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          ) AS image_url,
          (
            SELECT MIN(vsp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices vsp
              ON vsp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND vsp.price IS NOT NULL
          ) AS min_store_price,
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p.id
              AND pv.base_price IS NOT NULL
          ) AS min_base_price
        FROM competitor_analysis ca
        INNER JOIN products p
          ON p.id = ca.competitor_id
         AND p.product_type = 'smartphone'
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_dynamic_score ds
          ON ds.product_id = p.id
        WHERE ca.product_id = $1
        ORDER BY ca.competition_score DESC, ca.competitor_id ASC
        LIMIT $2
        `,
        [id, limit],
      );
      return result.rows || [];
    };

    let rows = await fetchRows();
    if (!rows.length) {
      try {
        await recomputeSmartphoneCompetitorAnalysis(db, {
          limit,
          productIds: [id],
        });
      } catch (err) {
        console.error("On-demand competitor recompute failed:", err);
      }
      rows = await fetchRows();
    }

    const toSafeNumber = (value, fallback = 0) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : fallback;
    };
    const toOneDecimal = (value, fallback = 0) =>
      Number(toSafeNumber(value, fallback).toFixed(1));
    const toNullableOneDecimal = (value) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? Number(parsed.toFixed(1)) : null;
    };

    const competitorIds = rows
      .map((row) => Number(row?.competitor_id))
      .filter((value) => Number.isInteger(value) && value > 0);

    let scoredByProductId = new Map();
    if (competitorIds.length) {
      const profileConfig = await readDeviceFieldProfilesConfig();
      const scoreRes = await db.query(
        `
        SELECT
          p.id AS product_id,
          p.name,
          p.product_type,

          b.name AS brand_name,

          s.category,
          s.model,
          s.launch_date,
          s.colors,
          s.build_design,
          s.display,
          s.performance,
          s.camera,
          s.battery,
          s.connectivity,
          s.network,
          s.ports,
          s.audio,
          s.multimedia,
          s.sensors,
          s.created_at,

          COALESCE(
            json_agg(
              DISTINCT jsonb_build_object(
                'variant_id', v.id,
                'ram', v.attributes->>'ram',
                'storage', v.attributes->>'storage',
                'base_price', v.base_price,
                'store_prices', (
                  SELECT COALESCE(
                    json_agg(
                      jsonb_build_object(
                        'id', sp.id,
                        'store_name', sp.store_name,
                        'price', sp.price,
                        'url', sp.url,
                        'offer_text', sp.offer_text,
                        'delivery_info', sp.delivery_info
                      )
                    ),
                    '[]'::json
                  )
                  FROM variant_store_prices sp
                  WHERE sp.variant_id = v.id
                )
              )
            ) FILTER (WHERE v.id IS NOT NULL),
            '[]'::json
          ) AS variants
        FROM products p
        INNER JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN product_variants v
          ON v.product_id = p.id
        WHERE p.product_type = 'smartphone'
          AND p.id = ANY($1::int[])
        GROUP BY
          p.id, b.name,
          s.category, s.model, s.launch_date,
          s.colors, s.build_design, s.display, s.performance,
          s.camera, s.battery, s.connectivity, s.network,
          s.ports, s.audio, s.multimedia, s.sensors, s.created_at
        `,
        [competitorIds],
      );

      const scoredRows = applySpecScoreToRows(
        "smartphone",
        (scoreRes.rows || []).map((row) => {
          const item = { ...(row || {}) };
          return stripScoreRecursively(item);
        }),
        profileConfig?.profiles,
      );

      scoredByProductId = new Map(
        scoredRows.map((row) => [Number(row?.product_id), row]),
      );
    }

    const competitors = rows.map((row) => {
      const storePrice = Number(row.min_store_price);
      const basePrice = Number(row.min_base_price);
      const price = Number.isFinite(storePrice)
        ? Number.isFinite(basePrice)
          ? Math.min(storePrice, basePrice)
          : storePrice
        : Number.isFinite(basePrice)
          ? basePrice
          : null;

      const analysis =
        row.analysis_json && typeof row.analysis_json === "object"
          ? row.analysis_json
          : {};
      const scored = scoredByProductId.get(Number(row.competitor_id)) || null;

      return {
        id: Number(row.competitor_id),
        name: row.name,
        brand_name: row.brand_name || null,
        image_url: row.image_url || null,
        price,
        hook_score: toOneDecimal(row.hook_score, 0),
        competition_score: toOneDecimal(row.competition_score, 0),
        spec_similarity_score: toOneDecimal(row.spec_similarity_score, 0),
        price_proximity_score: toOneDecimal(row.price_proximity_score, 0),
        compare_frequency_score: toOneDecimal(row.compare_frequency_score, 0),
        spec_score: toNullableOneDecimal(
          scored?.spec_score ?? scored?.specScore ?? null,
        ),
        overall_score: toNullableOneDecimal(
          scored?.overall_score ?? scored?.overallScore ?? null,
        ),
        spec_score_v2: toNullableOneDecimal(
          scored?.spec_score_v2 ?? scored?.specScoreV2 ?? null,
        ),
        overall_score_v2: toNullableOneDecimal(
          scored?.overall_score_v2 ?? scored?.overallScoreV2 ?? null,
        ),
        spec_score_v2_display_80_98: toNullableOneDecimal(
          scored?.spec_score_v2_display_80_98 ??
            scored?.specScoreV2Display8098 ??
            null,
        ),
        overall_score_v2_display_80_98: toNullableOneDecimal(
          scored?.overall_score_v2_display_80_98 ??
            scored?.overallScoreV2Display8098 ??
            null,
        ),
        overall_score_display: toNullableOneDecimal(
          scored?.overall_score_v2_display_80_98 ??
            scored?.overallScoreV2Display8098 ??
            null,
        ),
        reason:
          row.reason ||
          (typeof analysis.reason === "string" ? analysis.reason : null) ||
          "Similar price and specification profile",
        advantages: Array.isArray(analysis.advantages)
          ? analysis.advantages.slice(0, 3)
          : [],
        disadvantages: Array.isArray(analysis.disadvantages)
          ? analysis.disadvantages.slice(0, 3)
          : [],
        common_features: Array.isArray(analysis.common_features)
          ? analysis.common_features.slice(0, 3)
          : [],
        compare_count: Number(analysis.compare_count) || 0,
      };
    });

    return res.json({
      product_id: id,
      product_name: product.name,
      generated_at:
        rows[0]?.computed_at != null
          ? new Date(rows[0].computed_at).toISOString()
          : new Date().toISOString(),
      top_competitor: competitors[0] || null,
      other_competitors: competitors.slice(1),
      competitors,
    });
  } catch (err) {
    console.error("GET /api/public/product/:id/competitors error:", err);
    return res.status(500).json({ message: "Failed to load competitors" });
  }
});

// PUBLIC: Get smartphone/product details by ID (no auth required)
app.get("/api/public/product/:id", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid id" });

    console.log(`Fetching public product ${id}`);

    // Fetch product with all details
    const pRes = await db.query(
      `SELECT p.id, p.name, p.product_type, b.name AS brand, b.id AS brand_id, b.logo AS brand_logo, (to_jsonb(b)->>'website') AS brand_website
       FROM products p
       INNER JOIN product_publish pub
         ON pub.product_id = p.id
        AND pub.is_published = true
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.id = $1 LIMIT 1`,
      [id],
    );

    if (!pRes.rows.length) {
      console.log(`Product ${id} not found`);
      return res.status(404).json({ message: "Product not found" });
    }

    const product = pRes.rows[0];

    const scoreRes = await db.query(
      `SELECT hook_score, buyer_intent, trend_velocity, freshness, calculated_at
       FROM product_dynamic_score
       WHERE product_id = $1
       LIMIT 1`,
      [id],
    );
    const score = scoreRes.rows[0] || null;

    // Fetch product images
    const imgRes = await db.query(
      `SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC`,
      [id],
    );

    // Fetch product variants
    const varRes = await db.query(
      `SELECT id, variant_key, attributes->>'ram' AS ram, attributes->>'storage' AS storage, base_price
       FROM product_variants WHERE product_id = $1 ORDER BY id ASC`,
      [id],
    );
    const variants = [];
    const todayIndia = getIndiaDateOnly();
    for (const variant of varRes.rows) {
      const storesRes = await db.query(
        "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY price ASC NULLS LAST, id ASC",
        [variant.id],
      );
      variants.push({
        ...variant,
        store_prices: decorateStorePriceList(storesRes.rows, todayIndia),
      });
    }

    // For smartphones, fetch smartphone details
    let smartphoneDetails = null;
    if (product.product_type === "smartphone") {
      const smRes = await db.query(
        `SELECT * FROM smartphones WHERE product_id = $1 LIMIT 1`,
        [id],
      );
      if (smRes.rows.length) {
        smartphoneDetails = smRes.rows[0];
      }
    }

    // Merge all into a single flat object for compatibility with mapSingle
    const computedSlug = product.name
      ? String(product.name)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/(^-|-$)/g, "")
      : `product-${product.id}`;

    const responseData = {
      id: product.id,
      name: product.name,
      slug: computedSlug,
      product_type: product.product_type,
      brand: product.brand,
      brand_id: product.brand_id,
      brand_logo: product.brand_logo || null,
      brand_website: product.brand_website || null,
      images: imgRes.rows.map((r) => r.image_url),
      variants,
      ...(product.product_type === "smartphone"
        ? {
            hook_score: score?.hook_score ?? null,
            buyer_intent: score?.buyer_intent ?? null,
            trend_velocity: score?.trend_velocity ?? null,
            freshness: score?.freshness ?? null,
            hook_calculated_at: score?.calculated_at ?? null,
          }
        : {}),
      ...smartphoneDetails,
      // Include the smartphone object as well for backward compatibility
      smartphone: smartphoneDetails,
    };

    if (product.product_type === "smartphone") {
      const effectivePrice = resolveEffectiveSmartphonePrice(variants);
      responseData.price = effectivePrice;
      responseData.starting_price = effectivePrice;
      responseData.sale_start_date =
        getEarliestSaleStartDateFromVariants(variants);
      const launchStage = resolveSmartphoneLaunchStage(
        responseData,
        todayIndia,
      );
      responseData.launch_status = launchStage;
      responseData.launchStatus = launchStage;
      applySmartphoneLaunchPolicy(responseData, launchStage);
    }

    const scoredResponse = applySpecScoreToRow(
      product.product_type || "smartphone",
      stripScoreRecursively(responseData),
      profileConfig.profiles,
    );

    res.json(scoredResponse);
  } catch (err) {
    console.error("GET /api/public/product/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

async function runGlobalSearch(
  queryText,
  { publishedOnly = true, limit = 5 } = {},
) {
  const q = (queryText || "").trim();
  if (!q) return [];

  const normalizedQuery = q.toLowerCase();
  const containsTerm = `%${normalizedQuery}%`;
  const prefixTerm = `${normalizedQuery}%`;
  const wordPrefixTerm = `% ${normalizedQuery}%`;
  const resultLimit = Math.min(20, toPositiveInt(limit, 5));

  const publishFilter = publishedOnly
    ? `
       EXISTS (
         SELECT 1
         FROM product_publish pub
         WHERE pub.product_id = p.id
           AND pub.is_published = true
       )
      `
    : "TRUE";

  // Search products by name and brand with image
  const products = await db.query(
    `SELECT
      p.id,
      p.name,
      p.product_type,
      b.name AS brand_name,
      (SELECT image_url FROM product_images WHERE product_id = p.id AND position = 1 LIMIT 1) AS image_url,
      CASE
        WHEN LOWER(p.name) = $1 THEN 0
        WHEN LOWER(COALESCE(b.name, '')) = $1 THEN 1
        WHEN LOWER(p.name) LIKE $2 THEN 2
        WHEN LOWER(COALESCE(b.name, '')) LIKE $2 THEN 3
        WHEN LOWER(p.name) LIKE $3 THEN 4
        WHEN LOWER(p.name) LIKE $4 THEN 5
        WHEN LOWER(COALESCE(b.name, '')) LIKE $4 THEN 6
        ELSE 7
      END AS relevance_rank,
      CASE
        WHEN LOWER(p.name) LIKE $4 THEN POSITION($1 IN LOWER(p.name))
        ELSE 999
      END AS name_match_position
     FROM products p
     LEFT JOIN brands b ON b.id = p.brand_id
     WHERE ${publishFilter}
       AND (
         LOWER(p.name) LIKE $4
         OR LOWER(COALESCE(b.name, '')) LIKE $4
       )
     ORDER BY
       relevance_rank ASC,
       name_match_position ASC,
       LENGTH(p.name) ASC,
       p.name ASC
     LIMIT $5`,
    [normalizedQuery, prefixTerm, wordPrefixTerm, containsTerm, resultLimit],
  );

  const safeNum = (v) => {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const unique = (arr) => {
    const out = [];
    const seen = new Set();
    for (const x of arr || []) {
      const k = String(x || "").trim();
      if (!k || seen.has(k)) continue;
      seen.add(k);
      out.push(x);
    }
    return out;
  };

  const tryParseNumberFromString = (val) => {
    if (val === null || val === undefined) return null;
    if (typeof val === "number") return Number.isFinite(val) ? val : null;
    const m = String(val).match(/(\d{1,6})/);
    return m ? Number(m[1]) : null;
  };

  const extractSmartphoneHighlights = (row) => {
    const features = [];
    if (!row) return features;

    const display = row.display || {};
    const battery = row.battery || {};
    const camera = row.camera || {};
    const performance = row.performance || {};

    const displaySize =
      (display &&
        typeof display === "object" &&
        (display.size || display.screen_size || display.display_size)) ||
      (display && typeof display === "string" ? display : null);
    if (displaySize) features.push(String(displaySize));

    const battMah =
      tryParseNumberFromString(
        battery.battery_capacity_mah ??
          battery.capacity_mAh ??
          battery.capacity ??
          battery.battery_capacity ??
          battery,
      ) || null;
    if (battMah) features.push(`${battMah} mAh`);

    const mainMp =
      tryParseNumberFromString(
        camera.main_camera_megapixels ??
          camera.rear_camera?.main?.megapixels ??
          camera.rear_camera?.main?.resolution_mp ??
          camera.rear_camera?.main?.resolution ??
          camera.rear_camera?.main ??
          camera.main ??
          camera,
      ) || null;
    if (mainMp) features.push(`${mainMp} MP`);

    const processor =
      performance.processor || performance.cpu || performance.chipset || null;
    if (processor) features.push(String(processor));

    return features.filter(Boolean).slice(0, 3);
  };

  const results = [];

  // Add products to results
  for (const r of products.rows) {
    let minPrice = null;
    let variantTypes = [];
    let keyFeatures = [];

    try {
      const variantsRes = await db.query(
        `SELECT variant_key, attributes, base_price
         FROM product_variants
         WHERE product_id = $1
         ORDER BY id ASC`,
        [r.id],
      );

      const basePrices = variantsRes.rows
        .map((v) => safeNum(v.base_price))
        .filter((n) => n !== null);

      const minBase = basePrices.length > 0 ? Math.min(...basePrices) : null;

      const storeMinRes = await db.query(
        `SELECT MIN(vsp.price) AS min_price
         FROM variant_store_prices vsp
         INNER JOIN product_variants pv ON pv.id = vsp.variant_id
         WHERE pv.product_id = $1`,
        [r.id],
      );

      const minStore = safeNum(storeMinRes.rows?.[0]?.min_price);
      minPrice =
        minStore !== null && minBase !== null
          ? Math.min(minStore, minBase)
          : (minStore ?? minBase);

      variantTypes = unique(
        variantsRes.rows.map((v) => {
          const ram =
            v.attributes && typeof v.attributes === "object"
              ? v.attributes.ram || v.attributes.RAM || null
              : null;
          const storage =
            v.attributes && typeof v.attributes === "object"
              ? v.attributes.storage ||
                v.attributes.ROM_storage ||
                v.attributes.rom ||
                null
              : null;

          if (ram && storage) return `${ram}/${storage}`;
          if (ram) return String(ram);
          if (storage) return String(storage);
          return v.variant_key || null;
        }),
      ).slice(0, 3);
    } catch (e) {
      // defensive: search suggestions should not fail because of variant lookups
    }

    if (String(r.product_type).toLowerCase() === "smartphone") {
      try {
        const smRes = await db.query(
          `SELECT display, battery, camera, performance
           FROM smartphones
           WHERE product_id = $1
           LIMIT 1`,
          [r.id],
        );
        keyFeatures = extractSmartphoneHighlights(smRes.rows?.[0]);
      } catch (e) {
        // ignore highlight extraction errors
      }
    }

    results.push({
      type: "product",
      id: r.id,
      name: r.name,
      product_type: r.product_type,
      brand_name: r.brand_name || null,
      image_url: r.image_url || null,
      min_price: minPrice,
      variant_types: variantTypes,
      key_features: keyFeatures,
    });
  }

  return results;
}

// Public search: published products only
app.get("/api/search", async (req, res) => {
  try {
    const limit = Math.min(20, toPositiveInt(req.query.limit, 5));
    const results = await runGlobalSearch(req.query.q, {
      publishedOnly: true,
      limit,
    });
    res.json({ results });
  } catch (err) {
    console.error("GET /api/search error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin search: includes published + unpublished products
app.get("/api/search/admin", authenticate, async (req, res) => {
  try {
    const limit = Math.min(20, toPositiveInt(req.query.limit, 5));
    const results = await runGlobalSearch(req.query.q, {
      publishedOnly: false,
      limit,
    });
    res.json({ results });
  } catch (err) {
    console.error("GET /api/search/admin error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Wishlist
app.post("/api/wishlist", authenticateCustomer, async (req, res) => {
  const customerId = req.customer.id;
  const { product_id } = req.body;

  console.log(
    `Customer ${customerId} adding product ${product_id} to wishlist`,
  );

  if (!product_id) {
    return res.status(400).json({ message: "Product id required" });
  }

  try {
    await db.query(
      `
      INSERT INTO wishlist (customer_id, product_id)
      VALUES ($1, $2)
      ON CONFLICT (customer_id, product_id) DO NOTHING
      `,
      [customerId, product_id],
    );

    // Return the newly added wishlist item with product summary so client can append it
    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(v.base_price)
          FROM product_variants v
          WHERE v.product_id = p.id
        ) AS base_price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      WHERE p.id = $1
      `,
      [product_id],
    );

    const item = result.rows[0] || { product_id: product_id };
    res.json({ item });
  } catch (err) {
    console.error("POST /api/wishlist error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete(
  "/api/wishlist/:productId",
  authenticateCustomer,
  async (req, res) => {
    const customerId = req.customer.id;
    const productId = Number(req.params.productId);

    await db.query(
      `
      DELETE FROM wishlist
      WHERE customer_id = $1
        AND product_id = $2
      `,
      [customerId, productId],
    );

    res.json({ message: "Removed from wishlist" });
  },
);

app.get("/api/wishlist", authenticateCustomer, async (req, res) => {
  const customerId = req.customer.id;

  const result = await db.query(
    `
    SELECT
      p.id AS product_id,
      p.name,
      p.product_type,
      b.name AS brand_name,

      /* Images */
      COALESCE(
        (
          SELECT json_agg(pi.image_url)
          FROM product_images pi
          WHERE pi.product_id = p.id
        ),
        '[]'::json
      ) AS images,

      /* Rating */
      (
        SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
        FROM product_ratings r
        WHERE r.product_id = p.id
      ) AS rating,

      /* When was this item added to wishlist (latest) */
      MAX(w.created_at) AS added_at,

      /* Variants */
      COALESCE(
        json_agg(
          DISTINCT jsonb_build_object(
            'variant_id', v.id,
            'variant_key', v.variant_key,
            'base_price', v.base_price
          )
        ) FILTER (WHERE v.id IS NOT NULL),
        '[]'::json
      ) AS variants

    FROM wishlist w
    JOIN products p ON p.id = w.product_id
    LEFT JOIN brands b ON b.id = p.brand_id
    LEFT JOIN product_variants v ON v.product_id = p.id

    WHERE w.customer_id = $1
    GROUP BY p.id, p.name, p.product_type, p.brand_id, b.name, b.id
    ORDER BY added_at DESC;
  `,
    [customerId],
  );

  res.json({ wishlist: result.rows });
});

/* -----------------------
  Start server
------------------------*/
/* -----------------------
  RAM & Storage Config API
------------------------*/
// List configs
app.get("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, ram, storage, product_type, created_at FROM ram_storage_long ORDER BY id DESC`,
    );
    return res.json(result.rows);
  } catch (err) {
    console.error("Get ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Create config
app.post("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const { ram, storage } = req.body || {};
    const product_type = req.body.product_type || req.body.long || null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const result = await db.query(
      `INSERT INTO ram_storage_long (ram, storage, product_type) VALUES ($1,$2,$3) RETURNING id, ram, storage, product_type, created_at`,
      [ram, storage, product_type],
    );

    return res.status(201).json({ data: result.rows[0] });
  } catch (err) {
    console.error("Create ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Update config
app.put("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = req.params.id;
    const { ram, storage } = req.body || {};
    const product_type = req.body.product_type || req.body.long || null;

    const existing = await db.query(
      `SELECT id FROM ram_storage_long WHERE id = $1`,
      [id],
    );
    if (!existing.rows.length)
      return res.status(404).json({ message: "Not found" });

    const result = await db.query(
      `UPDATE ram_storage_long SET ram = $1, storage = $2, product_type = $3 WHERE id = $4 RETURNING id, ram, storage, product_type, created_at`,
      [ram || null, storage || null, product_type, id],
    );

    return res.json({ data: result.rows[0] });
  } catch (err) {
    console.error("Update ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Delete config
app.delete("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = req.params.id;
    await db.query(`DELETE FROM ram_storage_long WHERE id = $1`, [id]);
    return res.json({ message: "Deleted" });
  } catch (err) {
    console.error("Delete ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

const importSmartphonesRouter = require("./routes/importSmartphones");
const importLaptopsRouter = require("./routes/importLaptop");
const smartphonesReqRouter = require("./routes/smartphonesReq");
app.use("/api/import", authenticate, importSmartphonesRouter);
app.use("/api/import", authenticate, importLaptopsRouter);
app.use("/api/smartphones", authenticate, smartphonesReqRouter);

// ===== SPA CATCH-ALL ROUTE =====
// Serve index.html for any route that doesn't match an API endpoint.
// This allows React Router to handle client-side routing.
app.get("/{*splat}", (req, res) => {
  // Missing file requests should return 404 instead of HTML to avoid
  // module/CSS MIME mismatches when an outdated page references old assets.
  if (isDirectFileRequest(req.path)) {
    return res.status(404).end();
  }

  applyNoCacheHtmlHeaders(res);
  res.sendFile(path.join(distPath, "index.html"));
});

async function start() {
  try {
    // Wait for DB to be reachable before running migrations
    try {
      await db.waitForConnection(
        Number(process.env.DB_CONN_RETRIES) || 5,
        Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000,
      );
    } catch (err) {
      console.error("DB not reachable after retries:", err);
      throw err;
    }

    await runMigrations();

    // Optional: periodically recompute Hook Dynamic Score in-process.
    // In production, prefer an external scheduler calling the admin endpoint
    // or running the CLI script to avoid relying on a long-lived process.
    if (process.env.HOOK_SCORE_CRON_ENABLED === "true") {
      const defaultMs = 6 * 60 * 60 * 1000; // 6 hours
      const intervalRaw = Number(process.env.HOOK_SCORE_CRON_INTERVAL_MS);
      const intervalMs = Number.isFinite(intervalRaw)
        ? Math.max(15 * 60 * 1000, Math.floor(intervalRaw))
        : defaultMs;

      const run = async () => {
        try {
          const smartphones = await recomputeProductDynamicScoreSmartphones(db);
          const laptops = await recomputeProductDynamicScoreLaptops(db);
          const tvs = await recomputeProductDynamicScoreTVs(db);
          console.log("Hook score recompute:", {
            ok: true,
            updated:
              (smartphones.updated || 0) +
              (laptops.updated || 0) +
              (tvs.updated || 0),
            results: {
              smartphones,
              laptops,
              tvs,
            },
          });
        } catch (err) {
          console.error("Hook score recompute failed:", err);
        }
      };

      void run();
      const timer = setInterval(run, intervalMs);
      if (typeof timer.unref === "function") timer.unref();
      console.log("Hook score cron enabled:", { intervalMs });
    }

    // Optional: periodically recompute Trending Scores in-process.
    // In production, prefer an external scheduler calling the admin endpoint
    // or running the CLI script.
    if (process.env.TRENDING_SCORE_CRON_ENABLED === "true") {
      const defaultMs = 6 * 60 * 60 * 1000; // 6 hours
      const intervalRaw = Number(process.env.TRENDING_SCORE_CRON_INTERVAL_MS);
      const intervalMs = Number.isFinite(intervalRaw)
        ? Math.max(15 * 60 * 1000, Math.floor(intervalRaw))
        : defaultMs;

      const run = async () => {
        try {
          const r = await recomputeProductTrendingScores(db);
          console.log("Trending score recompute:", r);
        } catch (err) {
          console.error("Trending score recompute failed:", err);
        }
      };

      void run();
      const timer = setInterval(run, intervalMs);
      if (typeof timer.unref === "function") timer.unref();
      console.log("Trending score cron enabled:", { intervalMs });
    }

    // Optional: periodically recompute smartphone competitor analysis.
    // In production, prefer an external scheduler invoking the admin endpoint.
    if (process.env.COMPETITOR_ANALYSIS_CRON_ENABLED === "true") {
      const defaultMs = 24 * 60 * 60 * 1000; // daily
      const intervalRaw = Number(
        process.env.COMPETITOR_ANALYSIS_CRON_INTERVAL_MS,
      );
      const intervalMs = Number.isFinite(intervalRaw)
        ? Math.max(30 * 60 * 1000, Math.floor(intervalRaw))
        : defaultMs;

      const run = async () => {
        try {
          const limitRaw = Number(process.env.COMPETITOR_ANALYSIS_LIMIT);
          const limit = Number.isFinite(limitRaw)
            ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
            : 3;

          const result = await recomputeSmartphoneCompetitorAnalysis(db, {
            limit,
          });
          console.log("Competitor analysis recompute:", result);
          try {
            const syncResult = await syncAutomaticSmartphoneComparePages({
              recomputeIfMissing: false,
            });
            console.log("Automatic compare page sync after competitor recompute:", syncResult);
          } catch (syncErr) {
            console.error(
              "Automatic compare page sync after competitor recompute failed:",
              syncErr,
            );
          }
        } catch (err) {
          console.error("Competitor analysis recompute failed:", err);
        }
      };

      void run();
      const timer = setInterval(run, intervalMs);
      if (typeof timer.unref === "function") timer.unref();
      console.log("Competitor analysis cron enabled:", { intervalMs });
    }

    // Automatically generate and refresh SEO compare pages from competitor data
    // so newly inserted launch/spec details can appear within a few hours.
    if (process.env.AUTO_COMPARE_PAGES_CRON_ENABLED !== "false") {
      const defaultMs = 3 * 60 * 60 * 1000; // 3 hours
      const intervalRaw = Number(process.env.AUTO_COMPARE_PAGES_CRON_INTERVAL_MS);
      const intervalMs = Number.isFinite(intervalRaw)
        ? Math.max(30 * 60 * 1000, Math.floor(intervalRaw))
        : defaultMs;

      const run = async () => {
        try {
          const result = await syncAutomaticSmartphoneComparePages({
            recomputeIfMissing: true,
          });
          console.log("Automatic compare page sync:", result);
        } catch (err) {
          console.error("Automatic compare page sync failed:", err);
        }
      };

      void run();
      const timer = setInterval(run, intervalMs);
      if (typeof timer.unref === "function") timer.unref();
      console.log("Automatic compare page cron enabled:", { intervalMs });
    }
  } catch (err) {
    console.error("Migrations failed:", err);
    process.exit(1);
  }

  app.listen(PORT, "0.0.0.0", async () => {
    console.log(`Server running at ${PORT}`);
    try {
      const r = await db.query("SELECT now()");
      console.log("DB time:", r.rows[0].now);
    } catch (err) {
      console.error("DB health check failed:", err);
    }
  });
}

start();

module.exports = app;
