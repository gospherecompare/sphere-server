// index _fixed.js
const { projectRoot } = require("./bootstrap");

process.chdir(projectRoot);

require("dotenv").config();
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
  sendLoginOtpEmail,
  sendCareerApplicationEmail,
  sendCareerAssignmentEmail,
  sendCareerInterviewEmail,
  sendCareerHrEmail,
  sendCareerOfferEmail,
} = require("../utils/mailer");
const { authenticateCustomer, authenticate } = require("../middleware/auth");
const {
  recomputeProductDynamicScoreSmartphones,
  recomputeProductDynamicScoreLaptops,
  recomputeProductDynamicScoreTVs,
} = require("../utils/hookScore");
const { recomputeProductTrendingScores } = require("../utils/trendingScore");
const {
  normalizeCompareScoreConfig,
  buildCompareRanking,
  weightsToPercent,
} = require("../utils/compareScoring");
const {
  recomputeSmartphoneCompetitorAnalysis,
} = require("../utils/competitorAnalysis");
const {
  ROLE_PRESETS: RBAC_ROLE_PRESETS,
  expandPermissionSet: expandRbacPermissionSet,
  getDefaultPermissionsForRole,
  getPermissionMatrix: getRbacPermissionMatrix,
  hasPermissionSet: hasRbacPermissionSet,
  normalizePermissionToken: normalizeRbacPermissionToken,
  normalizeRole: normalizeRbacRole,
} = require("../utils/rbacCatalog");
const helmet = require("helmet");
const xss = require("xss-clean");
const { clean: xssClean } = require("xss-clean/lib/xss");

const SECRET = process.env.JWT_SECRET || "smartarena_secret_key_25";
const PORT = process.env.PORT || 5000;
const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "2mb";
const PROXY_EXTERNAL_BODY_LIMIT =
  process.env.PROXY_EXTERNAL_BODY_LIMIT || "50kb";
const COMPARE_DATA_RETENTION_DAYS = 548;
const PUBLIC_COMPARE_WINDOW_DAYS = 180;
const FRESH_COMPARE_WIDGET_DAYS = 7;
const COMPETITOR_ANALYSIS_REFRESH_DELAY_MS = 1500;
const COMPETITOR_ANALYSIS_MAX_AGE_MS = Math.max(
  30 * 60 * 1000,
  Number(process.env.COMPETITOR_ANALYSIS_MAX_AGE_MS) || 6 * 60 * 60 * 1000,
);

let competitorAnalysisRefreshTimer = null;
let competitorAnalysisRefreshRunning = false;
let competitorAnalysisRefreshPending = false;
const competitorAnalysisRefreshReasons = new Set();

const runScheduledSmartphoneCompetitorRefresh = async () => {
  if (competitorAnalysisRefreshRunning) {
    competitorAnalysisRefreshPending = true;
    return;
  }

  competitorAnalysisRefreshRunning = true;
  competitorAnalysisRefreshPending = false;
  const reasons = Array.from(competitorAnalysisRefreshReasons);
  competitorAnalysisRefreshReasons.clear();

  try {
    const limitRaw = Number(process.env.COMPETITOR_ANALYSIS_LIMIT);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
      : 5;
    const result = await recomputeSmartphoneCompetitorAnalysis(db, { limit });
    console.log("Automatic competitor refresh:", { reasons, ...result });
  } catch (err) {
    console.error("Automatic competitor refresh failed:", err);
  } finally {
    competitorAnalysisRefreshRunning = false;
    if (
      competitorAnalysisRefreshPending ||
      competitorAnalysisRefreshReasons.size > 0
    ) {
      competitorAnalysisRefreshTimer = setTimeout(
        runScheduledSmartphoneCompetitorRefresh,
        COMPETITOR_ANALYSIS_REFRESH_DELAY_MS,
      );
      if (typeof competitorAnalysisRefreshTimer.unref === "function") {
        competitorAnalysisRefreshTimer.unref();
      }
    }
  }
};

const scheduleSmartphoneCompetitorRefresh = (reason = "smartphone_changed") => {
  competitorAnalysisRefreshReasons.add(String(reason || "smartphone_changed"));
  if (competitorAnalysisRefreshRunning) {
    competitorAnalysisRefreshPending = true;
    return;
  }
  if (competitorAnalysisRefreshTimer) {
    clearTimeout(competitorAnalysisRefreshTimer);
  }
  competitorAnalysisRefreshTimer = setTimeout(
    runScheduledSmartphoneCompetitorRefresh,
    COMPETITOR_ANALYSIS_REFRESH_DELAY_MS,
  );
  if (typeof competitorAnalysisRefreshTimer.unref === "function") {
    competitorAnalysisRefreshTimer.unref();
  }
};

const app = express();

const payloadTooLargeHandler = (limit) => (err, _req, res, next) => {
  if (err?.type === "entity.too.large") {
    return res.status(413).json({
      message: `Request body too large. Maximum allowed size is ${limit}.`,
    });
  }
  return next(err);
};

app.set("trust proxy", 1);

const normalizeOrigin = (value) =>
  String(value || "")
    .trim()
    .replace(/\/$/, "");

const DEFAULT_ALLOWED_ORIGINS = [
  "http://localhost:3000",
  "http://localhost:5173",
  "https://workspace.tryhook.shop",
  "https://www.tryhook.shop",
  "https://tryhook.shop",
  "https://www.hooks.in",
  "https://hooks.in",
];

const ENV_ALLOWED_ORIGINS = [
  process.env.PUBLIC_SITE_ORIGIN,
  process.env.ADMIN_SITE_ORIGIN,
  process.env.CLIENT_SITE_ORIGIN,
  process.env.CLIENT_ADMIN_ORIGIN,
  ...String(
    process.env.CORS_ALLOWED_ORIGINS ||
      process.env.ALLOWED_ORIGINS ||
      process.env.WEBAUTHN_ALLOWED_ORIGINS ||
      "",
  ).split(","),
];

const ALLOWED_ORIGINS = new Set(
  [...DEFAULT_ALLOWED_ORIGINS, ...ENV_ALLOWED_ORIGINS]
    .map(normalizeOrigin)
    .filter(Boolean),
);

const ALLOWED_ORIGIN_HOST_SUFFIXES = [".tryhook.shop", ".hooks.in"];

const isAllowedOriginHost = (hostname) => {
  const normalizedHost = String(hostname || "")
    .trim()
    .toLowerCase();
  if (!normalizedHost) return false;
  if (
    normalizedHost === "localhost" ||
    normalizedHost === "127.0.0.1" ||
    normalizedHost === "::1"
  ) {
    return true;
  }
  return ALLOWED_ORIGIN_HOST_SUFFIXES.some((suffix) =>
    normalizedHost.endsWith(suffix),
  );
};

const isAllowedCorsOrigin = (origin) => {
  if (!origin) return true;

  const normalizedOrigin = normalizeOrigin(origin);
  if (!normalizedOrigin) return true;
  if (ALLOWED_ORIGINS.has(normalizedOrigin)) return true;

  try {
    const parsed = new URL(normalizedOrigin);
    return isAllowedOriginHost(parsed.hostname);
  } catch (error) {
    return false;
  }
};

app.use(
  cors({
    origin(origin, callback) {
      // Allow non-browser clients and our known first-party origins.
      if (!origin) return callback(null, true);
      if (isAllowedCorsOrigin(origin)) return callback(null, true);
      console.warn("CORS blocked origin:", normalizeOrigin(origin));
      return callback(null, false);
    },
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

// Security middlewares
app.disable("x-powered-by");
app.use(helmet());
app.use("/proxy/external", express.json({ limit: PROXY_EXTERNAL_BODY_LIMIT }));
app.use("/proxy/external", payloadTooLargeHandler(PROXY_EXTERNAL_BODY_LIMIT));
// Keep request bodies bounded while allowing normal admin/product JSON payloads.
app.use(express.json({ limit: REQUEST_BODY_LIMIT }));
// Parse URL-encoded bodies (for form submissions)
app.use(express.urlencoded({ extended: true, limit: REQUEST_BODY_LIMIT }));
app.use(payloadTooLargeHandler(REQUEST_BODY_LIMIT));

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

// Global rate limiting is not enabled, but targeted auth limits are applied below.

// important for preflight

const upload = multer({ storage: multer.memoryStorage() });

// Server-side proxy for external API calls to avoid CORS when needed.
// Example: POST /proxy/external/api/auth/login -> forwards to https://api.apisphere.in/api/auth/login
app.all(["/proxy/external", "/proxy/external/*proxyPath"], async (req, res) => {
  try {
    const targetBase = "https://api.apisphere.in";
    const targetPath = req.originalUrl.replace(/^\/proxy\/external/, "");
    const targetUrl = `${targetBase}${targetPath}`;

    const headers = { ...req.headers };
    // remove hop-by-hop headers that shouldn't be forwarded
    delete headers.host;
    delete headers.connection;
    delete headers.accept_encoding;
    delete headers.origin;

    const options = {
      method: req.method,
      headers,
    };

    if (req.method !== "GET" && req.method !== "HEAD") {
      // forward JSON bodies (most auth endpoints use JSON)
      if (req.is("application/json") || typeof req.body === "object") {
        options.body = JSON.stringify(req.body || {});
        options.headers["content-type"] = "application/json";
      }
    }

    const fetchRes = await fetch(targetUrl, options);
    const arrayBuffer = await fetchRes.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // Copy selected response headers
    fetchRes.headers.forEach((value, name) => {
      // skip transfer-encoding which may cause issues
      if (name.toLowerCase() === "transfer-encoding") return;
      res.setHeader(name, value);
    });

    res.status(fetchRes.status).send(buffer);
  } catch (err) {
    res.status(502).json({ error: "proxy_error", message: String(err) });
  }
});

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
]);
function normalizeLaunchStatusOverride(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return null;
  if (/rumou?r/.test(raw)) return "rumored";
  if (/announce/.test(raw)) return "announced";
  if (/(upcoming|coming soon|expected|scheduled)/i.test(raw)) return "upcoming";
  if (/(available|on sale|in stock)/i.test(raw)) return "available";
  if (/(released|launched|out now)/i.test(raw)) return "released";
  if (!LAUNCH_STATUS_VALUES.has(raw)) return null;
  return raw;
}

const resolveSmartphoneLaunchStage = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!device) return null;
  const override = normalizeLaunchStatusOverride(
    device.launch_status_override ||
      device.launchStatusOverride ||
      device.launch_status ||
      device.launchStatus,
  );
  const statusHint = normalizeLaunchStatusOverride(
    device.status || device.availability || device.badge || device.status_text,
  );
  const explicitStatus = override || statusHint;
  const dataAvailabilityStage = resolveSmartphoneDataAvailabilityStage(
    device,
    todayIndia,
  );

  if (dataAvailabilityStage) {
    return dataAvailabilityStage;
  }

  if (explicitStatus === "rumored" || explicitStatus === "announced") {
    return explicitStatus;
  }

  if (explicitStatus === "upcoming") {
    return "upcoming";
  }

  if (explicitStatus === "available") {
    return "available";
  }

  if (explicitStatus === "released") {
    return "released";
  }

  return "released";
};

const SMARTPHONE_COMPARE_LIMIT_DEFAULT = 4;
const SMARTPHONE_COMPETITOR_LIMIT_DEFAULT = 5;
const SMARTPHONE_SPEC_SCORE_MIN_IMPORTANT_INPUTS = 10;
const SMARTPHONE_IMPORTANT_SPEC_PATHS = {
  processor: ["performance.processor", "processor", "specs.processor"],
  gpu: ["performance.gpu", "performance.graphics", "specs.gpu"],
  ram: ["performance.ram", "variants[].ram", "specs.ram"],
  storage: ["performance.storage", "variants[].storage", "specs.storage"],
  display_size: ["display.size", "display.display_size", "specs.display_size"],
  display_resolution: ["display.resolution", "specs.resolution"],
  refresh_rate: [
    "display.refresh_rate",
    "display.refreshRate",
    "specs.refresh_rate",
  ],
  display_type: ["display.type", "display.panel_type", "display.display_type"],
  rear_camera: [
    "camera.rear_camera.main_camera.resolution",
    "camera.rear_camera.main.resolution",
    "camera.main_camera_megapixels",
    "camera.main_camera",
  ],
  front_camera: [
    "camera.front_camera.resolution",
    "camera.front_camera.megapixels",
    "camera.front_camera_megapixels",
    "camera.selfie_camera",
  ],
  battery_capacity: [
    "battery.capacity",
    "battery.battery_capacity",
    "battery.battery_capacity_mah",
  ],
  charging: [
    "battery.fast_charging",
    "battery.charging_speed",
    "battery.charging",
  ],
  operating_system: [
    "performance.operating_system",
    "performance.operatingSystem",
    "performance.os",
  ],
  network: [
    "connectivity.network_type",
    "network.network_type",
    "network.5g_support",
  ],
  weight: ["build_design.weight", "build_design.weight_g", "weight"],
  protection: [
    "build_design.ip_rating",
    "build_design.water_resistance",
    "display.protection",
  ],
};

const countSmartphoneImportantSpecInputs = (device) => {
  if (!device || typeof device !== "object") return 0;
  return Object.values(SMARTPHONE_IMPORTANT_SPEC_PATHS).filter(
    (paths) => resolveProfileValueByPaths(device, paths) != null,
  ).length;
};

const canShowUpcomingSmartphoneSpecScore = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  const launchDate = normalizeDateOnlyInput(
    device?.launch_date ?? device?.launchDate ?? null,
  );
  if (!launchDate || !todayIndia || launchDate > todayIndia) return false;

  return (
    countSmartphoneImportantSpecInputs(device) >=
    SMARTPHONE_SPEC_SCORE_MIN_IMPORTANT_INPUTS
  );
};

const resolveSmartphoneLaunchPolicy = (
  launchStage,
  device = null,
  todayIndia = getIndiaDateOnly(),
) => {
  const stage = normalizeLaunchStatusOverride(launchStage) || "released";
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

  if (stage === "upcoming") {
    return {
      ...base,
      allow_compare: false,
      allow_competitors: false,
      compare_limit: 0,
      competitor_limit: 0,
      allow_spec_score: canShowUpcomingSmartphoneSpecScore(device, todayIndia),
    };
  }

  if (stage === "announced") {
    return {
      ...base,
      compare_limit: 2,
      competitor_limit: 2,
      allow_spec_score: canShowUpcomingSmartphoneSpecScore(device, todayIndia),
    };
  }

  return base;
};

const applySmartphoneLaunchPolicy = (item, launchStage) => {
  if (!item) return item;
  const stage = normalizeLaunchStatusOverride(launchStage) || "released";
  const policy = resolveSmartphoneLaunchPolicy(stage, item);
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
  item.render_type =
    stage === "upcoming" || stage === "rumored" || stage === "announced"
      ? "upcoming"
      : "available";
  item.renderType = item.render_type;
  item.display_status =
    stage === "upcoming"
      ? "Upcoming"
      : stage === "rumored"
        ? "Rumored"
        : stage === "announced"
          ? "Announced"
          : "Available now";
  item.displayStatus = item.display_status;
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

const mergeSmartphoneUpdateBody = (value) => {
  const body = toPlainObject(value);
  const product = toPlainObject(body.product);
  const smartphone = toPlainObject(body.smartphone);
  return {
    ...body,
    ...product,
    ...smartphone,
  };
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

const toOfferPriceNumber = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const numeric =
    typeof value === "number"
      ? value
      : Number(String(value).replace(/[^0-9.]/g, ""));
  return Number.isFinite(numeric) ? numeric : null;
};

const hasSmartphonePurchaseSignal = (storePrice) => {
  const item = toPlainObject(storePrice);
  const price = toOfferPriceNumber(
    item.price ??
      item.current_price ??
      item.sale_price ??
      item.offer_price ??
      item.base_price,
  );
  const purchaseUrl = String(
    item.url ?? item.link ?? item.affiliate_link ?? item.affiliateUrl ?? "",
  ).trim();

  return Boolean(price || purchaseUrl);
};

const hasSmartphoneStoreEntrySignal = (storePrice) => {
  const item = toPlainObject(storePrice);
  return Boolean(
    hasSmartphonePurchaseSignal(item) ||
    String(
      item.store_name ??
        item.storeName ??
        item.store ??
        item.display_store_name ??
        item.displayStoreName ??
        item.logo ??
        item.store_logo ??
        item.storeLogo ??
        "",
    ).trim(),
  );
};

const hasFutureSmartphoneSaleDate = (
  value,
  todayIndia = getIndiaDateOnly(),
) => {
  const saleStartDate = normalizeDateOnlyInput(value);
  return Boolean(saleStartDate && todayIndia && saleStartDate > todayIndia);
};

const hasSmartphoneLiveStoreSignal = (
  storePrice,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!storePrice || typeof storePrice !== "object") return false;
  const saleStartDate = normalizeDateOnlyInput(
    storePrice.sale_start_date ??
      storePrice.saleStartDate ??
      storePrice.sale_date ??
      storePrice.saleDate ??
      storePrice.available_from ??
      storePrice.availableFrom ??
      null,
  );
  if (hasFutureSmartphoneSaleDate(saleStartDate, todayIndia)) return false;
  return hasSmartphonePurchaseSignal(storePrice);
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
  const isUpcomingSale = hasFutureSmartphoneSaleDate(saleStartDate, todayIndia);
  const isLive = !isUpcomingSale && hasSmartphonePurchaseSignal(item);

  return {
    ...item,
    sale_start_date: saleStartDate,
    availability_status: isUpcomingSale
      ? "upcoming"
      : isLive
        ? "live"
        : "listed",
    is_live: isLive,
    cta_label: isUpcomingSale
      ? "Upcoming"
      : isLive
        ? "Buy Now"
        : "View Details",
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

const getEarliestSaleStartDateFromStores = (storePrices) => {
  const dates = [];
  for (const store of Array.isArray(storePrices) ? storePrices : []) {
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
  if (!dates.length) return null;
  dates.sort();
  return dates[0];
};

const getSmartphoneSaleStartDate = (device = {}) =>
  normalizeDateOnlyInput(
    device?.sale_start_date ??
      device?.saleStartDate ??
      device?.sale_date ??
      device?.saleDate ??
      getEarliestSaleStartDateFromStores(
        device?.store_prices ?? device?.storePrices ?? [],
      ) ??
      getEarliestSaleStartDateFromVariants(device?.variants || []) ??
      null,
  );

const resolveEffectiveSmartphonePrice = (variants, fallbackPrice = null) => {
  const livePrices = [];
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
      livePrices.push(price);
    }
  }

  if (livePrices.length) return Math.min(...livePrices);
  if (basePrices.length) return Math.min(...basePrices);

  return toOfferPriceNumber(fallbackPrice);
};

function collectSmartphoneStoreRows(device = {}) {
  const rows = [];

  if (Array.isArray(device?.store_prices)) rows.push(...device.store_prices);
  if (Array.isArray(device?.storePrices)) rows.push(...device.storePrices);

  for (const variant of Array.isArray(device?.variants)
    ? device.variants
    : []) {
    const variantObj = toPlainObject(variant);
    if (Array.isArray(variantObj.store_prices)) {
      rows.push(...variantObj.store_prices);
    }
    if (Array.isArray(variantObj.storePrices)) {
      rows.push(...variantObj.storePrices);
    }
  }

  return rows.filter(Boolean);
}

const resolveSmartphoneDataAvailabilityStage = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!device) return null;
  const saleStartDate = getSmartphoneSaleStartDate(device);
  if (hasFutureSmartphoneSaleDate(saleStartDate, todayIndia)) {
    return "upcoming";
  }

  const storeRows = collectSmartphoneStoreRows(device);
  const hasStoreEntries = storeRows.some(hasSmartphoneStoreEntrySignal);
  if (!hasStoreEntries) return "upcoming";

  return "available";
};

function resolveSmartphoneSaleStage(device, todayIndia = getIndiaDateOnly()) {
  if (!device) return "sale_tbd";

  const saleStartDate = getSmartphoneSaleStartDate(device);
  const storeRows = collectSmartphoneStoreRows(device);
  const liveStores = storeRows.some((store) =>
    hasSmartphoneLiveStoreSignal(store, todayIndia),
  );
  const hasStoreSignals = storeRows.some(hasSmartphoneStoreEntrySignal);
  const launchStage = resolveSmartphoneLaunchStage(device, todayIndia);

  if (saleStartDate) {
    if (hasFutureSmartphoneSaleDate(saleStartDate, todayIndia))
      return "sale_scheduled";
    return liveStores ? "on_sale" : "sale_started";
  }

  const normalizedStatus = normalizeLaunchStatusOverride(
    device.launch_status_override ||
      device.launchStatusOverride ||
      device.launch_status ||
      device.launchStatus ||
      device.status ||
      "",
  );
  if (normalizedStatus === "available") return "on_sale";
  if (liveStores) return "on_sale";
  if (launchStage === "upcoming") return "sale_tbd";
  if (launchStage === "released" && hasStoreSignals) return "store_pending";
  return "sale_tbd";
}

const getSmartphoneFeedStartDate = (device) =>
  getSmartphoneSaleStartDate(device);

const SMARTPHONE_AVAILABILITY_FORECAST_TTL_MS = 10 * 60 * 1000;
let smartphoneAvailabilityForecastCache = {
  loadedAt: 0,
  byBrand: new Map(),
  globalMedianDays: 30,
};
let smartphoneAvailabilityForecastPromise = null;

const normalizeAvailabilityBrandKey = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const addDaysToDateOnly = (value, days) => {
  const normalized = normalizeDateOnlyInput(value);
  const numericDays = Number(days);
  if (!normalized || !Number.isFinite(numericDays)) return null;

  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;

  const date = new Date(
    Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])),
  );
  if (Number.isNaN(date.getTime())) return null;

  date.setUTCDate(date.getUTCDate() + Math.max(0, Math.round(numericDays)));
  return date.toISOString().slice(0, 10);
};

const fetchSmartphoneAvailabilityForecast = async () => {
  const now = Date.now();
  if (
    smartphoneAvailabilityForecastCache.loadedAt &&
    now - smartphoneAvailabilityForecastCache.loadedAt <
      SMARTPHONE_AVAILABILITY_FORECAST_TTL_MS
  ) {
    return smartphoneAvailabilityForecastCache;
  }

  if (smartphoneAvailabilityForecastPromise) {
    return smartphoneAvailabilityForecastPromise;
  }

  smartphoneAvailabilityForecastPromise = (async () => {
    const brandResult = await db.query(
      `
      WITH sale_rows AS (
        SELECT
          COALESCE(NULLIF(BTRIM(b.name), ''), 'Unknown') AS brand_name,
          s.launch_date::date AS launch_date,
          (
            SELECT MIN(sp.sale_start_date)
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND sp.sale_start_date IS NOT NULL
          ) AS sale_start_date
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        INNER JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN brands b
          ON b.id = p.brand_id
        WHERE p.product_type = 'smartphone'
          AND s.launch_date IS NOT NULL
      ),
      gap_rows AS (
        SELECT
          brand_name,
          (sale_start_date::date - launch_date::date)::int AS gap_days
        FROM sale_rows
        WHERE sale_start_date IS NOT NULL
          AND launch_date IS NOT NULL
      )
      SELECT
        brand_name,
        COUNT(*)::int AS sample_size,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY gap_days)::numeric AS median_gap_days
      FROM gap_rows
      GROUP BY brand_name
      `,
    );

    const globalResult = await db.query(
      `
      WITH sale_rows AS (
        SELECT
          s.launch_date::date AS launch_date,
          (
            SELECT MIN(sp.sale_start_date)
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND sp.sale_start_date IS NOT NULL
          ) AS sale_start_date
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        INNER JOIN smartphones s
          ON s.product_id = p.id
        WHERE p.product_type = 'smartphone'
          AND s.launch_date IS NOT NULL
      )
      SELECT
        COUNT(*)::int AS sample_size,
        percentile_cont(0.5) WITHIN GROUP (
          ORDER BY (sale_start_date::date - launch_date::date)::int
        )::numeric AS median_gap_days
      FROM sale_rows
      WHERE sale_start_date IS NOT NULL
        AND launch_date IS NOT NULL
      `,
    );

    const byBrand = new Map();
    for (const row of brandResult.rows || []) {
      const key = normalizeAvailabilityBrandKey(row?.brand_name);
      const median = Number(row?.median_gap_days);
      if (!key || !Number.isFinite(median)) continue;
      byBrand.set(key, Math.max(0, Math.round(median)));
    }

    const globalMedianRaw = Number(globalResult.rows?.[0]?.median_gap_days);
    const globalMedianDays = Number.isFinite(globalMedianRaw)
      ? Math.max(0, Math.round(globalMedianRaw))
      : 30;

    smartphoneAvailabilityForecastCache = {
      loadedAt: Date.now(),
      byBrand,
      globalMedianDays,
    };

    return smartphoneAvailabilityForecastCache;
  })().finally(() => {
    smartphoneAvailabilityForecastPromise = null;
  });

  return smartphoneAvailabilityForecastPromise;
};

const resolveSmartphoneAvailabilityFields = (
  device = {},
  forecast = smartphoneAvailabilityForecastCache,
  todayIndia = getIndiaDateOnly(),
) => {
  const launchDate = normalizeDateOnlyInput(
    device.launch_date ??
      device.launchDate ??
      device.created_at ??
      device.createdAt ??
      null,
  );
  const saleStartDate = getSmartphoneSaleStartDate(device);
  const isUpcomingSale = hasFutureSmartphoneSaleDate(saleStartDate, todayIndia);
  const availableDate =
    saleStartDate && saleStartDate <= todayIndia ? saleStartDate : null;
  const predictedAvailableDate = isUpcomingSale ? saleStartDate : null;
  const availableDateSource = availableDate ? "sale_start_date" : null;
  const availableDateLabel = isUpcomingSale
    ? "Upcoming"
    : availableDate
      ? "Available"
      : "Not set";

  return {
    brand_logo_url:
      device.brand_logo_url || device.brand_logo || device.brandLogo || null,
    brand_logo:
      device.brand_logo || device.brand_logo_url || device.brandLogo || null,
    best_price: toSafeNumeric(
      device.best_price ?? device.price ?? device.starting_price ?? null,
    ),
    bestPrice: toSafeNumeric(
      device.best_price ?? device.price ?? device.starting_price ?? null,
    ),
    available_date: availableDate,
    availableDate: availableDate,
    predicted_available_date: predictedAvailableDate,
    predictedAvailableDate: predictedAvailableDate,
    available_date_source: availableDateSource,
    availableDateSource: availableDateSource,
    available_date_label: availableDateLabel,
    availableDateLabel: availableDateLabel,
  };
};

const resolveSmartphoneStoreStage = (
  device = {},
  todayIndia = getIndiaDateOnly(),
) => {
  const storeRows = collectSmartphoneStoreRows(device);
  const saleStartDate = getSmartphoneSaleStartDate(device);
  if (hasFutureSmartphoneSaleDate(saleStartDate, todayIndia)) {
    return storeRows.some(hasSmartphoneStoreEntrySignal) ? "scheduled" : "none";
  }
  if (
    storeRows.some((store) => hasSmartphoneLiveStoreSignal(store, todayIndia))
  ) {
    return "live";
  }
  if (storeRows.length > 0) {
    return "listed";
  }
  return "none";
};

const applySmartphoneAvailabilityDetails = (
  item,
  forecast = smartphoneAvailabilityForecastCache,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!item || typeof item !== "object") return item;

  const availabilityFields = resolveSmartphoneAvailabilityFields(
    item,
    forecast,
    todayIndia,
  );
  Object.assign(item, availabilityFields);

  const saleStage = resolveSmartphoneSaleStage(item, todayIndia);
  item.sale_status = saleStage;
  item.saleStatus = saleStage;
  item.store_stage = resolveSmartphoneStoreStage(item, todayIndia);
  item.storeStage = item.store_stage;

  return item;
};

const isSmartphoneLatestFeedItem = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!device) return false;
  const saleStartDate = getSmartphoneFeedStartDate(device);
  const storeRows = collectSmartphoneStoreRows(device);
  if (resolveSmartphoneLaunchStage(device, todayIndia) === "upcoming") {
    return false;
  }
  const hasLiveStores = storeRows.some((store) =>
    hasSmartphoneLiveStoreSignal(store, todayIndia),
  );
  if (saleStartDate) {
    return saleStartDate <= todayIndia;
  }

  const launchStage = resolveSmartphoneLaunchStage(device, todayIndia);
  if (launchStage === "available") return true;
  if (launchStage === "released") return hasLiveStores;
  return ["available"].includes(launchStage);
};

const isSmartphoneUpcomingFeedItem = (
  device,
  todayIndia = getIndiaDateOnly(),
) => {
  if (!device) return false;
  const launchStage = resolveSmartphoneLaunchStage(device, todayIndia);
  return (
    launchStage === "upcoming" ||
    launchStage === "rumored" ||
    launchStage === "announced"
  );
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

const normalizePublicSpecScoreKey = (key) =>
  String(key || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const PUBLIC_SMARTPHONE_RESPONSE_EXACT_EXCLUDE_KEYS = new Set([
  "launchStatus",
  "official_preorder_url",
  "officialPreorderUrl",
  "allowCompare",
  "allowCompetitors",
  "compareLimit",
  "competitorLimit",
  "allowSpecScore",
]);

const PUBLIC_SMARTPHONE_RESPONSE_NORMALIZED_EXCLUDE_KEYS = new Set([
  "hookscore",
  "hookssscore",
  "buyerintent",
  "trendvelocity",
  "freshness",
  "hookcalculatedat",
  "hooksscalculatedat",
  "hookrankscore",
  "competitionscore",
  "specsimilarityscore",
  "priceproximityscore",
  "comparefrequencyscore",
  "trendscore",
  "trendingscore",
  "trendviews7d",
  "trendviewsprev7d",
  "trendmanualboost",
  "trendmanualpriority",
  "trendmanualbadge",
  "trendcalculatedat",
  "officialpreorderurl",
]);

const PUBLIC_SPEC_SCORE_EXCLUDE_KEYS = new Set([
  "fieldprofile",
  "specscoresource",
  "overallscore",
  "overallscoresource",
  "specscorev2raw",
  "specscorev2",
  "specscorev2source",
  "overallscorev2",
  "overallscorev2source",
  "specscorev2display8098",
  "overallscorev2display8098",
  "specscoredisplay",
  "overallscoredisplay",
  "specscoreprice",
  "specscorepriceband",
  "specscorefeaturecoverage",
  "camerascorev2raw",
  "camerascorev2display8099",
  "spectierv2",
  "mandatorycoverage",
  "displaycoverage",
  "sectionscores",
]);

const stripPublicSpecScoreDecorations = (value) => {
  if (value instanceof Date) return value;
  if (Array.isArray(value)) {
    return value.map((item) => stripPublicSpecScoreDecorations(item));
  }
  if (!value || typeof value !== "object") return value;

  const cleaned = {};
  for (const [key, val] of Object.entries(value)) {
    const normalized = normalizePublicSpecScoreKey(key);
    if (normalized === "score") continue;
    if (PUBLIC_SPEC_SCORE_EXCLUDE_KEYS.has(normalized)) continue;
    cleaned[key] = stripPublicSpecScoreDecorations(val);
  }
  return cleaned;
};

const stripPublicSmartphoneBusinessFields = (value) => {
  if (value instanceof Date) return value;
  if (Array.isArray(value)) {
    return value.map((item) => stripPublicSmartphoneBusinessFields(item));
  }
  if (!value || typeof value !== "object") return value;

  const cleaned = {};
  for (const [key, val] of Object.entries(value)) {
    if (PUBLIC_SMARTPHONE_RESPONSE_EXACT_EXCLUDE_KEYS.has(key)) continue;

    const normalized = normalizePublicSpecScoreKey(key);
    if (PUBLIC_SMARTPHONE_RESPONSE_NORMALIZED_EXCLUDE_KEYS.has(normalized)) {
      continue;
    }

    cleaned[key] = stripPublicSmartphoneBusinessFields(val);
  }
  return cleaned;
};

const toNullableOneDecimalNumber = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Number(parsed.toFixed(1)) : null;
};

const SMARTPHONE_PUBLIC_SCORE_MIN = 72;
const SMARTPHONE_PUBLIC_SCORE_MAX = 98;

const normalizePublicScoreSource = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const toPublicAlgorithmScore = (value, source) => {
  const normalized = toNullableOneDecimalNumber(value);
  if (normalized == null) return null;

  const sourceKey = normalizePublicScoreSource(source);
  if (sourceKey.includes("fallback") || sourceKey.includes("unavailable")) {
    return null;
  }

  return normalized;
};

const resolvePublicSmartphoneSpecScore = (
  value,
  { allowLegacySpecScore = false, useDisplayBand = false } = {},
) => {
  if (!value || typeof value !== "object") return null;

  const candidates = [
    toPublicAlgorithmScore(
      value.spec_score_v2,
      value.spec_score_v2_source ?? value.specScoreV2Source,
    ),
    toPublicAlgorithmScore(
      value.specScoreV2,
      value.spec_score_v2_source ?? value.specScoreV2Source,
    ),
    toPublicAlgorithmScore(
      value.spec_score_v2_raw,
      value.spec_score_v2_source ?? value.specScoreV2Source,
    ),
    toPublicAlgorithmScore(
      value.specScoreV2Raw,
      value.spec_score_v2_source ?? value.specScoreV2Source,
    ),
  ];

  if (allowLegacySpecScore) {
    candidates.push(
      toPublicAlgorithmScore(
        value.spec_score,
        value.spec_score_source ?? value.specScoreSource,
      ),
      toPublicAlgorithmScore(
        value.specScore,
        value.spec_score_source ?? value.specScoreSource,
      ),
    );
  }

  for (const candidate of candidates) {
    if (candidate != null) {
      return useDisplayBand
        ? mapScoreToDisplayBand(
            candidate,
            SMARTPHONE_PUBLIC_SCORE_MIN,
            SMARTPHONE_PUBLIC_SCORE_MAX,
          )
        : candidate;
    }
  }
  return null;
};

const toPublicSmartphoneResponse = (value) => {
  const withoutBusinessFields = stripPublicSmartphoneBusinessFields(value);
  const resolvedSpecScore = resolvePublicSmartphoneSpecScore(
    withoutBusinessFields,
    {
      useDisplayBand: true,
    },
  );
  const publicRow = stripPublicSpecScoreDecorations(withoutBusinessFields);

  if (resolvedSpecScore != null) {
    publicRow.spec_score = resolvedSpecScore;
  } else if (Object.prototype.hasOwnProperty.call(publicRow, "spec_score")) {
    publicRow.spec_score = toNullableOneDecimalNumber(publicRow.spec_score);
  }

  return publicRow;
};

const PUBLIC_TV_RESPONSE_EXACT_EXCLUDE_KEYS = new Set(
  PUBLIC_SMARTPHONE_RESPONSE_EXACT_EXCLUDE_KEYS,
);

const PUBLIC_TV_RESPONSE_NORMALIZED_EXCLUDE_KEYS = new Set([
  ...PUBLIC_SMARTPHONE_RESPONSE_NORMALIZED_EXCLUDE_KEYS,
  "views7d",
  "viewsprev7d",
  "velocity",
  "manualboost",
  "manualpriority",
  "manualbadge",
  "trenddelta",
  "trendingcalculatedat",
]);

const stripPublicTvBusinessFields = (value) => {
  if (value instanceof Date) return value;
  if (Array.isArray(value)) {
    return value.map((item) => stripPublicTvBusinessFields(item));
  }
  if (!value || typeof value !== "object") return value;

  const cleaned = {};
  for (const [key, val] of Object.entries(value)) {
    if (PUBLIC_TV_RESPONSE_EXACT_EXCLUDE_KEYS.has(key)) continue;

    const normalized = normalizePublicSpecScoreKey(key);
    if (PUBLIC_TV_RESPONSE_NORMALIZED_EXCLUDE_KEYS.has(normalized)) {
      continue;
    }

    cleaned[key] = stripPublicTvBusinessFields(val);
  }
  return cleaned;
};

const toPublicTvResponse = (value) => {
  const withoutBusinessFields = stripPublicTvBusinessFields(value);
  const resolvedSpecScore = resolvePublicSmartphoneSpecScore(
    withoutBusinessFields,
    { allowLegacySpecScore: true },
  );
  const publicRow = stripPublicSpecScoreDecorations(withoutBusinessFields);

  if (resolvedSpecScore != null) {
    publicRow.spec_score = resolvedSpecScore;
  } else if (Object.prototype.hasOwnProperty.call(publicRow, "spec_score")) {
    publicRow.spec_score = toNullableOneDecimalNumber(publicRow.spec_score);
  }

  return publicRow;
};

const normalizeManualTrendBadge = (value) => {
  if (value === null || value === undefined) return null;
  const badge = String(value).trim().replace(/\s+/g, " ").slice(0, 64);
  return badge || null;
};

const resolveAutomaticTrendBadge = ({ rank, hookScore, trendScore } = {}) => {
  const rankNumber = Number(rank);
  const hook = Number(hookScore);
  const trend = Number(trendScore);
  const hasHook = Number.isFinite(hook) && hook > 0;
  const hasTrend = Number.isFinite(trend) && trend > 0;

  if (Number.isFinite(rankNumber) && rankNumber === 1 && hasHook) {
    return "Top Trending";
  }
  if (Number.isFinite(rankNumber) && rankNumber <= 3 && hasHook) {
    return "Hot Trending";
  }
  if (hasHook && hook >= 85) return "Trending Now";
  if (hasHook && hook >= 70) return "Rising Fast";
  if (hasTrend && trend >= 60) return "Popular This Week";
  return "Trending";
};

const resolvePublicTrendBadge = ({
  manualBoost,
  manualBadge,
  rank,
  hookScore,
  trendScore,
} = {}) => {
  const cleanManualBadge = normalizeManualTrendBadge(manualBadge);
  if (manualBoost && cleanManualBadge) return cleanManualBadge;
  return resolveAutomaticTrendBadge({ rank, hookScore, trendScore });
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

const SPEC_SCORE_ALGORITHM_UPDATED_AT =
  process.env.SPEC_SCORE_ALGORITHM_UPDATED_AT || "2026-06-13";

const percentLabel = (value) => `${Number(value * 100).toFixed(0)}%`;

const buildFieldRows = (fields = {}) =>
  Object.entries(fields || {}).map(([key, paths]) => ({
    key,
    value: Array.isArray(paths) ? paths.join(", ") : String(paths || ""),
    note: Array.isArray(paths)
      ? `${paths.length} source path${paths.length === 1 ? "" : "s"} checked`
      : "1 source path checked",
  }));

const buildSpecScoreAlgorithmResponse = (profileConfig = {}) => {
  const profiles = normalizeDeviceFieldProfilesConfig(profileConfig.profiles);
  const profileUpdatedAt = profileConfig.updated_at || null;
  const coverageWeights = [
    {
      key: "mandatory_coverage",
      value: "75%",
      note: "Core fields that must exist for a product to look complete.",
    },
    {
      key: "display_coverage",
      value: "25%",
      note: "Extra product-page display fields that improve completeness.",
    },
  ];

  return {
    success: true,
    updated_at: SPEC_SCORE_ALGORITHM_UPDATED_AT,
    generated_at: new Date().toISOString(),
    categories: [
      {
        id: "smartphone",
        label: "Smartphone",
        model: "V2 raw spec score with runtime segment learning",
        status: "Active",
        updated_at: SPEC_SCORE_ALGORITHM_UPDATED_AT,
        public_display_band: `${SMARTPHONE_PUBLIC_SCORE_MIN}-${SMARTPHONE_PUBLIC_SCORE_MAX}`,
        score_outputs: [
          {
            key: "raw_spec_score",
            value: "0-100",
            note: "Internal technical score calculated from actual specs.",
          },
          {
            key: "learned_spec_score",
            value: "0-100",
            note: "Runtime peer percentile score for same segment, brand segment, or latest global fallback.",
          },
          {
            key: "display_spec_score",
            value: `${SMARTPHONE_PUBLIC_SCORE_MIN}-${SMARTPHONE_PUBLIC_SCORE_MAX}`,
            note: "Frontend-safe public score mapped from the internal raw/learned score.",
          },
        ],
        weights: [
          {
            key: "processor",
            value: percentLabel(0.24),
            note: "Chipset tier from processor text.",
          },
          {
            key: "display",
            value: percentLabel(0.16),
            note: "Refresh rate normalized from 60Hz to 165Hz.",
          },
          {
            key: "camera",
            value: percentLabel(0.3),
            note: "Camera quality score first, megapixel fallback second.",
          },
          {
            key: "battery",
            value: percentLabel(0.14),
            note: "Battery capacity normalized from 3000mAh to 7500mAh.",
          },
          {
            key: "charging",
            value: percentLabel(0.06),
            note: "Charging speed normalized from 10W to 150W.",
          },
          {
            key: "ram",
            value: percentLabel(0.06),
            note: "RAM normalized from 4GB to 24GB.",
          },
          {
            key: "storage",
            value: percentLabel(0.04),
            note: "Storage normalized from 64GB to 1024GB.",
          },
        ],
        learning: [
          {
            key: "brand_segment_peer_min",
            value: String(SMARTPHONE_BRAND_SEGMENT_PEER_MIN),
            note: "Minimum same-brand, same-price-band peers before brand learning is used.",
          },
          {
            key: "segment_peer_min",
            value: String(SMARTPHONE_SEGMENT_PEER_MIN),
            note: "Minimum same-price-band peers before segment learning is used.",
          },
          {
            key: "global_peer_min",
            value: String(SMARTPHONE_GLOBAL_PEER_MIN),
            note: "Minimum latest global peers before fallback learning is used.",
          },
          {
            key: "recency_half_life_days",
            value: String(SMARTPHONE_RECENCY_HALF_LIFE_DAYS),
            note: "Newer phones carry more peer-learning weight.",
          },
          {
            key: "brand_segment_blend",
            value: "58%",
            note: "Brand segment percentile share when enough brand peers exist.",
          },
          {
            key: "segment_blend",
            value: "42%",
            note: "Segment percentile share when brand segment learning also exists.",
          },
          {
            key: "final_peer_raw_blend",
            value: "68% peer percentile + 32% raw score",
            note: "Keeps current market context without losing real technical strength.",
          },
        ],
        notes: [
          "Smartphone learning is recalculated at response time when multiple smartphone rows are scored together.",
          "The learned value is not stored as a permanent database score by this endpoint.",
          "Rumored and announced launch policies can hide public spec score rendering.",
        ],
      },
      {
        id: "laptop",
        label: "Laptop",
        model: "Server profile coverage score",
        status: "Server computed",
        updated_at: profileUpdatedAt,
        public_display_band: "0-100",
        score_outputs: [
          {
            key: "spec_score",
            value: "0-100",
            note: "Generated on the server from mandatory/display field coverage when no stored score is supplied.",
          },
          {
            key: "overall_score",
            value: "0-100",
            note: "Uses supplied overall score, supplied spec score, or the server profile score.",
          },
        ],
        weights: coverageWeights,
        mandatory_fields: buildFieldRows(profiles.laptop?.mandatory),
        display_fields: buildFieldRows(profiles.laptop?.display),
        learning: [
          {
            key: "runtime_learning",
            value: "Not enabled",
            note: "Laptop scores do not currently use segment/brand peer percentile learning.",
          },
        ],
        notes: [
          "Laptop scoring currently measures spec data completeness, not performance power.",
          "Profile paths come from the server device field profile config.",
        ],
      },
      {
        id: "tv",
        label: "TV",
        model: "Server profile coverage score",
        status: "Server computed",
        updated_at: profileUpdatedAt,
        public_display_band: "0-100",
        score_outputs: [
          {
            key: "spec_score",
            value: "0-100",
            note: "Generated on the server from mandatory/display field coverage when no stored score is supplied.",
          },
          {
            key: "overall_score",
            value: "0-100",
            note: "Uses supplied overall score, supplied spec score, or the server profile score.",
          },
        ],
        weights: coverageWeights,
        mandatory_fields: buildFieldRows(profiles.tv?.mandatory),
        display_fields: buildFieldRows(profiles.tv?.display),
        learning: [
          {
            key: "runtime_learning",
            value: "Not enabled",
            note: "TV scores do not currently use segment/brand peer percentile learning.",
          },
        ],
        notes: [
          "TV scoring currently measures spec data completeness, not panel/video quality strength.",
          "Profile paths come from the server device field profile config.",
        ],
      },
    ],
  };
};

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

const computeSmartphoneRawSpecScoreV2 = (source) => {
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

  let rawScore = null;
  let sourceKey = "model_v2_feature_raw";

  if (weightTotal > 0) {
    rawScore = weightedTotal / weightTotal;
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

const SMARTPHONE_SEGMENT_PEER_MIN = 6;
const SMARTPHONE_BRAND_SEGMENT_PEER_MIN = 3;
const SMARTPHONE_GLOBAL_PEER_MIN = 8;
const SMARTPHONE_RECENCY_HALF_LIFE_DAYS = 540;

const normalizeSmartphoneScoreBrand = (row) => {
  const value =
    row?.brand_name ?? row?.brandName ?? row?.brand ?? row?.manufacturer ?? "";
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  return normalized || "unknown";
};

const parseSmartphoneScoreDateMs = (row) => {
  const candidates = [
    row?.sale_start_date,
    row?.saleStartDate,
    row?.available_date,
    row?.availableDate,
    row?.launch_date,
    row?.launchDate,
    row?.created_at,
    row?.createdAt,
  ];

  for (const candidate of candidates) {
    if (!candidate) continue;
    const parsed = new Date(candidate);
    const ms = parsed.getTime();
    if (Number.isFinite(ms)) return ms;
  }
  return null;
};

const getSmartphoneScoreRecencyWeight = (row, nowMs = Date.now()) => {
  const dateMs = parseSmartphoneScoreDateMs(row);
  if (dateMs == null) return 1;

  const ageDays = Math.max(0, (nowMs - dateMs) / 86400000);
  const recency = Math.exp(-ageDays / SMARTPHONE_RECENCY_HALF_LIFE_DAYS);
  return Number(
    Math.max(0.55, Math.min(1.35, 0.55 + recency * 0.8)).toFixed(3),
  );
};

const addSmartphoneScoreBucketEntry = (buckets, key, entry) => {
  const bucketKey = String(key || "unknown");
  if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
  buckets.get(bucketKey).push(entry);
};

const computeWeightedPercentileScore = (value, peerEntries) => {
  const target = Number(value);
  if (!Number.isFinite(target)) return null;

  const peers = (Array.isArray(peerEntries) ? peerEntries : [])
    .map((entry) => ({
      raw: Number(entry?.raw),
      weight: Number(entry?.weight),
    }))
    .filter(
      (entry) =>
        Number.isFinite(entry.raw) &&
        Number.isFinite(entry.weight) &&
        entry.weight > 0,
    )
    .sort((a, b) => a.raw - b.raw);

  if (!peers.length) return null;
  if (peers.length === 1) return 100;

  const totalWeight = peers.reduce((sum, entry) => sum + entry.weight, 0);
  if (!Number.isFinite(totalWeight) || totalWeight <= 0) return null;

  let belowWeight = 0;
  let equalWeight = 0;
  for (const entry of peers) {
    if (entry.raw < target) {
      belowWeight += entry.weight;
    } else if (entry.raw === target) {
      equalWeight += entry.weight;
    }
  }

  const percentile = ((belowWeight + equalWeight / 2) / totalWeight) * 100;
  return Number(Math.max(0, Math.min(100, percentile)).toFixed(1));
};

const resolveSmartphoneContextScore = (entry, buckets) => {
  const parts = [];
  const brandSegmentKey = `${entry.band}::${entry.brand}`;
  const brandBucket = buckets.brandSegment.get(brandSegmentKey) || [];
  const segmentBucket = buckets.segment.get(entry.band) || [];
  const globalBucket = buckets.global;

  if (
    entry.brand !== "unknown" &&
    brandBucket.length >= SMARTPHONE_BRAND_SEGMENT_PEER_MIN
  ) {
    const percentile = computeWeightedPercentileScore(entry.raw, brandBucket);
    if (percentile != null) {
      parts.push({
        score: percentile,
        weight: 0.58,
        source: "brand_segment",
      });
    }
  }

  if (segmentBucket.length >= SMARTPHONE_SEGMENT_PEER_MIN) {
    const percentile = computeWeightedPercentileScore(entry.raw, segmentBucket);
    if (percentile != null) {
      parts.push({
        score: percentile,
        weight: parts.length ? 0.42 : 1,
        source: "segment",
      });
    }
  }

  if (!parts.length && globalBucket.length >= SMARTPHONE_GLOBAL_PEER_MIN) {
    const percentile = computeWeightedPercentileScore(entry.raw, globalBucket);
    if (percentile != null) {
      parts.push({
        score: percentile,
        weight: 1,
        source: "latest",
      });
    }
  }

  if (!parts.length) return null;

  const weightTotal = parts.reduce((sum, part) => sum + part.weight, 0);
  if (!Number.isFinite(weightTotal) || weightTotal <= 0) return null;

  const percentile =
    parts.reduce((sum, part) => sum + part.score * part.weight, 0) /
    weightTotal;
  const blended = Number((percentile * 0.68 + entry.raw * 0.32).toFixed(1));
  const learned = Number(
    (Math.pow(Math.max(0, Math.min(100, blended)) / 100, 1.06) * 100).toFixed(
      1,
    ),
  );

  return {
    score: learned,
    source: `model_v2_${parts.map((part) => part.source).join("_")}_percentile`,
  };
};

const applySpecScoreToRow = (type, row, profiles) => {
  if (!row || typeof row !== "object" || Array.isArray(row)) return row;

  const source = buildSpecScoreSource(type, row);
  const normalizedType = normalizeProfileDeviceType(
    type || source?.product_type,
  );
  const fieldProfile = resolveDeviceFieldProfileScore(type, source, profiles);

  const providedSpecScore = toFiniteScore100(row.spec_score ?? row.specScore);
  const allowProfileFallbackScore = normalizedType !== "smartphone";
  let specScore =
    providedSpecScore != null
      ? providedSpecScore
      : allowProfileFallbackScore
        ? fieldProfile.score
        : null;
  let specScoreSource =
    providedSpecScore != null
      ? "provided"
      : allowProfileFallbackScore && specScore != null
        ? "profile_fallback"
        : "model_v2_unavailable";

  const providedOverallScore = toFiniteScore100(
    row.overall_score ?? row.overallScore,
  );
  let overallScore =
    providedOverallScore != null ? providedOverallScore : specScore;
  let overallScoreSource =
    providedOverallScore != null
      ? "provided"
      : providedSpecScore != null
        ? "derived_from_spec_score"
        : allowProfileFallbackScore && specScore != null
          ? "profile_fallback"
          : "model_v2_unavailable";

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

  if (normalizedType === "smartphone") {
    const v2 = computeSmartphoneRawSpecScoreV2(source);
    specScoreV2 = toFiniteScore100(v2.rawScore);
    specScoreV2Raw = specScoreV2;
    specScoreV2Source = v2.source;
    overallScoreV2 = specScoreV2;
    overallScoreV2Source =
      specScoreV2 != null ? "model_v2_raw" : "model_v2_unavailable";
    specScore = specScoreV2;
    specScoreSource = specScoreV2Source;
    overallScore = specScoreV2;
    overallScoreSource = overallScoreV2Source;
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
  const specScoreDisplay =
    normalizedType === "smartphone"
      ? mapScoreToDisplayBand(
          specScoreV2 ?? specScore,
          SMARTPHONE_PUBLIC_SCORE_MIN,
          SMARTPHONE_PUBLIC_SCORE_MAX,
        )
      : specScoreV2Display8098 != null
        ? specScoreV2Display8098
        : specScore;
  const overallScoreDisplay =
    normalizedType === "smartphone"
      ? mapScoreToDisplayBand(
          overallScoreV2 ?? overallScore,
          SMARTPHONE_PUBLIC_SCORE_MIN,
          SMARTPHONE_PUBLIC_SCORE_MAX,
        )
      : overallScoreV2Display8098 != null
        ? overallScoreV2Display8098
        : overallScore;

  return {
    ...row,
    camera: cameraWithScore,
    camera_json: cameraJsonWithScore,
    field_profile: fieldProfile,
    spec_score: specScore,
    spec_score_source: specScoreSource,
    overall_score: overallScore,
    overall_score_source: overallScoreSource,
    spec_score_v2_raw: specScoreV2Raw,
    spec_score_v2: specScoreV2,
    spec_score_v2_source: specScoreV2Source,
    overall_score_v2: overallScoreV2,
    overall_score_v2_source: overallScoreV2Source,
    spec_score_v2_display_80_98: specScoreV2Display8098,
    overall_score_v2_display_80_98: overallScoreV2Display8098,
    spec_score_display: specScoreDisplay,
    overall_score_display: overallScoreDisplay,
    spec_score_price: specScorePrice,
    spec_score_price_band: specScorePriceBand,
    spec_score_feature_coverage: specFeatureCoverage,
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

  const buckets = {
    brandSegment: new Map(),
    segment: new Map(),
    global: [],
  };
  const entries = [];
  const nowMs = Date.now();

  scoredRows.forEach((row, index) => {
    const raw = toFiniteScore100(row?.spec_score_v2_raw);
    if (raw == null) return;

    const sourceKey = String(row?.spec_score_v2_source || "").toLowerCase();
    if (sourceKey.includes("fallback") || sourceKey.includes("unavailable"))
      return;

    const entry = {
      index,
      raw,
      band: row?.spec_score_price_band || "unknown",
      brand: normalizeSmartphoneScoreBrand(row),
      weight: getSmartphoneScoreRecencyWeight(row, nowMs),
    };

    entries.push(entry);
    buckets.global.push(entry);
    addSmartphoneScoreBucketEntry(buckets.segment, entry.band, entry);
    addSmartphoneScoreBucketEntry(
      buckets.brandSegment,
      `${entry.band}::${entry.brand}`,
      entry,
    );
  });

  const updated = new Map();
  entries.forEach((entry) => {
    const context = resolveSmartphoneContextScore(entry, buckets);
    if (!context) return;

    const display8098 = mapScoreToDisplayBand(context.score);
    const publicDisplay = mapScoreToDisplayBand(
      context.score,
      SMARTPHONE_PUBLIC_SCORE_MIN,
      SMARTPHONE_PUBLIC_SCORE_MAX,
    );

    updated.set(entry.index, {
      spec_score: context.score,
      spec_score_source: context.source,
      overall_score: context.score,
      overall_score_source: context.source,
      spec_score_v2: context.score,
      overall_score_v2: context.score,
      spec_score_v2_source: context.source,
      overall_score_v2_source: context.source,
      spec_score_v2_display_80_98: display8098,
      overall_score_v2_display_80_98: display8098,
      spec_score_display: publicDisplay,
      overall_score_display: publicDisplay,
      spec_tier_v2: toSpecTier(context.score),
    });
  });

  return scoredRows.map((row, index) => {
    const patch = updated.get(index);
    return patch ? { ...row, ...patch } : row;
  });
};

const BLOG_ALLOWED_PRODUCT_TYPES = new Set(["smartphone", "laptop", "tv"]);
const BLOG_ALLOWED_STATUSES = new Set(["draft", "published"]);

const normalizeBlogTextField = (value, maxLength = 500) =>
  String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, maxLength);

const normalizeBlogTagsInput = (value) => {
  const rawItems = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(/[,;\n]+/)
      : [];

  const seen = new Set();
  const tags = [];
  for (const item of rawItems) {
    const label = normalizeBlogTextField(item, 48);
    const key = label.toLowerCase();
    if (!label || seen.has(key)) continue;
    seen.add(key);
    tags.push(label);
    if (tags.length >= 30) break;
  }
  return tags;
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

const escapeBlogHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

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
    return escapeBlogHtml(value);
  });
};

const ensureBlogManagerAccess = (req, res) => {
  const role = normalizeRbacRole(req?.user?.role || "");
  if (
    role === "admin" ||
    role === "ceo" ||
    role === "editor" ||
    role === "content_admin"
  )
    return true;
  res.status(403).json({ message: "Admin or editor access required" });
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
    hero_image: images[0] || null,
  };
};

const buildBlogTokenMap = (snapshot) => {
  const scored = toPlainObject(snapshot?.scored);
  const display = toPlainObject(scored.field_profile?.display_display);
  const mandatory = toPlainObject(scored.field_profile?.mandatory_display);
  const tokenMap = {};

  const setToken = (key, value) => {
    const normalizedKey = normalizeBlogTokenKey(key);
    if (!normalizedKey) return;
    const formatted = formatBlogValue(value);
    if (!formatted) return;
    tokenMap[normalizedKey] = formatted;
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
    const existingProductId = Number(existing.rows[0]?.product_id);
    if (
      Number.isInteger(Number(blogId)) &&
      Number(blogId) > 0 &&
      existingId === Number(blogId)
    ) {
      return slug;
    }
    if (hasProductId && existingProductId === Number(productId)) return slug;
    slug = `${baseSlug}-${counter}`;
    counter += 1;
  }
};

const normalizePositiveIntegerList = (values = []) =>
  Array.from(
    new Set(
      (Array.isArray(values) ? values : [values])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );

const readBlogLinkedProductIds = async (
  clientOrDb,
  blogId,
  fallbackProductId = null,
) => {
  const normalizedBlogId = Number(blogId);
  if (!Number.isInteger(normalizedBlogId) || normalizedBlogId <= 0) return [];

  const result = await clientOrDb.query(
    `
    SELECT product_id
    FROM blog_product_links
    WHERE blog_id = $1
    ORDER BY position ASC, product_id ASC
  `,
    [normalizedBlogId],
  );

  const linkedIds = normalizePositiveIntegerList(
    (result.rows || []).map((row) => row.product_id),
  );
  if (linkedIds.length) return linkedIds;
  return normalizePositiveIntegerList([fallbackProductId]);
};

const syncBlogProductLinks = async (clientOrDb, blogId, productIds = []) => {
  const normalizedBlogId = Number(blogId);
  if (!Number.isInteger(normalizedBlogId) || normalizedBlogId <= 0) return [];

  const normalizedIds = normalizePositiveIntegerList(productIds);
  if (!normalizedIds.length) {
    await clientOrDb.query(
      `DELETE FROM blog_product_links WHERE blog_id = $1`,
      [normalizedBlogId],
    );
    await clientOrDb.query(
      `
      UPDATE blogs
      SET product_id = NULL,
          blog_eligible = false
      WHERE id = $1
    `,
      [normalizedBlogId],
    );
    return [];
  }

  await clientOrDb.query(
    `
    DELETE FROM blog_product_links
    WHERE blog_id = $1
      AND NOT (product_id = ANY($2::int[]))
  `,
    [normalizedBlogId, normalizedIds],
  );

  await clientOrDb.query(
    `UPDATE blog_product_links SET is_primary = false WHERE blog_id = $1`,
    [normalizedBlogId],
  );

  for (const [index, productId] of normalizedIds.entries()) {
    await clientOrDb.query(
      `
      INSERT INTO blog_product_links (
        blog_id,
        product_id,
        position,
        is_primary
      ) VALUES ($1, $2, $3, $4)
      ON CONFLICT (blog_id, product_id)
      DO UPDATE SET
        position = EXCLUDED.position,
        is_primary = EXCLUDED.is_primary
    `,
      [normalizedBlogId, productId, index, index === 0],
    );
  }

  await clientOrDb.query(
    `
    UPDATE blogs
    SET product_id = $2,
        blog_eligible = true
    WHERE id = $1
  `,
    [normalizedBlogId, normalizedIds[0]],
  );

  return normalizedIds;
};

const readBlogPrimaryProductId = async (clientOrDb, blogId) => {
  const result = await clientOrDb.query(
    `SELECT product_id FROM blogs WHERE id = $1 LIMIT 1`,
    [blogId],
  );
  return Number(result.rows[0]?.product_id) || null;
};

const mapBlogLinkRows = (rows = []) =>
  (rows || []).map((row) => ({
    id: Number(row.id) || null,
    title: String(row.title || "").trim(),
    slug: String(row.slug || "").trim(),
    excerpt: String(row.excerpt || "").trim(),
    category: String(row.category || "").trim(),
    status: String(row.status || "draft")
      .trim()
      .toLowerCase(),
    is_published:
      typeof row.is_published === "boolean"
        ? row.is_published
        : String(row.status || "").toLowerCase() === "published",
    author_name: String(row.author_name || "").trim(),
    author_user_id: Number(row.author_user_id) || null,
    hero_image: row.hero_image || null,
    hero_image_source: String(row.hero_image_source || "").trim(),
    hero_image_alt: String(row.hero_image_alt || "").trim(),
    hero_image_caption: String(row.hero_image_caption || "").trim(),
    tags: normalizeBlogTagsInput(row.tags),
    featured: Boolean(row.featured),
    trending: Boolean(row.trending),
    pinned: Boolean(row.pinned),
    published_at: row.published_at || null,
    updated_at: row.updated_at || null,
    primary_product_id: Number(row.primary_product_id) || null,
    primary_product_name: String(row.primary_product_name || "").trim(),
    primary_product_type: String(row.primary_product_type || "").trim(),
    primary_brand_name: String(row.primary_brand_name || "").trim(),
    linked_product_count: Number(row.linked_product_count) || 0,
    is_linked: Boolean(row.is_linked),
  }));

/* -----------------------
  Migrations (all tables with   suffix)
------------------------*/
async function runMigrations() {
  try {
    // Helper to run migration queries but ignore duplicate pg_type errors
    async function safeQuery(sql) {
      try {
        await db.query(sql);
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
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS last_otp_verified_at TIMESTAMPTZ;
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active';
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS department TEXT;
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS bio TEXT;
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS avatar TEXT;
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS permissions_override JSONB DEFAULT '[]'::jsonb;
    `);

    await safeQuery(`
      ALTER TABLE "user"
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS rbac_roles (
        id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
        built_in BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS rbac_permissions (
        id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT DEFAULT '',
        module TEXT DEFAULT '',
        module_label TEXT DEFAULT '',
        action TEXT DEFAULT '',
        built_in BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS rbac_activity (
        id SERIAL PRIMARY KEY,
        actor TEXT DEFAULT 'System',
        actor_role TEXT DEFAULT 'admin',
        module TEXT NOT NULL,
        action TEXT NOT NULL,
        target TEXT DEFAULT '',
        status TEXT DEFAULT 'success',
        note TEXT DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS auth_login_challenges (
        challenge_id TEXT PRIMARY KEY,
        user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        otp_hash TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        attempts INT NOT NULL DEFAULT 0,
        max_attempts INT NOT NULL DEFAULT 5,
        resend_count INT NOT NULL DEFAULT 0,
        last_sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        channels JSONB NOT NULL DEFAULT '[]'::jsonb,
        verified_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_login_challenges_user_id
      ON auth_login_challenges (user_id);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_login_challenges_email
      ON auth_login_challenges (email);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_auth_login_challenges_expires_at
      ON auth_login_challenges (expires_at);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS auth_organization_pin (
        id INT PRIMARY KEY CHECK (id = 1),
        pin_hash TEXT NOT NULL,
        updated_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS auth_data_delete_pin (
        id INT PRIMARY KEY CHECK (id = 1),
        pin_hash TEXT NOT NULL,
        updated_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS admin_delete_audit (
        id SERIAL PRIMARY KEY,
        action TEXT NOT NULL DEFAULT 'delete',
        target_route TEXT NOT NULL,
        target_table TEXT,
        target_id TEXT,
        target_name TEXT,
        target_type TEXT,
        reason TEXT NOT NULL,
        deleted_by INT REFERENCES "user"(id) ON DELETE SET NULL,
        deleted_by_email TEXT,
        deleted_by_role TEXT,
        request_ip TEXT,
        user_agent TEXT,
        http_status INT,
        outcome TEXT,
        request_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        target_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_admin_delete_audit_created_at
      ON admin_delete_audit (created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_admin_delete_audit_target
      ON admin_delete_audit (target_table, target_id);
    `);

    const initialDataDeletePin = String(
      process.env.DATA_DELETE_PIN || process.env.DELETE_APPROVAL_PIN || "",
    ).trim();
    if (DATA_DELETE_PIN_PATTERN.test(initialDataDeletePin)) {
      const existingDeletePin = await db.query(
        `SELECT id FROM auth_data_delete_pin WHERE id = 1 LIMIT 1`,
      );
      if (!existingDeletePin.rows?.length) {
        const pinHash = await bcrypt.hash(
          initialDataDeletePin,
          DATA_DELETE_PIN_HASH_ROUNDS,
        );
        await db.query(
          `INSERT INTO auth_data_delete_pin (id, pin_hash, updated_at)
           VALUES (1, $1, now())
           ON CONFLICT (id) DO NOTHING`,
          [pinHash],
        );
      }
    }

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
      CREATE TABLE IF NOT EXISTS compare_sessions (
        id BIGSERIAL PRIMARY KEY,
        session_key TEXT,
        visitor_key TEXT,
        product_type TEXT,
        product_count INT NOT NULL DEFAULT 2,
        compared_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS compare_session_products (
        session_id BIGINT NOT NULL REFERENCES compare_sessions(id) ON DELETE CASCADE,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        position INT NOT NULL DEFAULT 1,
        PRIMARY KEY (session_id, product_id)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_product_comparisons_compared_at
      ON product_comparisons (compared_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_compare_sessions_compared_at
      ON compare_sessions (compared_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_compare_sessions_visitor_compared_at
      ON compare_sessions (visitor_key, compared_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_compare_session_products_product
      ON compare_session_products (product_id, session_id);
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
      CREATE TABLE IF NOT EXISTS compare_pages (
        id SERIAL PRIMARY KEY,
        compare_key TEXT NOT NULL UNIQUE,
        primary_product_id INT REFERENCES products(id) ON DELETE SET NULL,
        items JSONB NOT NULL DEFAULT '[]'::jsonb,
        segment_label TEXT,
        smartphone_type_label TEXT,
        slug TEXT,
        title TEXT,
        meta_description TEXT,
        status TEXT NOT NULL DEFAULT 'published',
        source TEXT NOT NULL DEFAULT 'manual',
        generation_reason TEXT,
        system_score NUMERIC NOT NULL DEFAULT 0,
        manual_compare_count INT NOT NULL DEFAULT 0,
        last_compared_at TIMESTAMP,
        generated_at TIMESTAMP,
        route_path TEXT,
        updated_at TIMESTAMP DEFAULT now(),
        published_at TIMESTAMP
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_compare_pages_updated_at
      ON compare_pages (updated_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_compare_pages_status_source
      ON compare_pages (status, source);
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
      CREATE TABLE IF NOT EXISTS contact_submissions (
        id SERIAL PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL,
        subject TEXT NOT NULL,
        subject_label TEXT NOT NULL,
        message TEXT NOT NULL,
        agree_terms BOOLEAN NOT NULL DEFAULT false,
        source TEXT,
        payload JSONB,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_contact_submissions_created_at
      ON contact_submissions (created_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_contact_submissions_email
      ON contact_submissions (email);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_contact_submissions_subject
      ON contact_submissions (subject);
    `);

    await safeQuery(`
      ALTER TABLE contact_submissions
      ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'new';
    `);

    await safeQuery(`
      ALTER TABLE contact_submissions
      ADD COLUMN IF NOT EXISTS admin_notes TEXT;
    `);

    await safeQuery(`
      ALTER TABLE contact_submissions
      ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_contact_submissions_status
      ON contact_submissions (status, created_at DESC);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS blogs (
        id SERIAL PRIMARY KEY,
        product_id INT UNIQUE
          REFERENCES products(id)
          ON DELETE CASCADE,
        title TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        excerpt TEXT,
        content_template TEXT NOT NULL,
        content_rendered TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        is_published BOOLEAN NOT NULL DEFAULT false,
        blog_eligible BOOLEAN NOT NULL DEFAULT false,
        eligibility_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        token_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        meta_title TEXT,
        meta_description TEXT,
        hero_image TEXT,
        hero_image_source TEXT,
        hero_image_alt TEXT,
        hero_image_caption TEXT,
        category TEXT,
        brand_name TEXT,
        tags JSONB NOT NULL DEFAULT '[]'::jsonb,
        featured BOOLEAN NOT NULL DEFAULT false,
        trending BOOLEAN NOT NULL DEFAULT false,
        pinned BOOLEAN NOT NULL DEFAULT false,
        author_name TEXT,
        author_user_id INT REFERENCES "user"(id) ON DELETE SET NULL,
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
      ADD COLUMN IF NOT EXISTS is_published BOOLEAN NOT NULL DEFAULT false;
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
      ADD COLUMN IF NOT EXISTS hero_image_source TEXT,
      ADD COLUMN IF NOT EXISTS hero_image_alt TEXT,
      ADD COLUMN IF NOT EXISTS hero_image_caption TEXT,
      ADD COLUMN IF NOT EXISTS category TEXT,
      ADD COLUMN IF NOT EXISTS brand_name TEXT,
      ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb,
      ADD COLUMN IF NOT EXISTS featured BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS trending BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS pinned BOOLEAN NOT NULL DEFAULT false;
    `);

    await safeQuery(`
      UPDATE blogs
      SET tags = '[]'::jsonb
      WHERE tags IS NULL;
    `);

    await safeQuery(`
      UPDATE blogs
      SET is_published = (status = 'published')
      WHERE is_published IS DISTINCT FROM (status = 'published');
    `);

    await safeQuery(`
      UPDATE blogs bl
      SET
        author_user_id = COALESCE(bl.author_user_id, bl.updated_by, bl.created_by),
        author_name = COALESCE(
          NULLIF(BTRIM(bl.author_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', u.first_name, u.last_name)), ''),
          NULLIF(BTRIM(u.user_name), ''),
          NULLIF(BTRIM(u.email), '')
        )
      FROM "user" u
      WHERE u.id = COALESCE(bl.author_user_id, bl.updated_by, bl.created_by)
        AND (
          bl.author_user_id IS NULL
          OR NULLIF(BTRIM(bl.author_name), '') IS NULL
        );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blogs_status_published_at
      ON blogs (status, published_at DESC, updated_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blogs_is_published
      ON blogs (is_published, published_at DESC, updated_at DESC);
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blogs_product
      ON blogs (product_id);
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS blog_product_links (
        blog_id INT NOT NULL REFERENCES blogs(id) ON DELETE CASCADE,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        position INT NOT NULL DEFAULT 0,
        is_primary BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMP DEFAULT now(),
        PRIMARY KEY (blog_id, product_id)
      );
    `);

    await safeQuery(`
      CREATE INDEX IF NOT EXISTS idx_blog_product_links_product
      ON blog_product_links (product_id, position ASC, blog_id DESC);
    `);

    await safeQuery(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_blog_product_links_primary_blog
      ON blog_product_links (blog_id)
      WHERE is_primary = true;
    `);

    await safeQuery(`
      INSERT INTO blog_product_links (blog_id, product_id, position, is_primary)
      SELECT id, product_id, 0, true
      FROM blogs
      WHERE product_id IS NOT NULL
      ON CONFLICT (blog_id, product_id)
      DO UPDATE SET
        position = EXCLUDED.position,
        is_primary = EXCLUDED.is_primary;
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

    console.log("✅ Migrations to   completed");
  } catch (err) {
    console.error("Migration error:", err);
    throw err;
  }
}

/* -----------------------
  Auth Middleware + Role-Based Access Control (RBAC)
------------------------*/

const OTP_CHALLENGE_TABLE = "auth_login_challenges";
const ORGANIZATION_PIN_TABLE = "auth_organization_pin";
const DATA_DELETE_PIN_TABLE = "auth_data_delete_pin";
const DATA_DELETE_AUDIT_TABLE = "admin_delete_audit";
const WEBAUTHN_CREDENTIAL_TABLE = "auth_webauthn_credentials";
const WEBAUTHN_CHALLENGE_TABLE = "auth_webauthn_challenges";
const OTP_CODE_LENGTH = 6;
const OTP_TTL_MS = 5 * 60 * 1000;
const OTP_RESEND_COOLDOWN_MS = 30 * 1000;
const OTP_MAX_ATTEMPTS = 5;
const OTP_HASH_ROUNDS = 10;
const ORGANIZATION_PIN_HASH_ROUNDS = 10;
const ORGANIZATION_PIN_LENGTH = 7;
const DATA_DELETE_PIN_HASH_ROUNDS = 10;
const DATA_DELETE_PIN_LENGTH = 4;
const DATA_DELETE_REASON_MIN_LENGTH = 5;
const DATA_DELETE_REASON_MAX_LENGTH = 1000;
const OTP_REVERIFY_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const ACCESS_TOKEN_TTL = "1h";
const PENDING_LOGIN_TOKEN_PURPOSE = "admin_pending_login";
const PENDING_LOGIN_TOKEN_TTL_SECONDS = 15 * 60;
const PENDING_LOGIN_TOKEN_TTL = `${PENDING_LOGIN_TOKEN_TTL_SECONDS}s`;
const WEBAUTHN_CHALLENGE_TTL_MS = 5 * 60 * 1000;
const WEBAUTHN_RP_NAME =
  String(process.env.WEBAUTHN_RP_NAME || "Hooks Admin").trim() || "Hooks Admin";

const WEBAUTHN_ALLOWED_ORIGINS = new Set([
  ...Array.from(ALLOWED_ORIGINS).map(normalizeOrigin).filter(Boolean),
  ...String(process.env.WEBAUTHN_ALLOWED_ORIGINS || "")
    .split(",")
    .map(normalizeOrigin)
    .filter(Boolean),
]);
const ORGANIZATION_PIN_PATTERN = new RegExp(
  `^\\d{${ORGANIZATION_PIN_LENGTH}}$`,
);
const DATA_DELETE_PIN_PATTERN = new RegExp(`^\\d{${DATA_DELETE_PIN_LENGTH}}$`);

const loginInitiateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many login attempts. Please try again later.",
  },
});

const loginOtpVerifyLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many OTP verification attempts. Please try again later.",
  },
});

const loginOtpResendLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many OTP resend requests. Please try again later.",
  },
});

const loginPinVerifyLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many PIN verification attempts. Please try again later.",
  },
});

const dataDeletePinVerifyLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    message: "Too many delete PIN attempts. Please try again later.",
  },
});

const normalizeLoginEmail = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const normalizeOrganizationPinInput = (value) =>
  String(value || "")
    .replace(/\D/g, "")
    .slice(0, ORGANIZATION_PIN_LENGTH);

const isValidOrganizationPin = (value) =>
  ORGANIZATION_PIN_PATTERN.test(String(value || ""));

const normalizeDataDeletePinInput = (value) =>
  String(value || "")
    .replace(/\D/g, "")
    .slice(0, DATA_DELETE_PIN_LENGTH);

const isValidDataDeletePin = (value) =>
  DATA_DELETE_PIN_PATTERN.test(String(value || ""));

const normalizeDeleteReasonInput = (value) =>
  String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, DATA_DELETE_REASON_MAX_LENGTH);

const parseBooleanInput = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return false;
};

const normalizeOtpInput = (value) => {
  const digits = String(value || "")
    .trim()
    .replace(/\D/g, "");
  return digits.length === OTP_CODE_LENGTH ? digits : null;
};

const generateOtpCode = () =>
  String(crypto.randomInt(0, 10 ** OTP_CODE_LENGTH)).padStart(
    OTP_CODE_LENGTH,
    "0",
  );

const normalizeSmsRecipient = (phone) => {
  const raw = String(phone || "").trim();
  if (!raw) return null;
  const compact = raw.replace(/\s+/g, "");
  if (/^\+\d{8,15}$/.test(compact)) return compact;

  const defaultCountryCode = String(
    process.env.OTP_SMS_DEFAULT_COUNTRY_CODE || "",
  ).trim();
  const digits = raw.replace(/\D/g, "");
  if (defaultCountryCode && /^\+\d{1,4}$/.test(defaultCountryCode) && digits) {
    return `${defaultCountryCode}${digits}`;
  }

  return null;
};

async function sendLoginOtpSms({ phone, otp, expiresInMinutes = 5, userName }) {
  const recipient = normalizeSmsRecipient(phone);
  if (!recipient) {
    return { delivered: false, skipped: true, reason: "invalid_phone" };
  }

  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const fromNumber = process.env.TWILIO_FROM_NUMBER;
  if (!accountSid || !authToken || !fromNumber) {
    return { delivered: false, skipped: true, reason: "sms_not_configured" };
  }

  if (typeof fetch !== "function") {
    return { delivered: false, skipped: true, reason: "fetch_not_available" };
  }

  const safeMinutes = Number.isFinite(expiresInMinutes)
    ? Math.max(1, Math.floor(expiresInMinutes))
    : 5;
  const body = `Your Hook login code is ${otp}. It expires in ${safeMinutes} minutes.`;

  const response = await fetch(
    `https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Messages.json`,
    {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`${accountSid}:${authToken}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        From: fromNumber,
        To: recipient,
        Body: body,
      }).toString(),
    },
  );

  if (!response.ok) {
    const errorText = await response.text().catch(() => "");
    throw new Error(
      `SMS delivery failed (${response.status})${errorText ? `: ${errorText}` : ""}`,
    );
  }

  return { delivered: true, channel: "sms", recipient, userName };
}

async function deliverLoginOtpNotifications({ user, otp, expiresInMinutes }) {
  const safeName = user.user_name || user.first_name || user.email || "there";
  const deliveryTasks = [
    sendLoginOtpEmail({
      email: user.email,
      otp,
      userName: safeName,
      expiresInMinutes,
    })
      .then(() => ({ delivered: true, channel: "email" }))
      .catch((error) => {
        console.error("Login OTP email failed:", error);
        return { delivered: false, channel: "email", error };
      }),
  ];

  if (user.phone) {
    deliveryTasks.push(
      sendLoginOtpSms({
        phone: user.phone,
        otp,
        expiresInMinutes,
        userName: safeName,
      })
        .then((result) => result)
        .catch((error) => {
          console.error("Login OTP SMS failed:", error);
          return { delivered: false, channel: "sms", error };
        }),
    );
  }

  const results = await Promise.all(deliveryTasks);
  return results
    .filter((result) => result && result.delivered)
    .map((result) => result.channel);
}

const normalizeTransportList = (value) =>
  (Array.isArray(value) ? value : []).filter(
    (transport) => typeof transport === "string" && transport.trim(),
  );

const serializeAdminTokenUser = (user) => ({
  id: user.id,
  email: user.email,
  role: user.role,
  username: user.user_name || user.username,
});

const serializeAdminUser = (user) => ({
  id: user.id,
  email: user.email,
  role: user.role,
  username: user.user_name || user.username,
  first_name: user.first_name || "",
  last_name: user.last_name || "",
  display_name:
    user.display_name || user.full_name || user.user_name || user.email || "",
  permissions_override: normalizeRbacPermissionList(
    user.permissions_override || [],
  ),
  role_permissions: normalizeRbacPermissionList(user.role_permissions || []),
  effective_permissions: normalizeRbacPermissionList(
    user.effective_permissions || [],
  ),
});

const issueAdminAccessToken = (user) =>
  jwt.sign(serializeAdminTokenUser(user), SECRET, {
    expiresIn: ACCESS_TOKEN_TTL,
  });

const buildAdminSessionUser = async (user = {}) => {
  try {
    const roleMap = await getRbacRoleMap();
    return buildRbacUserPayload(user, roleMap);
  } catch (err) {
    console.warn("Failed to hydrate RBAC session user:", err.message);
    return {
      ...user,
      role: normalizeRbacRole(user.role || "viewer"),
      permissions_override: normalizeRbacPermissionList(
        user.permissions_override || [],
      ),
      role_permissions: getDefaultPermissionsForRole(user.role || "viewer"),
      effective_permissions: expandRbacPermissionSet([
        ...getDefaultPermissionsForRole(user.role || "viewer"),
        ...normalizeRbacPermissionList(user.permissions_override || []),
      ]),
    };
  }
};

const buildSuccessfulAdminLoginResponse = async (
  user,
  message = "Login successful",
) => {
  const sessionUser = await buildAdminSessionUser(user);
  return {
    message,
    token: issueAdminAccessToken(sessionUser),
    user: serializeAdminUser(sessionUser),
  };
};

const hasFreshOtpVerification = (lastOtpVerifiedAt) => {
  if (!lastOtpVerifiedAt) return false;
  const verifiedAtMs = new Date(lastOtpVerifiedAt).getTime();
  if (!Number.isFinite(verifiedAtMs)) return false;
  return Date.now() - verifiedAtMs < OTP_REVERIFY_WINDOW_MS;
};

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

  return payload;
};

async function readOrganizationPinRecord() {
  const result = await db.query(
    `SELECT id, pin_hash, updated_by, updated_at
     FROM ${ORGANIZATION_PIN_TABLE}
     WHERE id = 1
     LIMIT 1`,
  );
  return result.rows[0] || null;
}

async function createOrganizationPin(pin, updatedBy) {
  const pinHash = await bcrypt.hash(pin, ORGANIZATION_PIN_HASH_ROUNDS);
  const result = await db.query(
    `INSERT INTO ${ORGANIZATION_PIN_TABLE} (id, pin_hash, updated_by, updated_at)
     VALUES (1, $1, $2, now())
     ON CONFLICT (id) DO NOTHING
     RETURNING updated_by, updated_at`,
    [pinHash, updatedBy],
  );
  return result.rows[0] || null;
}

async function updateOrganizationPin(pin, updatedBy) {
  const pinHash = await bcrypt.hash(pin, ORGANIZATION_PIN_HASH_ROUNDS);
  const result = await db.query(
    `INSERT INTO ${ORGANIZATION_PIN_TABLE} (id, pin_hash, updated_by, updated_at)
     VALUES (1, $1, $2, now())
     ON CONFLICT (id) DO UPDATE
       SET pin_hash = EXCLUDED.pin_hash,
           updated_by = EXCLUDED.updated_by,
           updated_at = EXCLUDED.updated_at
     RETURNING updated_by, updated_at`,
    [pinHash, updatedBy],
  );
  return result.rows[0] || null;
}

async function getOrganizationPinStatus() {
  const record = await readOrganizationPinRecord();
  return {
    isConfigured: Boolean(record?.pin_hash),
    updated_at: record?.updated_at || null,
    updated_by: record?.updated_by || null,
  };
}

async function readDataDeletePinRecord() {
  const result = await db.query(
    `SELECT id, pin_hash, updated_by, updated_at
     FROM ${DATA_DELETE_PIN_TABLE}
     WHERE id = 1
     LIMIT 1`,
  );
  return result.rows[0] || null;
}

async function updateDataDeletePin(pin, updatedBy) {
  const pinHash = await bcrypt.hash(pin, DATA_DELETE_PIN_HASH_ROUNDS);
  const result = await db.query(
    `INSERT INTO ${DATA_DELETE_PIN_TABLE} (id, pin_hash, updated_by, updated_at)
     VALUES (1, $1, $2, now())
     ON CONFLICT (id) DO UPDATE
       SET pin_hash = EXCLUDED.pin_hash,
           updated_by = EXCLUDED.updated_by,
           updated_at = EXCLUDED.updated_at
     RETURNING updated_by, updated_at`,
    [pinHash, updatedBy],
  );
  return result.rows[0] || null;
}

async function getDataDeletePinStatus() {
  const record = await readDataDeletePinRecord();
  return {
    isConfigured: Boolean(record?.pin_hash),
    updated_at: record?.updated_at || null,
    updated_by: record?.updated_by || null,
  };
}

const resolveRequestIp = (req) =>
  String(req.headers?.["x-forwarded-for"] || "")
    .split(",")[0]
    .trim() ||
  req.ip ||
  null;

const getDeleteApprovalInput = (req) => {
  const body = req.body || {};
  const rawPin =
    body.delete_pin ??
    body.deletePin ??
    body.pin ??
    body.confirm_pin ??
    body.confirmPin ??
    "";
  const rawReason =
    body.delete_reason ??
    body.deleteReason ??
    body.reason ??
    body.confirm_reason ??
    body.confirmReason ??
    "";

  return {
    pin: normalizeDataDeletePinInput(rawPin),
    reason: normalizeDeleteReasonInput(rawReason),
  };
};

const resolveDeleteAuditTarget = (req) => {
  const target = req.deleteAuditTarget || {};
  const params = req.params || {};
  const targetId =
    target.target_id ??
    target.targetId ??
    target.product_id ??
    target.productId ??
    params.id ??
    params.productId ??
    params.smartphoneId ??
    null;

  return {
    target_table: target.target_table || target.targetTable || null,
    target_id:
      targetId !== null && targetId !== undefined ? String(targetId) : null,
    target_name: target.target_name || target.targetName || target.name || null,
    target_type:
      target.target_type ||
      target.targetType ||
      target.product_type ||
      target.productType ||
      null,
    target_snapshot:
      target.target_snapshot || target.targetSnapshot || target.snapshot || {},
  };
};

async function writeDeleteAuditRecord(req, res) {
  if (!req?.deleteApproval) return;

  const target = resolveDeleteAuditTarget(req);
  const context = {
    params: req.params || {},
    query: req.query || {},
  };
  const status = Number(res?.statusCode) || null;
  const outcome =
    status !== null && status >= 200 && status < 300 ? "deleted" : "failed";

  await db.query(
    `INSERT INTO ${DATA_DELETE_AUDIT_TABLE}
       (action, target_route, target_table, target_id, target_name, target_type,
        reason, deleted_by, deleted_by_email, deleted_by_role, request_ip,
        user_agent, http_status, outcome, request_context, target_snapshot)
     VALUES
       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb)`,
    [
      "delete",
      req.originalUrl || req.url || "",
      target.target_table,
      target.target_id,
      target.target_name,
      target.target_type,
      req.deleteApproval.reason,
      req.user?.id || null,
      req.user?.email || null,
      req.user?.role || null,
      resolveRequestIp(req),
      req.headers?.["user-agent"] || null,
      status,
      outcome,
      JSON.stringify(context),
      JSON.stringify(target.target_snapshot || {}),
    ],
  );
}

async function requireDataDeleteApproval(req, res, next) {
  try {
    const { pin, reason } = getDeleteApprovalInput(req);

    if (!isValidDataDeletePin(pin)) {
      return res.status(400).json({
        message: `Delete PIN must be exactly ${DATA_DELETE_PIN_LENGTH} digits.`,
      });
    }

    if (reason.length < DATA_DELETE_REASON_MIN_LENGTH) {
      return res.status(400).json({
        message: `Delete reason must be at least ${DATA_DELETE_REASON_MIN_LENGTH} characters.`,
      });
    }

    const pinRecord = await readDataDeletePinRecord();
    if (!pinRecord?.pin_hash) {
      return res.status(409).json({
        message:
          "Delete PIN is not configured. Set DATA_DELETE_PIN or configure it from admin settings before deleting data.",
      });
    }

    const matches = await bcrypt.compare(pin, pinRecord.pin_hash);
    if (!matches) {
      return res.status(401).json({ message: "Invalid delete PIN." });
    }

    req.deleteApproval = { reason };
    res.on("finish", () => {
      void writeDeleteAuditRecord(req, res).catch((error) => {
        console.error("Delete audit write failed:", error);
      });
    });

    return next();
  } catch (err) {
    console.error("Delete approval verification failed:", err);
    return res.status(500).json({
      message: "Unable to verify delete approval. Please try again.",
    });
  }
}

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
  return result.rows[0] || null;
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

async function issueLoginOtpChallenge(user) {
  const normalizedEmail = normalizeLoginEmail(user.email);
  const challengeId = crypto.randomUUID();
  const otpCode = generateOtpCode();
  const otpHash = await bcrypt.hash(otpCode, OTP_HASH_ROUNDS);
  const expiresAt = new Date(Date.now() + OTP_TTL_MS);

  await db.query(
    `DELETE FROM ${OTP_CHALLENGE_TABLE}
     WHERE user_id = $1
       AND verified_at IS NULL`,
    [user.id],
  );

  await db.query(
    `INSERT INTO ${OTP_CHALLENGE_TABLE}
      (challenge_id, user_id, email, otp_hash, expires_at, attempts, max_attempts, resend_count, last_sent_at, channels)
     VALUES ($1, $2, $3, $4, $5, 0, $6, 0, now(), '[]'::jsonb)`,
    [
      challengeId,
      user.id,
      normalizedEmail,
      otpHash,
      expiresAt,
      OTP_MAX_ATTEMPTS,
    ],
  );

  const deliveryChannels = await deliverLoginOtpNotifications({
    user: { ...user, email: normalizedEmail },
    otp: otpCode,
    expiresInMinutes: OTP_TTL_MS / (60 * 1000),
  });

  if (!deliveryChannels.length) {
    await db.query(
      `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
      [challengeId],
    );
    throw new Error("Unable to deliver OTP");
  }

  try {
    await db.query(
      `UPDATE ${OTP_CHALLENGE_TABLE}
       SET channels = $2::jsonb
       WHERE challenge_id = $1`,
      [challengeId, JSON.stringify(deliveryChannels)],
    );
  } catch (err) {
    console.warn("Failed to persist OTP delivery channels:", err);
  }

  return {
    challengeId,
    deliveryChannels,
    expiresInSeconds: Math.floor(OTP_TTL_MS / 1000),
    resendAfterMs: OTP_RESEND_COOLDOWN_MS,
    maxAttempts: OTP_MAX_ATTEMPTS,
  };
}

/* -----------------------
  AUTH Routes
------------------------*/
app.post("/api/auth/register", async (req, res) => {
  try {
    const b = req.body || {};
    const user_name = b.user_name || null;
    const first_name = b.first_name || null;
    const last_name = b.last_name || null;
    const phone = b.phone || null;
    const gender = b.gender || null;
    const email = b.email;
    const password = b.password || `${Math.random()}${Date.now()}`;
    const role = normalizeRbacRole(b.role || "admin");

    if (!email || !password) {
      return res.status(400).json({ message: "email and password required" });
    }

    const hashed = await bcrypt.hash(password, 10);

    const result = await db.query(
      `INSERT INTO "user"
        (user_name, first_name, last_name, phone, gender, email, password, role)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING ${RBAC_USER_SELECT_FIELDS}`,
      [user_name, first_name, last_name, phone, gender, email, hashed, role],
    );

    const roleMap = await getRbacRoleMap();
    const user = buildRbacUserPayload(result.rows[0], roleMap);

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
    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const organizationPinStatus = await getOrganizationPinStatus();
    if (!organizationPinStatus.isConfigured) {
      return res.json(
        buildPendingLoginResponse(
          user,
          "pin_setup",
          "Create the 7-digit organization PIN to finish signing in.",
        ),
      );
    }

    return res.json(
      buildPendingLoginResponse(
        user,
        "pin",
        "Enter the 7-digit organization PIN to finish signing in.",
      ),
    );
  } catch (err) {
    console.error("Login PIN initiation error:", err);
    res.status(500).json({
      message: "Unable to start PIN login. Please try again.",
    });
  }
});

app.post("/api/auth/login/pin", loginPinVerifyLimiter, async (req, res) => {
  try {
    const loginTicket = String(req.body?.loginTicket || "").trim();
    const pin = normalizeOrganizationPinInput(req.body?.pin);
    const pendingLogin = verifyPendingLoginTicket(loginTicket);

    if (!pendingLogin || pendingLogin.nextAction !== "pin") {
      return res.status(401).json({
        message: "Login session expired. Please sign in again.",
      });
    }

    if (!isValidOrganizationPin(pin)) {
      return res.status(400).json({
        message: "Organization PIN must be exactly 7 digits.",
      });
    }

    const user = await getAdminUserById(pendingLogin.id);
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    const organizationPin = await readOrganizationPinRecord();
    if (!organizationPin?.pin_hash) {
      return res.status(410).json({
        message:
          "Organization PIN is not configured. Please sign in again to set it up.",
      });
    }

    const matches = await bcrypt.compare(pin, organizationPin.pin_hash);
    if (!matches) {
      return res.status(401).json({ message: "Invalid organization PIN." });
    }

    return res.json(await buildSuccessfulAdminLoginResponse(user));
  } catch (err) {
    console.error("PIN login verification error:", err);
    return res.status(500).json({
      message: "Unable to verify the organization PIN. Please try again.",
    });
  }
});

app.post(
  "/api/auth/login/pin/setup/verify",
  loginPinVerifyLimiter,
  async (req, res) => {
    try {
      const loginTicket = String(req.body?.loginTicket || "").trim();
      const newPin = normalizeOrganizationPinInput(req.body?.newPin);
      const pendingLogin = verifyPendingLoginTicket(loginTicket);

      if (!pendingLogin || pendingLogin.nextAction !== "pin_setup") {
        return res.status(401).json({
          message: "Login session expired. Please sign in again.",
        });
      }

      if (!isValidOrganizationPin(newPin)) {
        return res.status(400).json({
          message: "Organization PIN must be exactly 7 digits.",
        });
      }

      const user = await getAdminUserById(pendingLogin.id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      const created = await createOrganizationPin(newPin, user.id);
      if (!created) {
        return res.status(409).json({
          message:
            "Organization PIN has already been configured. Please sign in again and use the current PIN.",
        });
      }

      return res.json(
        await buildSuccessfulAdminLoginResponse(
          user,
          "Organization PIN created successfully",
        ),
      );
    } catch (err) {
      console.error("PIN setup verification error:", err);
      return res.status(500).json({
        message: "Unable to create the organization PIN. Please try again.",
      });
    }
  },
);

app.post(
  "/api/auth/login/verify-otp",
  loginOtpVerifyLimiter,
  async (req, res) => {
    let client;
    try {
      client = await db.connect();
      const b = req.body || {};
      const email = normalizeLoginEmail(b.email);
      const challengeId = String(b.challengeId || "").trim();
      const otp = normalizeOtpInput(b.otp);
      const deviceAuthSupported = parseBooleanInput(b.deviceAuthSupported);
      const forceOtpFallback = parseBooleanInput(b.forceOtpFallback);

      if (!email || !challengeId || !otp) {
        return res.status(400).json({
          message: "challengeId, email and 6-digit otp are required",
        });
      }

      await client.query("BEGIN");
      const challengeResult = await client.query(
        `SELECT *
         FROM ${OTP_CHALLENGE_TABLE}
         WHERE challenge_id = $1
           AND email = $2
         FOR UPDATE`,
        [challengeId, email],
      );

      if (!challengeResult.rows.length) {
        await client.query("ROLLBACK");
        return res.status(401).json({
          message: "OTP expired or invalid. Please log in again.",
        });
      }

      const challenge = challengeResult.rows[0];
      const expiresAtMs = new Date(challenge.expires_at).getTime();
      if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
        await client.query(
          `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
          [challengeId],
        );
        await client.query("COMMIT");
        return res.status(410).json({
          message: "OTP expired. Please log in again.",
        });
      }

      const maxAttempts = Number(challenge.max_attempts || OTP_MAX_ATTEMPTS);
      const attempts = Number(challenge.attempts || 0);
      if (attempts >= maxAttempts) {
        await client.query(
          `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
          [challengeId],
        );
        await client.query("COMMIT");
        return res.status(429).json({
          message: "Too many invalid OTP attempts. Please log in again.",
        });
      }

      const isValid = await bcrypt.compare(otp, challenge.otp_hash);
      if (!isValid) {
        const updated = await client.query(
          `UPDATE ${OTP_CHALLENGE_TABLE}
           SET attempts = attempts + 1
           WHERE challenge_id = $1
           RETURNING attempts, max_attempts`,
          [challengeId],
        );

        const nextAttempts = Number(updated.rows[0]?.attempts || attempts + 1);
        const nextMaxAttempts = Number(
          updated.rows[0]?.max_attempts || maxAttempts,
        );
        const attemptsRemaining = Math.max(0, nextMaxAttempts - nextAttempts);

        if (attemptsRemaining <= 0) {
          await client.query(
            `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
            [challengeId],
          );
          await client.query("COMMIT");
          return res.status(429).json({
            message: "Too many invalid OTP attempts. Please log in again.",
          });
        }

        await client.query("COMMIT");
        return res.status(401).json({
          message: "Invalid OTP",
          attemptsRemaining,
        });
      }

      await client.query(
        `UPDATE "user"
         SET last_otp_verified_at = now()
         WHERE id = $1`,
        [challenge.user_id],
      );
      await client.query(
        `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
        [challengeId],
      );
      await client.query("COMMIT");

      const userResult = await db.query('SELECT * FROM "user" WHERE id = $1', [
        challenge.user_id,
      ]);
      if (!userResult.rows.length) {
        return res.status(404).json({ message: "User not found" });
      }

      const user = userResult.rows[0];
      const credentials = await listUserWebAuthnCredentials(user.id);

      if (credentials.length > 0 && deviceAuthSupported && !forceOtpFallback) {
        return res.json(
          buildPendingLoginResponse(
            user,
            "device_auth",
            "OTP verified. Use your device verification to finish signing in.",
          ),
        );
      }

      if (!credentials.length && deviceAuthSupported && !forceOtpFallback) {
        return res.json(
          buildPendingLoginResponse(
            user,
            "device_setup",
            "OTP verified. Set up device verification for faster future logins.",
          ),
        );
      }

      return res.json(await buildSuccessfulAdminLoginResponse(user));
    } catch (err) {
      if (client) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          console.error("OTP verify rollback failed:", rollbackError);
        }
      }
      console.error("Login OTP verification error:", err);
      return res.status(500).json({
        message: "Unable to verify OTP. Please try again.",
      });
    } finally {
      if (client) {
        client.release();
      }
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
  loginOtpVerifyLimiter,
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
        await buildSuccessfulAdminLoginResponse(
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
  loginOtpVerifyLimiter,
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
        await buildSuccessfulAdminLoginResponse(
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

app.post("/api/auth/login/finalize", loginInitiateLimiter, async (req, res) => {
  try {
    const loginTicket = String(req.body?.loginTicket || "").trim();
    const pendingLogin = verifyPendingLoginTicket(loginTicket);

    if (!pendingLogin || pendingLogin.nextAction !== "device_setup") {
      return res.status(401).json({
        message: "Login session expired. Please sign in again.",
      });
    }

    const user = await getAdminUserById(pendingLogin.id);
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    await clearWebAuthnChallenges(loginTicket);
    return res.json(await buildSuccessfulAdminLoginResponse(user));
  } catch (err) {
    console.error("Finalize login error:", err);
    return res.status(500).json({
      message: "Unable to finish login. Please try again.",
    });
  }
});

app.post(
  "/api/auth/login/resend-otp",
  loginOtpResendLimiter,
  async (req, res) => {
    let client;
    try {
      client = await db.connect();
      const b = req.body || {};
      const email = normalizeLoginEmail(b.email);
      const challengeId = String(b.challengeId || "").trim();

      if (!email || !challengeId) {
        await client.query("ROLLBACK").catch(() => {});
        return res.status(400).json({
          message: "challengeId and email are required",
        });
      }

      await client.query("BEGIN");
      const challengeResult = await client.query(
        `SELECT *
         FROM ${OTP_CHALLENGE_TABLE}
         WHERE challenge_id = $1
           AND email = $2
         FOR UPDATE`,
        [challengeId, email],
      );

      if (!challengeResult.rows.length) {
        await client.query("ROLLBACK");
        return res.status(410).json({
          message: "OTP expired. Please log in again.",
        });
      }

      const challenge = challengeResult.rows[0];
      const expiresAtMs = new Date(challenge.expires_at).getTime();
      if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
        await client.query(
          `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
          [challengeId],
        );
        await client.query("COMMIT");
        return res.status(410).json({
          message: "OTP expired. Please log in again.",
        });
      }

      const maxAttempts = Number(challenge.max_attempts || OTP_MAX_ATTEMPTS);
      const attempts = Number(challenge.attempts || 0);
      if (attempts >= maxAttempts) {
        await client.query(
          `DELETE FROM ${OTP_CHALLENGE_TABLE} WHERE challenge_id = $1`,
          [challengeId],
        );
        await client.query("COMMIT");
        return res.status(429).json({
          message: "Too many invalid OTP attempts. Please log in again.",
        });
      }

      const lastSentAtMs = new Date(challenge.last_sent_at).getTime();
      const cooldownRemainingMs = Math.max(
        0,
        OTP_RESEND_COOLDOWN_MS - (Date.now() - lastSentAtMs),
      );
      if (cooldownRemainingMs > 0) {
        await client.query("ROLLBACK");
        return res.status(429).json({
          message: "Please wait before requesting another OTP.",
          retryAfterMs: cooldownRemainingMs,
        });
      }

      const userResult = await client.query(
        'SELECT * FROM "user" WHERE id = $1',
        [challenge.user_id],
      );
      if (!userResult.rows.length) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "User not found" });
      }

      const user = userResult.rows[0];
      const otpCode = generateOtpCode();
      const otpHash = await bcrypt.hash(otpCode, OTP_HASH_ROUNDS);
      const nextExpiresAt = new Date(Date.now() + OTP_TTL_MS);
      const previousState = {
        otp_hash: challenge.otp_hash,
        expires_at: challenge.expires_at,
        attempts: challenge.attempts,
        resend_count: challenge.resend_count,
        last_sent_at: challenge.last_sent_at,
        channels: challenge.channels || [],
      };

      await client.query(
        `UPDATE ${OTP_CHALLENGE_TABLE}
         SET otp_hash = $2,
             expires_at = $3,
             attempts = 0,
             last_sent_at = now()
         WHERE challenge_id = $1`,
        [challengeId, otpHash, nextExpiresAt],
      );
      await client.query("COMMIT");

      const deliveryChannels = await deliverLoginOtpNotifications({
        user,
        otp: otpCode,
        expiresInMinutes: OTP_TTL_MS / (60 * 1000),
      });

      if (!deliveryChannels.length) {
        const revertClient = await db.connect();
        try {
          await revertClient.query("BEGIN");
          await revertClient.query(
            `UPDATE ${OTP_CHALLENGE_TABLE}
             SET otp_hash = $2,
                 expires_at = $3,
                 attempts = $4,
                 resend_count = $5,
                 last_sent_at = $6,
                 channels = $7::jsonb
             WHERE challenge_id = $1`,
            [
              challengeId,
              previousState.otp_hash,
              previousState.expires_at,
              previousState.attempts,
              previousState.resend_count,
              previousState.last_sent_at,
              JSON.stringify(previousState.channels || []),
            ],
          );
          await revertClient.query("COMMIT");
        } catch (revertError) {
          try {
            await revertClient.query("ROLLBACK");
          } catch (rollbackError) {
            console.error("OTP resend rollback failed:", rollbackError);
          }
          console.error("OTP resend revert failed:", revertError);
        } finally {
          revertClient.release();
        }

        return res.status(500).json({
          message: "Unable to deliver OTP. Please try again.",
        });
      }

      await db
        .query(
          `UPDATE ${OTP_CHALLENGE_TABLE}
         SET channels = $2::jsonb,
             resend_count = resend_count + 1,
             last_sent_at = now()
         WHERE challenge_id = $1`,
          [challengeId, JSON.stringify(deliveryChannels)],
        )
        .catch((err) => {
          console.warn("Failed to update OTP resend metadata:", err);
        });

      return res.json({
        message: "OTP sent again",
        challengeId,
        expiresIn: Math.floor(OTP_TTL_MS / 1000),
        resendAfterMs: OTP_RESEND_COOLDOWN_MS,
        maxAttempts: OTP_MAX_ATTEMPTS,
        deliveryChannels,
      });
    } catch (err) {
      if (client) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          console.error("OTP resend rollback failed:", rollbackError);
        }
      }
      console.error("Login OTP resend error:", err);
      return res.status(500).json({
        message: "Unable to resend OTP. Please try again.",
      });
    } finally {
      if (client) {
        client.release();
      }
    }
  },
);

app.get("/api/auth/organization-pin/status", authenticate, async (req, res) => {
  try {
    const pinStatus = await getOrganizationPinStatus();
    return res.json({
      success: true,
      isConfigured: pinStatus.isConfigured,
      updated_at: pinStatus.updated_at,
      updated_by: pinStatus.updated_by,
    });
  } catch (err) {
    console.error("Get organization PIN status error:", err);
    return res.status(500).json({
      message: "Unable to load organization PIN status.",
    });
  }
});

app.put("/api/auth/organization-pin", authenticate, async (req, res) => {
  try {
    const currentPin = normalizeOrganizationPinInput(req.body?.currentPin);
    const newPin = normalizeOrganizationPinInput(req.body?.newPin);
    const pinStatus = await getOrganizationPinStatus();

    if (!isValidOrganizationPin(newPin)) {
      return res.status(400).json({
        message: "Organization PIN must be exactly 7 digits.",
      });
    }

    if (pinStatus.isConfigured) {
      if (!currentPin) {
        return res
          .status(400)
          .json({ message: "Current organization PIN is required" });
      }

      if (!isValidOrganizationPin(currentPin)) {
        return res.status(400).json({
          message: "Organization PIN must be exactly 7 digits.",
        });
      }

      if (currentPin === newPin) {
        return res.status(400).json({
          message: "New PIN must be different from current PIN",
        });
      }

      const organizationPin = await readOrganizationPinRecord();
      const matches =
        organizationPin?.pin_hash &&
        (await bcrypt.compare(currentPin, organizationPin.pin_hash));
      if (!matches) {
        return res
          .status(401)
          .json({ message: "Current organization PIN is incorrect" });
      }
    }

    const updated = await updateOrganizationPin(newPin, req.user.id);
    return res.json({
      success: true,
      message: pinStatus.isConfigured
        ? "Organization PIN updated successfully"
        : "Organization PIN created successfully",
      isConfigured: true,
      updated_at: updated?.updated_at || null,
      updated_by: updated?.updated_by ?? req.user.id,
    });
  } catch (err) {
    console.error("Update organization PIN error:", err);
    return res.status(500).json({
      message: "Unable to update the organization PIN. Please try again.",
    });
  }
});

app.get("/api/auth/data-delete-pin/status", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const pinStatus = await getDataDeletePinStatus();
    return res.json({
      success: true,
      isConfigured: pinStatus.isConfigured,
      updated_at: pinStatus.updated_at,
      updated_by: pinStatus.updated_by,
    });
  } catch (err) {
    console.error("Get data delete PIN status error:", err);
    return res.status(500).json({
      message: "Unable to load delete PIN status.",
    });
  }
});

app.get("/api/auth/data-delete-audit", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const limitRaw = Number(req.query?.limit ?? 50);
    const offsetRaw = Number(req.query?.offset ?? 0);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(100, Math.max(1, Math.floor(limitRaw)))
      : 50;
    const offset = Number.isFinite(offsetRaw)
      ? Math.max(0, Math.floor(offsetRaw))
      : 0;

    const [auditResult, countResult] = await Promise.all([
      db.query(
        `
        SELECT
          audit.id,
          audit.action,
          audit.target_route,
          audit.target_table,
          audit.target_id,
          audit.target_name,
          audit.target_type,
          audit.reason,
          audit.deleted_by,
          audit.deleted_by_email,
          audit.deleted_by_role,
          audit.request_ip,
          audit.user_agent,
          audit.http_status,
          audit.outcome,
          audit.request_context,
          audit.target_snapshot,
          audit.created_at,
          u.user_name AS deleted_by_user_name,
          u.first_name AS deleted_by_first_name,
          u.last_name AS deleted_by_last_name,
          u.email AS deleted_by_current_email
        FROM ${DATA_DELETE_AUDIT_TABLE} audit
        LEFT JOIN "user" u
          ON u.id = audit.deleted_by
        ORDER BY audit.created_at DESC, audit.id DESC
        LIMIT $1 OFFSET $2
        `,
        [limit, offset],
      ),
      db.query(`SELECT COUNT(*)::int AS total FROM ${DATA_DELETE_AUDIT_TABLE}`),
    ]);

    return res.json({
      success: true,
      audits: auditResult.rows || [],
      total: Number(countResult.rows?.[0]?.total) || 0,
      limit,
      offset,
    });
  } catch (err) {
    console.error("Get data delete audit error:", err);
    return res.status(500).json({
      message: "Unable to load delete audit tracking.",
    });
  }
});

app.put("/api/auth/data-delete-pin", authenticate, async (req, res) => {
  try {
    // enforce admin-only
    if (!requireAdminAccess(req, res)) return;

    // require admin password for sensitive action
    const currentAdminPassword = String(
      req.body?.currentPassword || req.body?.current_password || "",
    ).trim();
    if (!currentAdminPassword) {
      return res.status(400).json({ message: "Admin password is required" });
    }

    // verify admin password
    const userResult = await db.query(
      'SELECT password FROM "user" WHERE id = $1',
      [req.user.id],
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }
    const user = userResult.rows[0];
    const passwordMatch = await bcrypt.compare(
      currentAdminPassword,
      user.password,
    );
    if (!passwordMatch) {
      return res.status(401).json({ message: "Admin password is incorrect" });
    }

    const currentPin = normalizeDataDeletePinInput(req.body?.currentPin);
    const newPin = normalizeDataDeletePinInput(req.body?.newPin);
    const pinStatus = await getDataDeletePinStatus();

    if (!isValidDataDeletePin(newPin)) {
      return res.status(400).json({
        message: `Delete PIN must be exactly ${DATA_DELETE_PIN_LENGTH} digits.`,
      });
    }

    if (pinStatus.isConfigured) {
      if (!currentPin) {
        return res
          .status(400)
          .json({ message: "Current delete PIN is required" });
      }

      if (!isValidDataDeletePin(currentPin)) {
        return res.status(400).json({
          message: `Delete PIN must be exactly ${DATA_DELETE_PIN_LENGTH} digits.`,
        });
      }

      if (currentPin === newPin) {
        return res.status(400).json({
          message: "New delete PIN must be different from current PIN",
        });
      }

      const deletePin = await readDataDeletePinRecord();
      const matches =
        deletePin?.pin_hash &&
        (await bcrypt.compare(currentPin, deletePin.pin_hash));
      if (!matches) {
        return res
          .status(401)
          .json({ message: "Current delete PIN is incorrect" });
      }
    }

    const updated = await updateDataDeletePin(newPin, req.user.id);
    return res.json({
      success: true,
      message: pinStatus.isConfigured
        ? "Delete PIN updated successfully"
        : "Delete PIN created successfully",
      isConfigured: true,
      updated_at: updated?.updated_at || null,
      updated_by: updated?.updated_by ?? req.user.id,
    });
  } catch (err) {
    console.error("Update data delete PIN error:", err);
    return res.status(500).json({
      message: "Unable to update the delete PIN. Please try again.",
    });
  }
});

// Delete the configured data-delete PIN (admin + password required)
app.delete("/api/auth/data-delete-pin", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const currentAdminPassword = String(
      req.body?.currentPassword || req.body?.current_password || "",
    ).trim();
    if (!currentAdminPassword) {
      return res.status(400).json({ message: "Admin password is required" });
    }

    const userResult = await db.query(
      'SELECT password FROM "user" WHERE id = $1',
      [req.user.id],
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }
    const user = userResult.rows[0];
    const passwordMatch = await bcrypt.compare(
      currentAdminPassword,
      user.password,
    );
    if (!passwordMatch) {
      return res.status(401).json({ message: "Admin password is incorrect" });
    }

    await db.query(`DELETE FROM ${DATA_DELETE_PIN_TABLE} WHERE id = 1`);

    return res.json({ success: true, message: "Delete PIN removed" });
  } catch (err) {
    console.error("Delete data delete PIN error:", err);
    return res.status(500).json({ message: "Unable to remove delete PIN" });
  }
});

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
    const { email, phone, first_name, last_name, gender } = req.body;

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
       SET email = $1, phone = $2, first_name = $3, last_name = $4, gender = $5
       WHERE id = $6
       RETURNING id, user_name, email, phone, first_name, last_name, gender, role`,
      [
        email,
        phone || null,
        first_name || null,
        last_name || null,
        gender || null,
        userId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const updatedUser = result.rows[0];

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

/* ---- Contact (Public Submit + Admin Inbox) ---- */
const CONTACT_SUBJECT_LABELS = {
  "general-support": "General support",
  "product-correction": "Product correction",
  "feature-request": "Feature request",
  "partnership-inquiry": "Partnership inquiry",
  "media-press": "Media or press inquiry",
};

const CONTACT_SUBMISSION_STATUSES = new Set([
  "new",
  "in_progress",
  "resolved",
  "archived",
]);

const normalizeContactStatus = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z_]/g, "");

const cleanContactText = (value) => {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text.length ? text : null;
};

const buildContactSubjectKey = (value) => {
  const text = cleanContactText(value);
  if (!text) return null;
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
};

const resolveContactSubjectLabel = (subject, explicitLabel) => {
  const label = cleanContactText(explicitLabel);
  if (label) return label;
  if (subject && CONTACT_SUBJECT_LABELS[subject]) {
    return CONTACT_SUBJECT_LABELS[subject];
  }
  return subject
    ? String(subject)
        .replace(/[-_]+/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase())
    : "Contact request";
};

app.post("/api/contact-submissions", async (req, res) => {
  try {
    const b = req.body || {};
    const fullName = cleanContactText(b.full_name || b.fullName);
    const email = cleanContactText(b.email);
    const subject =
      buildContactSubjectKey(b.subject) ||
      buildContactSubjectKey(b.subject_label || b.subjectLabel);
    const subjectLabel = resolveContactSubjectLabel(
      subject,
      b.subject_label || b.subjectLabel,
    );
    const message = cleanContactText(b.message);
    const agreeTerms = Boolean(b.agree_terms ?? b.agreeTerms ?? b.agreed);
    const source = cleanContactText(b.source || "hooks-web-contact");

    if (!fullName || !email || !subject || !message || !agreeTerms) {
      return res.status(400).json({
        message: "full name, email, subject, message and consent are required",
      });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    const inserted = await db.query(
      `INSERT INTO contact_submissions (
         full_name, email, subject, subject_label, message,
         agree_terms, source, payload
       ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8
       )
       RETURNING id, status, created_at`,
      [
        fullName,
        email.toLowerCase(),
        subject,
        subjectLabel,
        message,
        agreeTerms,
        source,
        b,
      ],
    );

    return res.status(201).json({
      message: "Contact request submitted successfully",
      submission: inserted.rows[0],
    });
  } catch (err) {
    console.error("Create contact submission error:", err);
    return res
      .status(500)
      .json({ message: "Failed to submit contact request" });
  }
});

app.get("/api/admin/contact-submissions", authenticate, async (req, res) => {
  try {
    const pageRaw = Number(req.query.page);
    const limitRaw = Number(req.query.limit);
    const page = Number.isFinite(pageRaw) && pageRaw > 0 ? pageRaw : 1;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 100) : 25;
    const offset = (page - 1) * limit;

    const [rowsResult, countResult] = await Promise.all([
      db.query(
        `SELECT id, full_name, email, subject, subject_label, message,
                agree_terms, source, payload, status, admin_notes,
                resolved_at, created_at, updated_at
         FROM contact_submissions
         ORDER BY created_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset],
      ),
      db.query(`SELECT COUNT(*)::int AS total FROM contact_submissions`),
    ]);

    return res.json({
      page,
      limit,
      total: countResult.rows[0]?.total || 0,
      rows: rowsResult.rows,
    });
  } catch (err) {
    console.error("List contact submissions error:", err);
    return res
      .status(500)
      .json({ message: "Failed to fetch contact submissions" });
  }
});

app.patch(
  "/api/admin/contact-submissions/:id",
  authenticate,
  async (req, res) => {
    try {
      const id = Number(req.params.id);
      const body = req.body || {};
      const statusProvided = Object.prototype.hasOwnProperty.call(
        body,
        "status",
      );
      const notesProvided =
        Object.prototype.hasOwnProperty.call(body, "admin_notes") ||
        Object.prototype.hasOwnProperty.call(body, "adminNotes");
      const nextStatus = statusProvided
        ? normalizeContactStatus(body.status)
        : null;
      const adminNotes = notesProvided
        ? cleanContactText(body.admin_notes ?? body.adminNotes)
        : null;

      if (!Number.isInteger(id) || id <= 0) {
        return res.status(400).json({ message: "Invalid id" });
      }

      if (!statusProvided && !notesProvided) {
        return res.status(400).json({
          message: "At least one of status or admin_notes is required",
        });
      }

      if (statusProvided && !CONTACT_SUBMISSION_STATUSES.has(nextStatus)) {
        return res.status(400).json({
          message:
            "Invalid status. Allowed values: new, in_progress, resolved, archived",
        });
      }

      const result = await db.query(
        `UPDATE contact_submissions
       SET status = COALESCE($1, status),
           admin_notes = CASE WHEN $2 THEN $3 ELSE admin_notes END,
           resolved_at = CASE
             WHEN COALESCE($1, status) = 'resolved' AND status <> 'resolved'
               THEN now()
             WHEN COALESCE($1, status) <> 'resolved'
               THEN NULL
             ELSE resolved_at
           END,
           updated_at = now()
       WHERE id = $4
       RETURNING id, full_name, email, subject, subject_label, message,
                 agree_terms, source, payload, status, admin_notes,
                 resolved_at, created_at, updated_at`,
        [statusProvided ? nextStatus : null, notesProvided, adminNotes, id],
      );

      if (!result.rows.length) {
        return res
          .status(404)
          .json({ message: "Contact submission not found" });
      }

      return res.json({
        message: "Contact submission updated successfully",
        submission: result.rows[0],
      });
    } catch (err) {
      console.error("Update contact submission error:", err);
      return res
        .status(500)
        .json({ message: "Failed to update contact submission" });
    }
  },
);

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

const RBAC_USER_SELECT_FIELDS = `
  id,
  user_name,
  first_name,
  last_name,
  phone,
  gender,
  email,
  role,
  status,
  department,
  bio,
  avatar,
  permissions_override,
  created_at,
  updated_at
`;

const parseRbacArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed;
    } catch {
      // Fall through to comma separated values.
    }
    return trimmed
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
};

const normalizeRbacPermissionList = (value = []) =>
  Array.from(
    new Set(
      parseRbacArray(value)
        .map((permission) => normalizeRbacPermissionToken(permission))
        .filter(Boolean),
    ),
  );

const splitRbacPermissionName = (name = "") => {
  const normalized = normalizeRbacPermissionToken(name);
  const lastDotIndex = normalized.lastIndexOf(".");
  if (lastDotIndex <= 0) {
    return { module: "", action: "" };
  }
  return {
    module: normalized.slice(0, lastDotIndex),
    action: normalized.slice(lastDotIndex + 1),
  };
};

const getBuiltinRbacRoleRecords = () =>
  Object.entries(RBAC_ROLE_PRESETS).map(([name, preset]) => {
    const permissions = normalizeRbacPermissionList(preset.permissions || []);
    return {
      id: name,
      name,
      title: String(preset.label || name).trim(),
      description: String(preset.description || "").trim(),
      permissions,
      effective_permissions: expandRbacPermissionSet(permissions),
      built_in: true,
      source: "builtin",
      created_at: null,
      updated_at: null,
    };
  });

const getBuiltinRbacPermissionRecords = () =>
  getRbacPermissionMatrix().flatMap((module) =>
    module.permissions.map((permission) => ({
      id: permission.code,
      name: permission.code,
      description: `Allows ${permission.action} on ${module.label}`,
      module: module.key,
      module_label: module.label,
      action: permission.action,
      built_in: true,
      source: "builtin",
      created_at: null,
      updated_at: null,
    })),
  );

const isBuiltinRbacRole = (value = "") => {
  const raw = String(value || "").trim();
  if (!raw) return false;
  return Boolean(RBAC_ROLE_PRESETS[normalizeRbacRole(raw)]);
};

const isBuiltinRbacPermission = (value = "") => {
  const normalized = normalizeRbacPermissionToken(value);
  if (!normalized) return false;
  return getBuiltinRbacPermissionRecords().some(
    (permission) =>
      permission.name === normalized || permission.id === normalized,
  );
};

const normalizeRbacRoleRecord = (row = {}) => {
  const roleName = normalizeRbacRole(row.name || row.id || "");
  const permissions = normalizeRbacPermissionList(row.permissions || []);
  return {
    id: roleName,
    name: roleName,
    title: String(row.title || roleName || "Role").trim(),
    description: String(row.description || "").trim(),
    permissions,
    effective_permissions: expandRbacPermissionSet(permissions),
    built_in: Boolean(row.built_in),
    source: row.source || "server",
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
};

const normalizeRbacPermissionRecord = (row = {}) => {
  const name = normalizeRbacPermissionToken(row.name || row.id || "");
  const derived = splitRbacPermissionName(name);
  return {
    id: name,
    name,
    description: String(row.description || "").trim(),
    module: String(row.module || derived.module || "").trim(),
    module_label: String(
      row.module_label || row.module || derived.module || "",
    ).trim(),
    action: String(row.action || derived.action || "").trim(),
    built_in: Boolean(row.built_in),
    source: row.source || "server",
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
};

const listRbacRoleRecords = async () => {
  const result = await db.query(
    `
    SELECT id, name, title, description, permissions, built_in, created_at, updated_at
    FROM rbac_roles
    ORDER BY title ASC, name ASC
  `,
  );
  const merged = new Map();

  getBuiltinRbacRoleRecords().forEach((role) => {
    merged.set(role.name, role);
  });

  (result.rows || []).forEach((row) => {
    const role = normalizeRbacRoleRecord(row);
    if (!role.name || merged.has(role.name)) return;
    merged.set(role.name, role);
  });

  return Array.from(merged.values());
};

const listRbacPermissionRecords = async () => {
  const result = await db.query(
    `
    SELECT id, name, description, module, module_label, action, built_in, created_at, updated_at
    FROM rbac_permissions
    ORDER BY name ASC
  `,
  );
  const merged = new Map();

  getBuiltinRbacPermissionRecords().forEach((permission) => {
    merged.set(permission.name, permission);
  });

  (result.rows || []).forEach((row) => {
    const permission = normalizeRbacPermissionRecord(row);
    if (!permission.name || merged.has(permission.name)) return;
    merged.set(permission.name, permission);
  });

  return Array.from(merged.values()).sort((a, b) =>
    String(a.name).localeCompare(String(b.name)),
  );
};

const getRbacRoleMap = async () => {
  const roles = await listRbacRoleRecords();
  return new Map(roles.map((role) => [normalizeRbacRole(role.name), role]));
};

const inferRbacDepartment = (role = "") => {
  const normalized = normalizeRbacRole(role);
  if (normalized === "ceo") return "Executive";
  if (normalized === "admin") return "Administration";
  if (["content_admin", "editor", "author", "moderator"].includes(normalized)) {
    return "Content";
  }
  if (normalized === "product_manager") return "Products";
  if (normalized === "analyst") return "Analytics";
  if (normalized === "seo") return "SEO";
  return "General";
};

const buildRbacUserPayload = (user = {}, roleMap = new Map()) => {
  const role = normalizeRbacRole(user.role || "viewer");
  const roleRecord =
    roleMap.get(role) ||
    normalizeRbacRoleRecord({
      id: role,
      name: role,
      title: role,
      permissions: getDefaultPermissionsForRole(role),
      built_in: isBuiltinRbacRole(role),
    });
  const rolePermissions = normalizeRbacPermissionList(
    roleRecord.permissions || [],
  );
  const permissionsOverride = normalizeRbacPermissionList(
    user.permissions_override || [],
  );
  const effectivePermissions = expandRbacPermissionSet([
    ...rolePermissions,
    ...permissionsOverride,
  ]);
  const fullName = [user.first_name, user.last_name]
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .join(" ");
  const displayName =
    fullName ||
    String(user.user_name || "").trim() ||
    String(user.email || "").trim() ||
    `User ${user.id}`;
  const status =
    String(user.status || "active")
      .trim()
      .toLowerCase() === "inactive"
      ? "inactive"
      : "active";

  return {
    id: Number(user.id),
    user_name: user.user_name || "",
    username: user.user_name || "",
    first_name: user.first_name || "",
    last_name: user.last_name || "",
    full_name: fullName || displayName,
    display_name: displayName,
    author_name: displayName,
    email: user.email || "",
    phone: user.phone || "",
    gender: user.gender || "",
    bio: user.bio || "",
    avatar: user.avatar || "",
    role,
    role_title: roleRecord.title || role,
    role_description: roleRecord.description || "",
    role_permissions: rolePermissions,
    permissions_override: permissionsOverride,
    effective_permissions: effectivePermissions,
    permissions: effectivePermissions,
    status,
    department: user.department || inferRbacDepartment(role),
    is_active: status !== "inactive",
    active: status !== "inactive",
    source: "server",
    created_at: user.created_at || null,
    updated_at: user.updated_at || null,
  };
};

const listRbacUsers = async ({ includeInactive = true } = {}) => {
  const roleMap = await getRbacRoleMap();
  const whereClause = includeInactive
    ? ""
    : "WHERE COALESCE(status, 'active') <> 'inactive'";
  const result = await db.query(
    `
    SELECT ${RBAC_USER_SELECT_FIELDS}
    FROM "user"
    ${whereClause}
    ORDER BY
      COALESCE(first_name, '') ASC,
      COALESCE(user_name, '') ASC,
      id ASC
    LIMIT 500
  `,
  );

  return (result.rows || []).map((user) => buildRbacUserPayload(user, roleMap));
};

const getRbacUserById = async (id) => {
  const result = await db.query(
    `
    SELECT ${RBAC_USER_SELECT_FIELDS}
    FROM "user"
    WHERE id = $1
    LIMIT 1
  `,
    [id],
  );
  if (!result.rows.length) return null;
  const roleMap = await getRbacRoleMap();
  return buildRbacUserPayload(result.rows[0], roleMap);
};

const logRbacActivity = async (
  req,
  { module, action, target = "", status = "success", note = "" } = {},
) => {
  try {
    const actor =
      req.user?.display_name ||
      req.user?.user_name ||
      req.user?.username ||
      req.user?.email ||
      "System";
    const actorRole = normalizeRbacRole(req.user?.role || "admin");
    await db.query(
      `
      INSERT INTO rbac_activity (actor, actor_role, module, action, target, status, note)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
    `,
      [
        String(actor || "System"),
        actorRole,
        String(module || "rbac"),
        String(action || "updated"),
        String(target || ""),
        String(status || "success"),
        String(note || ""),
      ],
    );
  } catch (err) {
    console.warn("Failed to write RBAC activity:", err.message);
  }
};

const requestHasRbacAccess = async (req, requestedPermissions = []) => {
  const role = normalizeRbacRole(req.user?.role || "");
  if (role === "admin" || role === "ceo") return true;

  const userId = Number(req.user?.id);
  if (!Number.isInteger(userId) || userId <= 0) return false;

  const user = await getRbacUserById(userId);
  const effectivePermissions = normalizeRbacPermissionList(
    user?.effective_permissions || [],
  );
  if (hasRbacPermissionSet(effectivePermissions, "*")) return true;

  const permissions = Array.isArray(requestedPermissions)
    ? requestedPermissions
    : [requestedPermissions];
  return permissions
    .map((permission) => normalizeRbacPermissionToken(permission))
    .filter(Boolean)
    .some((permission) =>
      hasRbacPermissionSet(effectivePermissions, permission),
    );
};

const requireRbacAccess = async (
  req,
  res,
  requestedPermissions = [],
  message = "RBAC permission required",
) => {
  if (await requestHasRbacAccess(req, requestedPermissions)) return true;
  res.status(403).json({ message });
  return false;
};

/* ---- RBAC (Users, Roles, Permissions) ---- */
app.get("/api/users", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["users.view", "users.manage"],
        "User management access required",
      ))
    )
      return;
    const includeInactive =
      String(req.query.includeInactive || "true") !== "false";
    const users = await listRbacUsers({ includeInactive });
    return res.json(users);
  } catch (err) {
    console.error("GET /api/users error:", err);
    return res.status(500).json({ message: "Failed to load users" });
  }
});

app.put("/api/users/:id", authenticate, async (req, res) => {
  try {
    const body = req.body || {};
    const requestedPermissions = Object.prototype.hasOwnProperty.call(
      body,
      "permissions_override",
    )
      ? ["users.edit", "users.manage", "permissions.manage"]
      : ["users.edit", "users.manage"];
    if (
      !(await requireRbacAccess(
        req,
        res,
        requestedPermissions,
        "User edit access required",
      ))
    )
      return;

    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid user id" });
    }

    const updates = [];
    const values = [];
    const addUpdate = (column, value) => {
      values.push(value);
      updates.push(`${column} = $${values.length}`);
    };

    if (Object.prototype.hasOwnProperty.call(body, "user_name")) {
      addUpdate("user_name", String(body.user_name || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "first_name")) {
      addUpdate("first_name", String(body.first_name || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "last_name")) {
      addUpdate("last_name", String(body.last_name || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "phone")) {
      addUpdate("phone", String(body.phone || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "gender")) {
      addUpdate("gender", String(body.gender || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "email")) {
      const email = String(body.email || "")
        .trim()
        .toLowerCase();
      if (!email) return res.status(400).json({ message: "Email is required" });
      addUpdate("email", email);
    }
    if (Object.prototype.hasOwnProperty.call(body, "role")) {
      const role = normalizeRbacRole(body.role || "viewer");
      const roleMap = await getRbacRoleMap();
      if (!roleMap.has(role)) {
        return res.status(400).json({ message: "Unknown role" });
      }
      addUpdate("role", role);
    }
    if (Object.prototype.hasOwnProperty.call(body, "status")) {
      const status =
        String(body.status || "active")
          .trim()
          .toLowerCase() === "inactive"
          ? "inactive"
          : "active";
      addUpdate("status", status);
    }
    if (Object.prototype.hasOwnProperty.call(body, "department")) {
      addUpdate("department", String(body.department || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "bio")) {
      addUpdate("bio", String(body.bio || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "avatar")) {
      addUpdate("avatar", String(body.avatar || "").trim() || null);
    }
    if (Object.prototype.hasOwnProperty.call(body, "permissions_override")) {
      values.push(
        JSON.stringify(normalizeRbacPermissionList(body.permissions_override)),
      );
      updates.push(`permissions_override = $${values.length}::jsonb`);
    }
    if (String(body.password || "").trim()) {
      addUpdate(
        "password",
        await bcrypt.hash(String(body.password).trim(), 10),
      );
    }

    if (!updates.length) {
      const currentUser = await getRbacUserById(id);
      if (!currentUser)
        return res.status(404).json({ message: "User not found" });
      return res.json({ user: currentUser });
    }

    values.push(id);
    const result = await db.query(
      `
      UPDATE "user"
      SET ${updates.join(", ")}, updated_at = now()
      WHERE id = $${values.length}
      RETURNING ${RBAC_USER_SELECT_FIELDS}
    `,
      values,
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const roleMap = await getRbacRoleMap();
    const user = buildRbacUserPayload(result.rows[0], roleMap);
    await logRbacActivity(req, {
      module: "users",
      action: "updated",
      target: user.display_name,
      note: "Updated user profile or permission overrides.",
    });
    return res.json({ user });
  } catch (err) {
    if (err.code === "23505") {
      return res.status(409).json({ message: "Email already registered" });
    }
    console.error("PUT /api/users/:id error:", err);
    return res.status(500).json({ message: "Failed to update user" });
  }
});

app.delete("/api/users/:id", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["users.delete", "users.manage"],
        "User delete access required",
      ))
    )
      return;
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid user id" });
    }

    const currentUser = await getRbacUserById(id);
    if (!currentUser)
      return res.status(404).json({ message: "User not found" });

    await db.query(`DELETE FROM "user" WHERE id = $1`, [id]);
    await logRbacActivity(req, {
      module: "users",
      action: "deleted",
      target: currentUser.display_name,
      note: "Deleted user account.",
    });
    return res.json({ message: "User deleted", user: currentUser });
  } catch (err) {
    console.error("DELETE /api/users/:id error:", err);
    return res.status(500).json({ message: "Failed to delete user" });
  }
});

app.get("/api/rbac/users", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;
    const includeInactive =
      String(req.query.includeInactive || "true") !== "false";
    const users = await listRbacUsers({ includeInactive });
    return res.json(users);
  } catch (err) {
    console.error("GET /api/rbac/users error:", err);
    return res.status(500).json({ message: "Failed to load users" });
  }
});

app.post("/api/rbac/users/:id/roles", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["users.assign", "users.manage", "roles.manage"],
        "Role assignment access required",
      ))
    )
      return;

    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid user id" });
    }

    const rawRole = req.body?.role_id || req.body?.role || req.body?.name;
    const requestedRole = normalizeRbacRole(rawRole || "");
    const roleMap = await getRbacRoleMap();
    const roleRecord = roleMap.get(requestedRole);

    if (!roleRecord) {
      return res.status(400).json({ message: "Unknown role" });
    }

    const result = await db.query(
      `
      UPDATE "user"
      SET role = $1, updated_at = now()
      WHERE id = $2
      RETURNING ${RBAC_USER_SELECT_FIELDS}
    `,
      [roleRecord.name, id],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const user = buildRbacUserPayload(result.rows[0], roleMap);
    await logRbacActivity(req, {
      module: "users",
      action: "role_assigned",
      target: user.display_name,
      note: `Assigned ${roleRecord.title || roleRecord.name} role.`,
    });
    return res.json({ user });
  } catch (err) {
    console.error("POST /api/rbac/users/:id/roles error:", err);
    return res.status(500).json({ message: "Failed to assign role" });
  }
});

app.get("/api/rbac/roles", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        [
          "roles.view",
          "roles.manage",
          "permissions.view",
          "permissions.manage",
        ],
        "Role catalog access required",
      ))
    )
      return;
    const roles = await listRbacRoleRecords();
    return res.json(roles);
  } catch (err) {
    console.error("GET /api/rbac/roles error:", err);
    return res.status(500).json({ message: "Failed to load RBAC roles" });
  }
});

app.post("/api/rbac/roles", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["roles.create", "roles.manage"],
        "Role create access required",
      ))
    )
      return;

    const body = req.body || {};
    const rawName = String(body.name || "").trim();
    if (!rawName)
      return res.status(400).json({ message: "Role name is required" });

    const name = normalizeRbacRole(rawName);
    if (isBuiltinRbacRole(name)) {
      return res
        .status(409)
        .json({ message: "Built-in roles cannot be recreated" });
    }

    const permissions = normalizeRbacPermissionList(body.permissions || []);
    const title = String(body.title || rawName).trim();
    const description = String(body.description || "").trim();
    const result = await db.query(
      `
      INSERT INTO rbac_roles (id, name, title, description, permissions, built_in)
      VALUES ($1,$2,$3,$4,$5::jsonb,false)
      ON CONFLICT (id)
      DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        permissions = EXCLUDED.permissions,
        built_in = false,
        updated_at = now()
      RETURNING id, name, title, description, permissions, built_in, created_at, updated_at
    `,
      [name, name, title, description, JSON.stringify(permissions)],
    );

    const role = normalizeRbacRoleRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "roles",
      action: "created",
      target: role.title,
      note: "Created or updated custom role.",
    });
    return res.status(201).json(role);
  } catch (err) {
    console.error("POST /api/rbac/roles error:", err);
    return res.status(500).json({ message: "Failed to save role" });
  }
});

app.put("/api/rbac/roles/:id", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["roles.edit", "roles.manage"],
        "Role edit access required",
      ))
    )
      return;

    const rawName = String(req.params.id || "").trim();
    if (!rawName)
      return res.status(400).json({ message: "Role id is required" });

    const name = normalizeRbacRole(rawName);
    if (isBuiltinRbacRole(name)) {
      return res
        .status(403)
        .json({ message: "Built-in roles cannot be edited" });
    }

    const body = req.body || {};
    const permissions = normalizeRbacPermissionList(body.permissions || []);
    const title = String(body.title || body.name || name).trim();
    const description = String(body.description || "").trim();
    const result = await db.query(
      `
      UPDATE rbac_roles
      SET title = $1,
          description = $2,
          permissions = $3::jsonb,
          updated_at = now()
      WHERE id = $4
      RETURNING id, name, title, description, permissions, built_in, created_at, updated_at
    `,
      [title, description, JSON.stringify(permissions), name],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Role not found" });
    }

    const role = normalizeRbacRoleRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "roles",
      action: "updated",
      target: role.title,
      note: "Updated custom role.",
    });
    return res.json(role);
  } catch (err) {
    console.error("PUT /api/rbac/roles/:id error:", err);
    return res.status(500).json({ message: "Failed to update role" });
  }
});

app.delete("/api/rbac/roles/:id", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["roles.delete", "roles.manage"],
        "Role delete access required",
      ))
    )
      return;

    const rawName = String(req.params.id || "").trim();
    if (!rawName)
      return res.status(400).json({ message: "Role id is required" });

    const name = normalizeRbacRole(rawName);
    if (isBuiltinRbacRole(name)) {
      return res
        .status(403)
        .json({ message: "Built-in roles cannot be deleted" });
    }

    const result = await db.query(
      `
      DELETE FROM rbac_roles
      WHERE id = $1
      RETURNING id, name, title, description, permissions, built_in, created_at, updated_at
    `,
      [name],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Role not found" });
    }

    await db.query(
      `UPDATE "user" SET role = 'viewer', updated_at = now() WHERE role = $1`,
      [name],
    );

    const role = normalizeRbacRoleRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "roles",
      action: "deleted",
      target: role.title,
      note: "Deleted custom role and moved assigned users to Viewer.",
    });
    return res.json({ message: "Role deleted", role });
  } catch (err) {
    console.error("DELETE /api/rbac/roles/:id error:", err);
    return res.status(500).json({ message: "Failed to delete role" });
  }
});

app.get("/api/rbac/permissions", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        [
          "permissions.view",
          "permissions.manage",
          "roles.view",
          "roles.manage",
        ],
        "Permission catalog access required",
      ))
    )
      return;
    const permissions = await listRbacPermissionRecords();
    return res.json(permissions);
  } catch (err) {
    console.error("GET /api/rbac/permissions error:", err);
    return res.status(500).json({ message: "Failed to load RBAC permissions" });
  }
});

app.post("/api/rbac/permissions", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["permissions.create", "permissions.manage"],
        "Permission create access required",
      ))
    )
      return;

    const body = req.body || {};
    const name = normalizeRbacPermissionToken(body.name || body.id || "");
    if (!name) {
      return res.status(400).json({ message: "Permission name is required" });
    }
    if (isBuiltinRbacPermission(name)) {
      return res
        .status(409)
        .json({ message: "Built-in permissions cannot be recreated" });
    }

    const derived = splitRbacPermissionName(name);
    const result = await db.query(
      `
      INSERT INTO rbac_permissions
        (id, name, description, module, module_label, action, built_in)
      VALUES ($1,$2,$3,$4,$5,$6,false)
      ON CONFLICT (id)
      DO UPDATE SET
        description = EXCLUDED.description,
        module = EXCLUDED.module,
        module_label = EXCLUDED.module_label,
        action = EXCLUDED.action,
        built_in = false,
        updated_at = now()
      RETURNING id, name, description, module, module_label, action, built_in, created_at, updated_at
    `,
      [
        name,
        name,
        String(body.description || "").trim(),
        String(body.module || derived.module || "").trim(),
        String(body.module_label || body.module || derived.module || "").trim(),
        String(body.action || derived.action || "").trim(),
      ],
    );

    const permission = normalizeRbacPermissionRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "permissions",
      action: "created",
      target: permission.name,
      note: "Created or updated custom permission.",
    });
    return res.status(201).json(permission);
  } catch (err) {
    console.error("POST /api/rbac/permissions error:", err);
    return res.status(500).json({ message: "Failed to save permission" });
  }
});

app.put("/api/rbac/permissions/:id", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["permissions.edit", "permissions.manage"],
        "Permission edit access required",
      ))
    )
      return;

    const name = normalizeRbacPermissionToken(req.params.id || "");
    if (!name)
      return res.status(400).json({ message: "Permission id is required" });
    if (isBuiltinRbacPermission(name)) {
      return res
        .status(403)
        .json({ message: "Built-in permissions cannot be edited" });
    }

    const body = req.body || {};
    const derived = splitRbacPermissionName(name);
    const result = await db.query(
      `
      UPDATE rbac_permissions
      SET description = $1,
          module = $2,
          module_label = $3,
          action = $4,
          updated_at = now()
      WHERE id = $5
      RETURNING id, name, description, module, module_label, action, built_in, created_at, updated_at
    `,
      [
        String(body.description || "").trim(),
        String(body.module || derived.module || "").trim(),
        String(body.module_label || body.module || derived.module || "").trim(),
        String(body.action || derived.action || "").trim(),
        name,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Permission not found" });
    }

    const permission = normalizeRbacPermissionRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "permissions",
      action: "updated",
      target: permission.name,
      note: "Updated custom permission.",
    });
    return res.json(permission);
  } catch (err) {
    console.error("PUT /api/rbac/permissions/:id error:", err);
    return res.status(500).json({ message: "Failed to update permission" });
  }
});

app.delete("/api/rbac/permissions/:id", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["permissions.delete", "permissions.manage"],
        "Permission delete access required",
      ))
    )
      return;

    const name = normalizeRbacPermissionToken(req.params.id || "");
    if (!name)
      return res.status(400).json({ message: "Permission id is required" });
    if (isBuiltinRbacPermission(name)) {
      return res
        .status(403)
        .json({ message: "Built-in permissions cannot be deleted" });
    }

    const result = await db.query(
      `
      DELETE FROM rbac_permissions
      WHERE id = $1
      RETURNING id, name, description, module, module_label, action, built_in, created_at, updated_at
    `,
      [name],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Permission not found" });
    }

    await db.query(
      `
      UPDATE rbac_roles
      SET permissions = (
            SELECT COALESCE(jsonb_agg(permission_value.value), '[]'::jsonb)
            FROM jsonb_array_elements_text(permissions) AS permission_value(value)
            WHERE permission_value.value <> $1
          ),
          updated_at = now()
      WHERE permissions ? $1
    `,
      [name],
    );

    const permission = normalizeRbacPermissionRecord(result.rows[0]);
    await logRbacActivity(req, {
      module: "permissions",
      action: "deleted",
      target: permission.name,
      note: "Deleted custom permission.",
    });
    return res.json({ message: "Permission deleted", permission });
  } catch (err) {
    console.error("DELETE /api/rbac/permissions/:id error:", err);
    return res.status(500).json({ message: "Failed to delete permission" });
  }
});

app.get("/api/rbac/activity", authenticate, async (req, res) => {
  try {
    if (
      !(await requireRbacAccess(
        req,
        res,
        ["activity.view", "reports.view"],
        "Activity access required",
      ))
    )
      return;
    const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
    const result = await db.query(
      `
      SELECT id, actor, actor_role, module, action, target, status, note, created_at
      FROM rbac_activity
      ORDER BY created_at DESC, id DESC
      LIMIT $1
    `,
      [limit],
    );
    return res.json(
      (result.rows || []).map((activity) => ({
        ...activity,
        at: activity.created_at,
      })),
    );
  } catch (err) {
    console.error("GET /api/rbac/activity error:", err);
    return res.status(500).json({ message: "Failed to load RBAC activity" });
  }
});

app.get("/api/admin/blogs/candidates", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

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
      if (!ensureBlogManagerAccess(req, res)) return;

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

      const tokenMap = buildBlogTokenMap(snapshot);
      const suggestions = buildBlogSuggestions(snapshot, tokenMap);
      const existing = await db.query(
        `
      SELECT
        id,
        product_id,
        title,
        slug,
        excerpt,
        content_template,
        content_rendered,
        status,
        blog_eligible,
        meta_title,
        meta_description,
        COALESCE(
          hero_image,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = blogs.product_id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          )
        ) AS hero_image,
        published_at,
        created_at,
        updated_at
      FROM blogs
      WHERE product_id = $1
      LIMIT 1
    `,
        [productId],
      );

      return res.json({
        product: {
          product_id: snapshot.product_id,
          product_type: snapshot.product_type,
          name: snapshot.core?.name || "",
          brand_name: snapshot.core?.brand_name || "",
          spec_score: snapshot.scored?.spec_score ?? 0,
          price: formatBlogPrice(snapshot.lowest_price) || null,
          image: snapshot.hero_image || null,
          images: Array.isArray(snapshot.images) ? snapshot.images : [],
        },
        token_map: tokenMap,
        token_keys: Object.keys(tokenMap).sort(),
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

app.post("/api/admin/blogs/context", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

    const requestedProductIds = normalizePositiveIntegerList(
      Array.isArray(req.body?.product_ids)
        ? req.body.product_ids
        : Array.isArray(req.body?.productIds)
          ? req.body.productIds
          : [req.body?.product_id],
    );
    const requestedPrimaryProductId = Number(
      req.body?.primary_product_id ?? req.body?.product_id,
    );
    const orderedProductIds = normalizePositiveIntegerList([
      requestedPrimaryProductId,
      ...requestedProductIds,
    ]);

    if (!orderedProductIds.length) {
      return res
        .status(400)
        .json({ message: "Select at least one product first" });
    }

    const profileConfig = await readDeviceFieldProfilesConfig();
    const snapshots = await Promise.all(
      orderedProductIds.map((productId) =>
        fetchBlogProductSnapshot(productId, profileConfig.profiles),
      ),
    );

    const missingProductId = orderedProductIds.find(
      (_productId, index) => !snapshots[index],
    );
    if (missingProductId) {
      return res.status(404).json({
        message: `Product not found or unsupported: ${missingProductId}`,
      });
    }

    const products = snapshots.map((snapshot) => ({
      product_id: snapshot.product_id,
      id: snapshot.product_id,
      product_type: snapshot.product_type,
      name: snapshot.core?.name || "",
      brand_name: snapshot.core?.brand_name || "",
      spec_score: snapshot.scored?.spec_score ?? 0,
      price: formatBlogPrice(snapshot.lowest_price) || null,
      image: snapshot.hero_image || null,
      image_url: snapshot.hero_image || null,
      hero_image: snapshot.hero_image || null,
      images: Array.isArray(snapshot.images) ? snapshot.images : [],
    }));

    const primarySnapshot = snapshots[0];
    const tokenMap = buildBlogTokenMap(primarySnapshot);
    const tokenKeys = Object.keys(tokenMap).sort();
    const suggestions = buildBlogSuggestions(primarySnapshot, tokenMap);

    let existingBlog = null;
    const shouldMatchExisting =
      req.body?.match_existing === true || req.body?.matchExisting === true;
    if (shouldMatchExisting) {
      const existing = await db.query(
        `
        SELECT
          bl.id,
          bl.product_id,
          bl.title,
          bl.slug,
          bl.excerpt,
          bl.content_template,
          bl.content_rendered,
          bl.status,
          bl.is_published,
          bl.author_name,
          bl.author_user_id,
          bl.blog_eligible,
          bl.eligibility_snapshot,
          bl.token_snapshot,
          bl.meta_title,
          bl.meta_description,
          bl.category,
          COALESCE(
            bl.hero_image,
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = bl.product_id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            )
          ) AS hero_image,
          bl.hero_image_source,
          bl.hero_image_alt,
          bl.hero_image_caption,
          bl.tags,
          bl.featured,
          bl.trending,
          bl.pinned,
          bl.published_at,
          bl.created_at,
          bl.updated_at
        FROM blogs bl
        WHERE bl.product_id = $1
           OR EXISTS (
             SELECT 1
             FROM blog_product_links bpl
             WHERE bpl.blog_id = bl.id
               AND bpl.product_id = $1
           )
        ORDER BY
          CASE WHEN bl.product_id = $1 THEN 0 ELSE 1 END,
          bl.updated_at DESC,
          bl.id DESC
        LIMIT 1
      `,
        [orderedProductIds[0]],
      );

      if (existing.rows[0]) {
        const row = existing.rows[0];
        const existingProductIds = await readBlogLinkedProductIds(
          db,
          row.id,
          row.product_id,
        );
        const existingTokenMap = toPlainObject(row.token_snapshot);
        existingBlog = {
          ...row,
          product_ids: existingProductIds,
          primary_product_id:
            existingProductIds[0] || Number(row.product_id) || null,
          token_map: existingTokenMap,
          token_keys: Object.keys(existingTokenMap).sort(),
          category: row.category || null,
          author_name: row.author_name || "",
          author_user_id: row.author_user_id || null,
          hero_image_source: row.hero_image_source || null,
          hero_image_alt: row.hero_image_alt || "",
          hero_image_caption: row.hero_image_caption || "",
          tags: normalizeBlogTagsInput(row.tags),
          featured: Boolean(row.featured),
          trending: Boolean(row.trending),
          pinned: Boolean(row.pinned),
        };
      }
    }

    return res.json({
      product_ids: orderedProductIds,
      primary_product_id: orderedProductIds[0],
      products,
      product: products[0] || null,
      token_map: tokenMap,
      token_keys: tokenKeys,
      suggestions,
      existing_blog: existingBlog,
    });
  } catch (err) {
    console.error("POST /api/admin/blogs/context error:", err);
    return res.status(500).json({ message: "Failed to load blog context" });
  }
});

app.post("/api/admin/blogs/preview", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

    const productIdRaw = req.body?.product_id;
    const productId = Number(productIdRaw);
    const hasProductId = Number.isInteger(productId) && productId > 0;
    const content = String(req.body?.content || "");
    if (!content.trim()) {
      return res.status(400).json({ message: "content is required" });
    }

    let tokenMap = toPlainObject(req.body?.token_map);
    if (hasProductId) {
      const profileConfig = await readDeviceFieldProfilesConfig();
      const snapshot = await fetchBlogProductSnapshot(
        productId,
        profileConfig.profiles,
      );
      if (!snapshot) {
        return res.status(404).json({ message: "Product not found" });
      }
      tokenMap = buildBlogTokenMap(snapshot);
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
    if (!ensureBlogManagerAccess(req, res)) return;

    const rawBlogId = Number(req.body?.blog_id);
    const requestedProductIds = normalizePositiveIntegerList(
      Array.isArray(req.body?.product_ids)
        ? req.body.product_ids
        : Array.isArray(req.body?.productIds)
          ? req.body.productIds
          : [],
    );
    const requestedPrimaryProductId = Number(
      req.body?.primary_product_id ?? req.body?.product_id,
    );
    const orderedProductIds = normalizePositiveIntegerList([
      requestedPrimaryProductId,
      ...requestedProductIds,
    ]);
    const rawProductId = orderedProductIds[0] || 0;
    const hasBlogId = Number.isInteger(rawBlogId) && rawBlogId > 0;
    const hasProductId = Number.isInteger(rawProductId) && rawProductId > 0;
    let targetBlogId = hasBlogId ? rawBlogId : null;
    const productId = hasProductId ? rawProductId : null;

    const title = String(req.body?.title || "").trim();
    const excerpt = String(req.body?.excerpt || "").trim();
    const contentTemplate = String(req.body?.content_template || "").trim();
    const requestedSlug = String(req.body?.slug || "").trim();
    const hasIsPublishedInput =
      Object.prototype.hasOwnProperty.call(req.body || {}, "is_published") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "isPublished") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "publish");
    const requestedIsPublished = hasIsPublishedInput
      ? parseBooleanInput(
          req.body?.is_published ?? req.body?.isPublished ?? req.body?.publish,
        )
      : null;
    const requestedStatus = String(req.body?.status || "draft")
      .trim()
      .toLowerCase();
    const status = hasIsPublishedInput
      ? requestedIsPublished
        ? "published"
        : "draft"
      : BLOG_ALLOWED_STATUSES.has(requestedStatus)
        ? requestedStatus
        : "draft";
    const isPublished = status === "published";
    const metaTitle = String(req.body?.meta_title || "").trim();
    const metaDescription = String(req.body?.meta_description || "").trim();
    const category =
      normalizeBlogTextField(req.body?.category ?? req.body?.section, 80) ||
      null;
    const brandName =
      normalizeBlogTextField(
        req.body?.brand_name ?? req.body?.brandName ?? req.body?.brand,
        120,
      ) || null;
    const heroImageSource =
      normalizeBlogTextField(
        req.body?.hero_image_source ?? req.body?.heroImageSource,
        300,
      ) || null;
    const heroImageAlt =
      normalizeBlogTextField(
        req.body?.hero_image_alt ?? req.body?.heroImageAlt,
        180,
      ) || null;
    const heroImageCaption =
      normalizeBlogTextField(
        req.body?.hero_image_caption ?? req.body?.heroImageCaption,
        300,
      ) || null;
    const tags = normalizeBlogTagsInput(req.body?.tags ?? req.body?.keywords);
    const featured = parseBooleanInput(req.body?.featured);
    const trending = parseBooleanInput(req.body?.trending);
    const pinned = parseBooleanInput(req.body?.pinned);
    const rawAuthorUserId = Number(
      req.body?.author_user_id ?? req.body?.authorUserId,
    );
    const authorUserId =
      Number.isInteger(rawAuthorUserId) && rawAuthorUserId > 0
        ? rawAuthorUserId
        : null;
    let authorName = String(
      req.body?.author_name ??
        req.body?.authorName ??
        req.body?.byline ??
        req.body?.author ??
        "",
    ).trim();

    if (!title) return res.status(400).json({ message: "title is required" });
    if (!contentTemplate) {
      return res.status(400).json({ message: "content_template is required" });
    }

    if (!targetBlogId && productId) {
      const existingByProduct = await db.query(
        "SELECT id FROM blogs WHERE product_id = $1 LIMIT 1",
        [productId],
      );
      targetBlogId = Number(existingByProduct.rows[0]?.id) || null;
    }

    let snapshot = null;
    if (productId) {
      const profileConfig = await readDeviceFieldProfilesConfig();
      snapshot = await fetchBlogProductSnapshot(
        productId,
        profileConfig.profiles,
      );
      if (!snapshot) {
        return res.status(404).json({ message: "Product not found" });
      }
    }

    const eligibilitySnapshot = {
      advisory_only: true,
      product_linked: Boolean(productId),
    };

    const requestedTokenMap = toPlainObject(req.body?.token_map);
    const tokenMap = snapshot
      ? { ...buildBlogTokenMap(snapshot), ...requestedTokenMap }
      : requestedTokenMap;

    const contentRendered = renderBlogTemplateWithTokens(
      contentTemplate,
      tokenMap,
      {
        preserveUnknown: true,
      },
    );
    const unresolvedTokens = collectTemplateTokens(contentRendered);
    if (isPublished && unresolvedTokens.length) {
      return res.status(400).json({
        message: `Resolve content placeholders before publishing: ${unresolvedTokens
          .map((token) => `{{${token}}}`)
          .join(", ")}`,
        unresolved_tokens: unresolvedTokens,
      });
    }
    const slug = await resolveUniqueBlogSlug(
      requestedSlug || title || snapshot?.core?.name,
      productId,
      targetBlogId,
    );
    const heroImage = String(
      req.body?.hero_image || snapshot?.hero_image || "",
    ).trim();
    const nowPublishedAt = isPublished ? new Date() : null;
    const actorId =
      Number.isInteger(Number(req.user?.id)) && Number(req.user?.id) > 0
        ? Number(req.user.id)
        : null;
    if (!authorName && authorUserId) {
      const authorResult = await db.query(
        `
        SELECT user_name, first_name, last_name, email
        FROM "user"
        WHERE id = $1
        LIMIT 1
        `,
        [authorUserId],
      );
      const author = authorResult.rows[0] || null;
      authorName =
        [author?.first_name, author?.last_name]
          .map((value) => String(value || "").trim())
          .filter(Boolean)
          .join(" ") ||
        String(author?.user_name || "").trim() ||
        String(author?.email || "").trim();
    }

    let writeResult;
    if (targetBlogId) {
      writeResult = await db.query(
        `
        UPDATE blogs
        SET
          product_id = $2,
          title = $3,
          slug = $4,
          excerpt = $5,
          content_template = $6,
          content_rendered = $7,
          status = $8,
          is_published = ($8 = 'published'),
          blog_eligible = $9,
          eligibility_snapshot = $10::jsonb,
          token_snapshot = $11::jsonb,
          meta_title = $12,
          meta_description = $13,
          hero_image = $14,
          hero_image_source = $15,
          hero_image_alt = $16,
          hero_image_caption = $17,
          category = $18,
          brand_name = $19,
          tags = $20::jsonb,
          featured = $21,
          trending = $22,
          pinned = $23,
          author_name = $24,
          author_user_id = $25,
          updated_by = $26,
          published_at = CASE
            WHEN $8 = 'published' THEN COALESCE(published_at, $27)
            ELSE NULL
          END,
          updated_at = now()
        WHERE id = $1
        RETURNING
          id,
          product_id,
          title,
          slug,
          excerpt,
          content_template,
          content_rendered,
          status,
          is_published,
          blog_eligible,
          eligibility_snapshot,
          token_snapshot,
          meta_title,
          meta_description,
          hero_image,
          hero_image_source,
          hero_image_alt,
          hero_image_caption,
          category,
          brand_name,
          tags,
          featured,
          trending,
          pinned,
          author_name,
          author_user_id,
          published_at,
          created_at,
          updated_at
      `,
        [
          targetBlogId,
          productId,
          title,
          slug,
          excerpt || null,
          contentTemplate,
          contentRendered,
          status,
          Boolean(productId),
          JSON.stringify(eligibilitySnapshot),
          JSON.stringify(tokenMap),
          metaTitle || null,
          metaDescription || null,
          heroImage || null,
          heroImageSource,
          heroImageAlt,
          heroImageCaption,
          category,
          brandName,
          JSON.stringify(tags),
          featured,
          trending,
          pinned,
          authorName || null,
          authorUserId,
          actorId,
          nowPublishedAt,
        ],
      );
      if (!writeResult.rows.length) {
        return res.status(404).json({ message: "Blog not found" });
      }
    } else {
      writeResult = await db.query(
        `
        INSERT INTO blogs (
          product_id,
          title,
          slug,
          excerpt,
          content_template,
          content_rendered,
          status,
          is_published,
          blog_eligible,
          eligibility_snapshot,
          token_snapshot,
          meta_title,
          meta_description,
          hero_image,
          hero_image_source,
          hero_image_alt,
          hero_image_caption,
          category,
          brand_name,
          tags,
          featured,
          trending,
          pinned,
          author_name,
          author_user_id,
          created_by,
          updated_by,
          published_at,
          created_at,
          updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,($7 = 'published'),$8,$9::jsonb,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20,$21,$22,$23,$24,$25,$26,$27,now(),now()
        )
        RETURNING
          id,
          product_id,
          title,
          slug,
          excerpt,
          content_template,
          content_rendered,
          status,
          is_published,
          blog_eligible,
          eligibility_snapshot,
          token_snapshot,
          meta_title,
          meta_description,
          hero_image,
          hero_image_source,
          hero_image_alt,
          hero_image_caption,
          category,
          brand_name,
          tags,
          featured,
          trending,
          pinned,
          author_name,
          author_user_id,
          published_at,
          created_at,
          updated_at
      `,
        [
          productId,
          title,
          slug,
          excerpt || null,
          contentTemplate,
          contentRendered,
          status,
          Boolean(productId),
          JSON.stringify(eligibilitySnapshot),
          JSON.stringify(tokenMap),
          metaTitle || null,
          metaDescription || null,
          heroImage || null,
          heroImageSource,
          heroImageAlt,
          heroImageCaption,
          category,
          brandName,
          JSON.stringify(tags),
          featured,
          trending,
          pinned,
          authorName || null,
          authorUserId,
          actorId,
          actorId,
          nowPublishedAt,
        ],
      );
    }

    if (writeResult.rows[0]?.id) {
      const syncedProductIds = await syncBlogProductLinks(
        db,
        writeResult.rows[0].id,
        orderedProductIds,
      );
      writeResult.rows[0].product_ids = syncedProductIds;
      writeResult.rows[0].primary_product_id = syncedProductIds[0] || null;
    }

    return res.status(201).json({
      message: "Blog saved successfully",
      blog: writeResult.rows[0],
      unresolved_tokens: unresolvedTokens,
    });
  } catch (err) {
    console.error("POST /api/admin/blogs error:", err);
    return res.status(500).json({ message: "Failed to save blog" });
  }
});

app.get("/api/admin/blogs", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

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
        bl.title,
        bl.slug,
        bl.status,
        bl.is_published,
        COALESCE(
          NULLIF(BTRIM(bl.author_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', blog_author.first_name, blog_author.last_name)), ''),
          NULLIF(BTRIM(blog_author.user_name), ''),
          NULLIF(BTRIM(blog_author.email), '')
        ) AS author_name,
        COALESCE(bl.author_user_id, bl.updated_by, bl.created_by) AS author_user_id,
        blog_author.role AS author_role,
        bl.category,
        bl.blog_eligible,
        COALESCE(
          bl.hero_image,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = bl.product_id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          )
        ) AS hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        COALESCE(NULLIF(BTRIM(bl.brand_name), ''), b.name) AS brand_name
      FROM blogs bl
      LEFT JOIN "user" blog_author
        ON blog_author.id = bl.author_user_id
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

    return res.json({
      page,
      limit,
      total: countRes.rows[0]?.total || 0,
      rows: listRes.rows || [],
    });
  } catch (err) {
    console.error("GET /api/admin/blogs error:", err);
    return res.status(500).json({ message: "Failed to fetch blogs" });
  }
});

app.get("/api/admin/blogs/:id", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

    const blogId = Number(req.params.id);
    if (!Number.isInteger(blogId) || blogId <= 0) {
      return res.status(400).json({ message: "Invalid blog id" });
    }

    const blogRes = await db.query(
      `
      SELECT
        bl.id,
        bl.product_id,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.content_template,
        bl.content_rendered,
        bl.status,
        bl.is_published,
        COALESCE(
          NULLIF(BTRIM(bl.author_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', blog_author.first_name, blog_author.last_name)), ''),
          NULLIF(BTRIM(blog_author.user_name), ''),
          NULLIF(BTRIM(blog_author.email), '')
        ) AS author_name,
        COALESCE(bl.author_user_id, bl.updated_by, bl.created_by) AS author_user_id,
        blog_author.role AS author_role,
        bl.category,
        bl.blog_eligible,
        bl.eligibility_snapshot,
        bl.token_snapshot,
        bl.meta_title,
        bl.meta_description,
        COALESCE(
          bl.hero_image,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = bl.product_id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          )
        ) AS hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.created_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        COALESCE(NULLIF(BTRIM(bl.brand_name), ''), b.name) AS brand_name
      FROM blogs bl
      LEFT JOIN "user" blog_author
        ON blog_author.id = bl.author_user_id
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE bl.id = $1
      LIMIT 1
    `,
      [blogId],
    );

    const blog = blogRes.rows[0] || null;
    if (!blog) {
      return res.status(404).json({ message: "Blog not found" });
    }

    const productIds = await readBlogLinkedProductIds(
      db,
      blog.id,
      blog.product_id,
    );

    let products = [];
    if (productIds.length) {
      const productsRes = await db.query(
        `
        SELECT
          p.id AS product_id,
          p.name,
          p.product_type,
          b.name AS brand_name,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = p.id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          ) AS image
        FROM products p
        LEFT JOIN brands b
          ON b.id = p.brand_id
        WHERE p.id = ANY($1::int[])
        ORDER BY array_position($1::int[], p.id)
      `,
        [productIds],
      );
      products = (productsRes.rows || []).map((row) => ({
        product_id: Number(row.product_id),
        id: Number(row.product_id),
        name: row.name || "",
        product_type: row.product_type || "",
        brand_name: row.brand_name || "",
        image: row.image || null,
        hero_image: row.image || null,
      }));
    }

    const tokenMap = toPlainObject(blog.token_snapshot);
    const firstProduct = products[0] || null;

    return res.json({
      blog: {
        ...blog,
        product_ids: productIds,
        primary_product_id: productIds[0] || Number(blog.product_id) || null,
        products,
        token_map: tokenMap,
        token_keys: Object.keys(tokenMap).sort(),
        product_type: blog.product_type || firstProduct?.product_type || null,
        category: blog.category || null,
        author_name: blog.author_name || "",
        author_user_id: blog.author_user_id || null,
        hero_image_source: blog.hero_image_source || null,
        hero_image_alt: blog.hero_image_alt || "",
        hero_image_caption: blog.hero_image_caption || "",
        tags: normalizeBlogTagsInput(blog.tags),
        featured: Boolean(blog.featured),
        trending: Boolean(blog.trending),
        pinned: Boolean(blog.pinned),
      },
    });
  } catch (err) {
    console.error("GET /api/admin/blogs/:id error:", err);
    return res.status(500).json({ message: "Failed to fetch blog" });
  }
});

app.patch("/api/admin/blogs/:id/publish", authenticate, async (req, res) => {
  try {
    if (!ensureBlogManagerAccess(req, res)) return;

    const blogId = Number(req.params.id);
    if (!Number.isInteger(blogId) || blogId <= 0) {
      return res.status(400).json({ message: "Invalid blog id" });
    }

    const hasIsPublishedInput =
      Object.prototype.hasOwnProperty.call(req.body || {}, "is_published") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "isPublished") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "publish");
    if (!hasIsPublishedInput) {
      return res
        .status(400)
        .json({ message: "is_published boolean is required" });
    }

    const isPublished = parseBooleanInput(
      req.body?.is_published ?? req.body?.isPublished ?? req.body?.publish,
    );
    const existingResult = await db.query(
      `
      SELECT id, content_rendered, content_template
      FROM blogs
      WHERE id = $1
      LIMIT 1
      `,
      [blogId],
    );

    if (!existingResult.rows.length) {
      return res.status(404).json({ message: "Blog not found" });
    }

    if (isPublished) {
      const unresolvedTokens = collectTemplateTokens(
        existingResult.rows[0].content_rendered ||
          existingResult.rows[0].content_template ||
          "",
      );
      if (unresolvedTokens.length) {
        return res.status(400).json({
          message: `Resolve content placeholders before publishing: ${unresolvedTokens
            .map((token) => `{{${token}}}`)
            .join(", ")}`,
          unresolved_tokens: unresolvedTokens,
        });
      }
    }

    const actorId =
      Number.isInteger(Number(req.user?.id)) && Number(req.user?.id) > 0
        ? Number(req.user.id)
        : null;
    const updateResult = await db.query(
      `
      UPDATE blogs
      SET
        is_published = $2,
        status = CASE WHEN $2 THEN 'published' ELSE 'draft' END,
        published_at = CASE WHEN $2 THEN COALESCE(published_at, now()) ELSE NULL END,
        updated_by = $3,
        updated_at = now()
      WHERE id = $1
      RETURNING
        id,
        product_id,
        title,
        slug,
        status,
        is_published,
        published_at,
        updated_at
      `,
      [blogId, isPublished, actorId],
    );

    return res.json({
      message: isPublished
        ? "Blog published successfully"
        : "Blog unpublished successfully",
      blog: updateResult.rows[0],
    });
  } catch (err) {
    console.error("PATCH /api/admin/blogs/:id/publish error:", err);
    return res
      .status(500)
      .json({ message: "Failed to update blog publish status" });
  }
});

app.delete(
  "/api/admin/blogs/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
    try {
      if (!ensureBlogManagerAccess(req, res)) return;

      const blogId = Number(req.params.id);
      if (!Number.isInteger(blogId) || blogId <= 0) {
        return res.status(400).json({ message: "Invalid blog id" });
      }

      const deleteRes = await db.query(
        `
      DELETE FROM blogs
      WHERE id = $1
      RETURNING id, product_id, title, slug
    `,
        [blogId],
      );

      if (!deleteRes.rows.length) {
        return res.status(404).json({ message: "Blog not found" });
      }

      const deletedBlog = deleteRes.rows[0];
      req.deleteAuditTarget = {
        target_table: "blogs",
        target_id: deletedBlog.id,
        target_name: deletedBlog.title || `News article ${deletedBlog.id}`,
        target_type: "news_article",
        target_snapshot: deletedBlog,
      };

      return res.json({
        message: "Blog deleted successfully",
        blog: deletedBlog,
      });
    } catch (err) {
      console.error("DELETE /api/admin/blogs/:id error:", err);
      return res.status(500).json({ message: "Failed to delete blog" });
    }
  },
);

app.get("/api/admin/news-articles/search", authenticate, async (req, res) => {
  try {
    const limit = Math.min(25, toPositiveInt(req.query.limit, 12));
    const query = String(req.query.query || req.query.q || "").trim();
    const selectedBlogIds = normalizePositiveIntegerList(
      Array.isArray(req.query.selected_ids)
        ? req.query.selected_ids
        : Array.isArray(req.query.selectedIds)
          ? req.query.selectedIds
          : String(req.query.selected_ids || req.query.selectedIds || "")
              .split(",")
              .map((value) => value.trim())
              .filter(Boolean),
    );

    const searchParams = [selectedBlogIds];
    let searchFilterSql = "";

    if (query) {
      searchParams.push(`%${query}%`);
      searchFilterSql = `
        AND (
          bl.title ILIKE $2
          OR bl.slug ILIKE $2
          OR COALESCE(primary_product.name, '') ILIKE $2
          OR COALESCE(primary_brand.name, '') ILIKE $2
        )
      `;
    }

    searchParams.push(limit);
    const limitIndex = searchParams.length;

    const searchRes = await db.query(
      `
      SELECT
        bl.id,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.status,
        bl.is_published,
        bl.author_name,
        bl.author_user_id,
        bl.category,
        bl.hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        bl.product_id AS primary_product_id,
        primary_product.name AS primary_product_name,
        primary_product.product_type AS primary_product_type,
        primary_brand.name AS primary_brand_name,
        COALESCE(link_counts.total_links, 0) AS linked_product_count,
        bl.id = ANY($1::int[]) AS is_linked
      FROM blogs bl
      LEFT JOIN products primary_product
        ON primary_product.id = bl.product_id
      LEFT JOIN brands primary_brand
        ON primary_brand.id = primary_product.brand_id
      LEFT JOIN (
        SELECT blog_id, COUNT(*)::int AS total_links
        FROM blog_product_links
        GROUP BY blog_id
      ) link_counts
        ON link_counts.blog_id = bl.id
      WHERE bl.status IN ('draft', 'published')
      ${searchFilterSql}
      ORDER BY
        CASE WHEN bl.id = ANY($1::int[]) THEN 0 ELSE 1 END,
        bl.updated_at DESC,
        bl.id DESC
      LIMIT $${limitIndex}
    `,
      searchParams,
    );

    return res.json({
      query,
      limit,
      rows: mapBlogLinkRows(searchRes.rows),
      selected_blog_ids: selectedBlogIds,
    });
  } catch (err) {
    console.error("GET /api/admin/news-articles/search error:", err);
    return res.status(500).json({ message: "Failed to search news articles" });
  }
});

app.get(
  "/api/admin/products/:productId/linked-news",
  authenticate,
  async (req, res) => {
    try {
      const productId = Number(req.params.productId);
      if (!Number.isInteger(productId) || productId <= 0) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      const limit = Math.min(25, toPositiveInt(req.query.limit, 12));
      const query = String(req.query.query || req.query.q || "").trim();

      const productRes = await db.query(
        `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name
      FROM products p
      LEFT JOIN brands b
        ON b.id = p.brand_id
      WHERE p.id = $1
      LIMIT 1
    `,
        [productId],
      );

      if (!productRes.rows.length) {
        return res.status(404).json({ message: "Product not found" });
      }

      const linkedRes = await db.query(
        `
      SELECT
        bl.id,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.status,
        bl.is_published,
        bl.author_name,
        bl.author_user_id,
        bl.category,
        bl.hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        bl.product_id AS primary_product_id,
        primary_product.name AS primary_product_name,
        primary_product.product_type AS primary_product_type,
        primary_brand.name AS primary_brand_name,
        link_counts.total_links AS linked_product_count,
        true AS is_linked
      FROM blog_product_links bpl
      INNER JOIN blogs bl
        ON bl.id = bpl.blog_id
      LEFT JOIN products primary_product
        ON primary_product.id = bl.product_id
      LEFT JOIN brands primary_brand
        ON primary_brand.id = primary_product.brand_id
      LEFT JOIN (
        SELECT blog_id, COUNT(*)::int AS total_links
        FROM blog_product_links
        GROUP BY blog_id
      ) link_counts
        ON link_counts.blog_id = bl.id
      WHERE bpl.product_id = $1
      ORDER BY bl.updated_at DESC, bl.id DESC
    `,
        [productId],
      );

      const searchParams = [productId];
      let searchFilterSql = "";
      if (query) {
        searchParams.push(`%${query}%`);
        searchFilterSql = `
        AND (
          bl.title ILIKE $2
          OR bl.slug ILIKE $2
          OR COALESCE(primary_product.name, '') ILIKE $2
          OR COALESCE(primary_brand.name, '') ILIKE $2
        )
      `;
      }
      searchParams.push(limit);
      const limitIndex = searchParams.length;

      const searchRes = await db.query(
        `
      SELECT
        bl.id,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.status,
        bl.is_published,
        bl.author_name,
        bl.author_user_id,
        bl.category,
        bl.hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        bl.product_id AS primary_product_id,
        primary_product.name AS primary_product_name,
        primary_product.product_type AS primary_product_type,
        primary_brand.name AS primary_brand_name,
        COALESCE(link_counts.total_links, 0) AS linked_product_count,
        EXISTS (
          SELECT 1
          FROM blog_product_links bpl_match
          WHERE bpl_match.blog_id = bl.id
            AND bpl_match.product_id = $1
        ) AS is_linked
      FROM blogs bl
      LEFT JOIN products primary_product
        ON primary_product.id = bl.product_id
      LEFT JOIN brands primary_brand
        ON primary_brand.id = primary_product.brand_id
      LEFT JOIN (
        SELECT blog_id, COUNT(*)::int AS total_links
        FROM blog_product_links
        GROUP BY blog_id
      ) link_counts
        ON link_counts.blog_id = bl.id
      WHERE bl.status IN ('draft', 'published')
      ${searchFilterSql}
      ORDER BY
        CASE
          WHEN EXISTS (
            SELECT 1
            FROM blog_product_links bpl_match
            WHERE bpl_match.blog_id = bl.id
              AND bpl_match.product_id = $1
          ) THEN 0
          ELSE 1
        END,
        bl.updated_at DESC,
        bl.id DESC
      LIMIT $${limitIndex}
    `,
        searchParams,
      );

      return res.json({
        product: productRes.rows[0],
        linked_articles: mapBlogLinkRows(linkedRes.rows),
        search_results: mapBlogLinkRows(searchRes.rows),
        query,
        limit,
      });
    } catch (err) {
      console.error(
        "GET /api/admin/products/:productId/linked-news error:",
        err,
      );
      return res.status(500).json({ message: "Failed to fetch linked news" });
    }
  },
);

app.put(
  "/api/admin/products/:productId/linked-news",
  authenticate,
  async (req, res) => {
    const connection = await db.connect();

    try {
      const productId = Number(req.params.productId);
      if (!Number.isInteger(productId) || productId <= 0) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      const requestedBlogIds = normalizePositiveIntegerList(
        Array.isArray(req.body?.blog_ids)
          ? req.body.blog_ids
          : Array.isArray(req.body?.blogIds)
            ? req.body.blogIds
            : [],
      );

      const productRes = await connection.query(
        `SELECT id FROM products WHERE id = $1 LIMIT 1`,
        [productId],
      );
      if (!productRes.rows.length) {
        return res.status(404).json({ message: "Product not found" });
      }

      if (requestedBlogIds.length) {
        const existingBlogsRes = await connection.query(
          `SELECT id FROM blogs WHERE id = ANY($1::int[])`,
          [requestedBlogIds],
        );
        const existingBlogIds = normalizePositiveIntegerList(
          (existingBlogsRes.rows || []).map((row) => row.id),
        );
        if (existingBlogIds.length !== requestedBlogIds.length) {
          return res.status(400).json({
            message: "One or more selected news articles were not found",
          });
        }
      }

      await connection.query("BEGIN");

      const currentLinksRes = await connection.query(
        `
      SELECT blog_id
      FROM blog_product_links
      WHERE product_id = $1
      ORDER BY blog_id ASC
    `,
        [productId],
      );
      const currentBlogIds = normalizePositiveIntegerList(
        (currentLinksRes.rows || []).map((row) => row.blog_id),
      );

      const blogIdsToRemove = currentBlogIds.filter(
        (blogId) => !requestedBlogIds.includes(blogId),
      );
      const blogIdsToAdd = requestedBlogIds.filter(
        (blogId) => !currentBlogIds.includes(blogId),
      );

      for (const blogId of blogIdsToAdd) {
        const fallbackPrimaryProductId = await readBlogPrimaryProductId(
          connection,
          blogId,
        );
        const existingProductIds = await readBlogLinkedProductIds(
          connection,
          blogId,
          fallbackPrimaryProductId,
        );
        await syncBlogProductLinks(connection, blogId, [
          ...existingProductIds,
          productId,
        ]);
      }

      for (const blogId of blogIdsToRemove) {
        const fallbackPrimaryProductId = await readBlogPrimaryProductId(
          connection,
          blogId,
        );
        const existingProductIds = await readBlogLinkedProductIds(
          connection,
          blogId,
          fallbackPrimaryProductId,
        );
        await syncBlogProductLinks(
          connection,
          blogId,
          existingProductIds.filter(
            (linkedProductId) => linkedProductId !== productId,
          ),
        );
      }

      await connection.query("COMMIT");

      const refreshedLinks = await db.query(
        `
      SELECT
        bl.id,
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.status,
        bl.is_published,
        bl.author_name,
        bl.author_user_id,
        bl.category,
        bl.hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        bl.product_id AS primary_product_id,
        primary_product.name AS primary_product_name,
        primary_product.product_type AS primary_product_type,
        primary_brand.name AS primary_brand_name,
        link_counts.total_links AS linked_product_count,
        true AS is_linked
      FROM blog_product_links bpl
      INNER JOIN blogs bl
        ON bl.id = bpl.blog_id
      LEFT JOIN products primary_product
        ON primary_product.id = bl.product_id
      LEFT JOIN brands primary_brand
        ON primary_brand.id = primary_product.brand_id
      LEFT JOIN (
        SELECT blog_id, COUNT(*)::int AS total_links
        FROM blog_product_links
        GROUP BY blog_id
      ) link_counts
        ON link_counts.blog_id = bl.id
      WHERE bpl.product_id = $1
      ORDER BY bl.updated_at DESC, bl.id DESC
    `,
        [productId],
      );

      return res.json({
        message: "Linked news updated successfully",
        linked_articles: mapBlogLinkRows(refreshedLinks.rows),
        blog_ids: requestedBlogIds,
      });
    } catch (err) {
      try {
        await connection.query("ROLLBACK");
      } catch {}
      console.error(
        "PUT /api/admin/products/:productId/linked-news error:",
        err,
      );
      return res.status(500).json({ message: "Failed to update linked news" });
    } finally {
      connection.release();
    }
  },
);

app.get("/api/public/blogs", async (req, res) => {
  try {
    const limit = Math.min(50, toPositiveInt(req.query.limit, 12));
    const requestedProductId = Number(
      req.query.productId ?? req.query.product_id,
    );
    const hasProductFilter =
      Number.isInteger(requestedProductId) && requestedProductId > 0;
    const productType = String(
      req.query.productType || req.query.product_type || "",
    )
      .trim()
      .toLowerCase();
    const whereClauses = ["bl.is_published = true"];
    const queryParams = [];

    if (hasProductFilter) {
      queryParams.push(requestedProductId);
      const index = queryParams.length;
      whereClauses.push(`
        (
          bl.product_id = $${index}
          OR EXISTS (
            SELECT 1
            FROM blog_product_links bpl_filter
            WHERE bpl_filter.blog_id = bl.id
              AND bpl_filter.product_id = $${index}
          )
        )
      `);
    }

    if (productType) {
      queryParams.push(productType);
      const index = queryParams.length;
      whereClauses.push(`
        (
          p.product_type = $${index}
          OR EXISTS (
            SELECT 1
            FROM blog_product_links bpl_type
            INNER JOIN products p_type
              ON p_type.id = bpl_type.product_id
            WHERE bpl_type.blog_id = bl.id
              AND p_type.product_type = $${index}
          )
        )
      `);
    }

    queryParams.push(limit);
    const limitParamIndex = queryParams.length;

    const result = await db.query(
      `
      SELECT
        bl.id,
        bl.product_id,
        bl.slug,
        bl.title,
        bl.excerpt,
        bl.is_published,
        COALESCE(
          NULLIF(BTRIM(bl.author_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', public_blog_author.first_name, public_blog_author.last_name)), ''),
          NULLIF(BTRIM(public_blog_author.user_name), ''),
          NULLIF(BTRIM(public_blog_author.email), '')
        ) AS author_name,
        COALESCE(bl.author_user_id, bl.updated_by, bl.created_by) AS author_user_id,
        public_blog_author.role AS author_role,
        bl.category,
        bl.content_rendered,
        bl.meta_title,
        bl.meta_description,
        COALESCE(
          bl.hero_image,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = bl.product_id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          )
        ) AS hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        COALESCE(NULLIF(BTRIM(bl.brand_name), ''), b.name) AS brand_name,
        COALESCE(
          linked.product_ids,
          CASE
            WHEN bl.product_id IS NOT NULL THEN ARRAY[bl.product_id]::int[]
            ELSE ARRAY[]::int[]
          END
        ) AS product_ids,
        COALESCE(
          linked.products,
          CASE
            WHEN bl.product_id IS NOT NULL THEN json_build_array(
              json_build_object(
                'product_id', bl.product_id,
                'id', bl.product_id,
                'name', p.name,
                'product_type', p.product_type,
                'brand_name', b.name
              )
            )
            ELSE '[]'::json
          END
        ) AS products
      FROM blogs bl
      LEFT JOIN "user" public_blog_author
        ON public_blog_author.id = COALESCE(bl.author_user_id, bl.updated_by, bl.created_by)
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN LATERAL (
        SELECT
          array_agg(linked_product.id ORDER BY bpl.position ASC, linked_product.id ASC) AS product_ids,
          json_agg(
            json_build_object(
              'product_id', linked_product.id,
              'id', linked_product.id,
              'name', linked_product.name,
              'product_type', linked_product.product_type,
              'brand_name', linked_brand.name
            )
            ORDER BY bpl.position ASC, linked_product.id ASC
          ) AS products
        FROM blog_product_links bpl
        INNER JOIN products linked_product
          ON linked_product.id = bpl.product_id
        LEFT JOIN brands linked_brand
          ON linked_brand.id = linked_product.brand_id
        WHERE bpl.blog_id = bl.id
      ) linked ON true
      WHERE ${whereClauses.join("\n        AND ")}
      ORDER BY bl.published_at DESC NULLS LAST, bl.updated_at DESC
      LIMIT $${limitParamIndex}
    `,
      queryParams,
    );

    return res.json({
      limit,
      product_id: hasProductFilter ? requestedProductId : null,
      product_type: productType || null,
      blogs: result.rows || [],
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
        bl.title,
        bl.slug,
        bl.excerpt,
        bl.is_published,
        COALESCE(
          NULLIF(BTRIM(bl.author_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', public_blog_author.first_name, public_blog_author.last_name)), ''),
          NULLIF(BTRIM(public_blog_author.user_name), ''),
          NULLIF(BTRIM(public_blog_author.email), '')
        ) AS author_name,
        COALESCE(bl.author_user_id, bl.updated_by, bl.created_by) AS author_user_id,
        public_blog_author.role AS author_role,
        bl.category,
        bl.content_rendered,
        bl.meta_title,
        bl.meta_description,
        COALESCE(
          bl.hero_image,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = bl.product_id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          )
        ) AS hero_image,
        bl.hero_image_source,
        bl.hero_image_alt,
        bl.hero_image_caption,
        bl.tags,
        bl.featured,
        bl.trending,
        bl.pinned,
        bl.published_at,
        bl.updated_at,
        p.name AS product_name,
        p.product_type,
        COALESCE(NULLIF(BTRIM(bl.brand_name), ''), b.name) AS brand_name,
        COALESCE(
          linked.product_ids,
          CASE
            WHEN bl.product_id IS NOT NULL THEN ARRAY[bl.product_id]::int[]
            ELSE ARRAY[]::int[]
          END
        ) AS product_ids,
        COALESCE(
          linked.products,
          CASE
            WHEN bl.product_id IS NOT NULL THEN json_build_array(
              json_build_object(
                'product_id', bl.product_id,
                'id', bl.product_id,
                'name', p.name,
                'product_type', p.product_type,
                'brand_name', b.name
              )
            )
            ELSE '[]'::json
          END
        ) AS products
      FROM blogs bl
      LEFT JOIN "user" public_blog_author
        ON public_blog_author.id = COALESCE(bl.author_user_id, bl.updated_by, bl.created_by)
      LEFT JOIN products p
        ON p.id = bl.product_id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN LATERAL (
        SELECT
          array_agg(linked_product.id ORDER BY bpl.position ASC, linked_product.id ASC) AS product_ids,
          json_agg(
            json_build_object(
              'product_id', linked_product.id,
              'id', linked_product.id,
              'name', linked_product.name,
              'product_type', linked_product.product_type,
              'brand_name', linked_brand.name
            )
            ORDER BY bpl.position ASC, linked_product.id ASC
          ) AS products
        FROM blog_product_links bpl
        INNER JOIN products linked_product
          ON linked_product.id = bpl.product_id
        LEFT JOIN brands linked_brand
          ON linked_brand.id = linked_product.brand_id
        WHERE bpl.blog_id = bl.id
      ) linked ON true
      WHERE bl.slug = $1
        AND bl.is_published = true
      LIMIT $2
    `,
      [slug, 1],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Blog not found" });
    }

    return res.json({ blog: result.rows[0] });
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

app.delete(
  "/api/admin/customers/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
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
        launch_status_override,
        images, colors, build_design, display, performance,
        camera, battery, connectivity, network,
        ports, audio, multimedia, sensors
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6,
        $7,$8,$9,$10,$11,
        $12,$13,$14,$15,$16,$17,$18,$19
      )
      RETURNING id
      `,
      [
        productId,
        smartphone.category || smartphone.segment || null,
        smartphone.brand || null,
        smartphone.model || null,
        smartphone.launch_date || null,
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
    scheduleSmartphoneCompetitorRefresh(`smartphone_created:${productId}`);

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
        s.category, s.model, s.launch_date, s.launch_status_override,
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

    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
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
        applySmartphoneAvailabilityDetails(
          item,
          availabilityForecast,
          todayIndia,
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

    const publicSmartphones = sortedSmartphones.map((item) =>
      toPublicSmartphoneResponse(item),
    );
    res.json({ smartphones: publicSmartphones });
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
        s.category, s.model, s.launch_date, s.launch_status_override,
        s.colors, s.build_design, s.display, s.performance,
        s.camera, s.battery, s.connectivity, s.network,
        s.ports, s.audio, s.multimedia, s.sensors, s.created_at, pub.is_published

      ORDER BY p.id DESC;
    `);

    const todayIndia = getIndiaDateOnly();
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
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
        applySmartphoneAvailabilityDetails(
          item,
          availabilityForecast,
          todayIndia,
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
    sanitized.id = productId;
    sanitized.product_id = productId;
    sanitized.sale_start_date = getEarliestSaleStartDateFromVariants(variants);
    sanitized.name = productName;
    sanitized.brand_logo = productBrandLogo || null;
    sanitized.brand_website = productBrandWebsite || null;
    sanitized.launch_date = smartphone.launch_date || null;
    sanitized.created_at = smartphone.created_at || null;
    sanitized.price = resolveEffectiveSmartphonePrice(
      variants,
      sanitized.price,
    );
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
    applySmartphoneAvailabilityDetails(
      sanitized,
      availabilityForecast,
      todayIndia,
    );
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
    );
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

const resolveExistingBrandId = async (client, brandIdInput, brandNameInput) => {
  const brandId = Number(brandIdInput);
  if (Number.isInteger(brandId) && brandId > 0) {
    const brandRes = await client.query(
      `SELECT id
       FROM brands
       WHERE id = $1
       LIMIT 1`,
      [brandId],
    );
    if (brandRes.rows[0]) return brandRes.rows[0].id;
  }

  return resolveBrandIdByName(client, brandNameInput);
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

    const basicInfo = toPlainObject(payload.basic_info_json);
    const product = toPlainObject(payload.product);
    const model = normalizeNullableText(
      payload.model || basicInfo.model_number || basicInfo.model,
    );
    if (!model) {
      return res.status(400).json({ message: "model is required" });
    }

    const category = normalizeNullableText(payload.category);
    const publish = hasOwn(payload, "publish")
      ? Boolean(payload.publish)
      : false;

    const brandName = normalizeNullableText(
      payload.brand_name ||
        payload.brand ||
        product.brand_name ||
        product.brand ||
        basicInfo.brand_name ||
        basicInfo.brand,
    );
    const brandId = await resolveExistingBrandId(
      client,
      payload.brand_id,
      brandName,
    );
    if (!brandId) {
      return res.status(400).json({
        message:
          "brand is required and must reference an existing brand using brand_id or brand_name",
      });
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
      ORDER BY
        COALESCE(ds.hook_score, 0) DESC,
        COALESCE(ds.buyer_intent, 0) DESC,
        COALESCE(ds.trend_velocity, 0) DESC,
        COALESCE(ds.freshness, 0) DESC,
        p.id DESC
    `);

    const tvs = applySpecScoreToRows(
      "tv",
      (result.rows || []).map((row) => stripScoreRecursively(row || {})),
      profileConfig.profiles,
    ).map((row) => toPublicTvResponse(row || {}));
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
  req.body = mergeSmartphoneUpdateBody(req.body);
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
    // Accept several name aliases: `name`, `product_name`, `productName`, normalized variants,
    // and `product.name` (create-style payload) so PUT can accept the same format as POST.
    const name =
      n.name ||
      n.productname ||
      req.body.name ||
      req.body.product_name ||
      req.body.productName ||
      // support create-style nested product object: { product: { name: '...' } }
      req.body.product?.name ||
      req.body.product?.product_name ||
      // also accept nested smartphone.product.name when clients wrap product inside `smartphone`
      req.body.smartphone?.product?.name ||
      req.body.smartphone?.product_name ||
      // fallback to stringified productName if present
      (req.body.productName ? req.body.productName.toString() : undefined);
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
        launch_status_override=$5,
        images=$6, colors=$7, build_design=$8, display=$9, performance=$10,
        camera=$11, battery=$12, connectivity=$13, network=$14, ports=$15,
        audio=$16, multimedia=$17, sensors=$18
      WHERE id=$19
      RETURNING *;
    `;

    const phoneRes = await client.query(updatePhoneSQL, [
      req.body.category || req.body.segment || null,
      req.body.brand || null,
      req.body.model || null,
      parseDateForImport(req.body.launch_date),
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
    scheduleSmartphoneCompetitorRefresh(`smartphone_updated:${productId}`);
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
    const b = mergeSmartphoneUpdateBody(req.body || {});

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
        launch_status_override=$5,
        images=$6, colors=$7, build_design=$8, display=$9, performance=$10,
        camera=$11, battery=$12, connectivity=$13, network=$14, ports=$15,
        audio=$16, multimedia=$17, sensors=$18
      WHERE id=$19
      RETURNING *;
    `;

    const phoneRes = await client.query(updateSQL, [
      category,
      brand,
      model,
      parseDateForImport(launch_date),
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
    scheduleSmartphoneCompetitorRefresh(`smartphone_updated:${productId}`);
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
app.delete(
  "/api/smartphone/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
      const productMetaRes = await client.query(
        `SELECT
         p.id,
         p.name,
         p.product_type,
         b.name AS brand_name,
         COALESCE(pub.is_published, false) AS is_published
       FROM products p
       LEFT JOIN brands b
         ON b.id = p.brand_id
       LEFT JOIN product_publish pub
         ON pub.product_id = p.id
       WHERE p.id = $1
       LIMIT 1`,
        [productId],
      );
      const productMeta = productMetaRes.rows[0] || {};
      req.deleteAuditTarget = {
        target_table: "products",
        target_id: productId,
        target_name: productMeta.name || `Smartphone ${productId}`,
        target_type: productMeta.product_type || "smartphone",
        target_snapshot: productMeta,
      };

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
  },
);

// Delete laptop
app.delete(
  "/api/laptop/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
      const productMetaRes = await client.query(
        `SELECT
         p.id,
         p.name,
         p.product_type,
         b.name AS brand_name,
         COALESCE(pub.is_published, false) AS is_published
       FROM products p
       LEFT JOIN brands b
         ON b.id = p.brand_id
       LEFT JOIN product_publish pub
         ON pub.product_id = p.id
       WHERE p.id = $1
       LIMIT 1`,
        [productId],
      );
      const productMeta = productMetaRes.rows[0] || {};
      req.deleteAuditTarget = {
        target_table: "products",
        target_id: productId,
        target_name: productMeta.name || `Laptop ${productId}`,
        target_type: productMeta.product_type || "laptop",
        target_snapshot: productMeta,
      };

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
  },
);

// Delete TV
app.delete(
  "/api/tvs/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
      const productMetaRes = await client.query(
        `SELECT
         p.id,
         p.name,
         p.product_type,
         b.name AS brand_name,
         COALESCE(pub.is_published, false) AS is_published
       FROM products p
       LEFT JOIN brands b
         ON b.id = p.brand_id
       LEFT JOIN product_publish pub
         ON pub.product_id = p.id
       WHERE p.id = $1
       LIMIT 1`,
        [productId],
      );
      const productMeta = productMetaRes.rows[0] || {};
      req.deleteAuditTarget = {
        target_table: "products",
        target_id: productId,
        target_name: productMeta.name || `TV ${productId}`,
        target_type: productMeta.product_type || "tv",
        target_snapshot: productMeta,
      };

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
  },
);

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
    ).map((row) => stripPublicSpecScoreDecorations(row || {}));
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

    const scoredTv = stripPublicSpecScoreDecorations(
      applySpecScoreToRow(
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
      ),
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
app.delete(
  "/api/ram-storage-config/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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
app.delete(
  "/api/categories/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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
app.delete(
  "/api/online-stores/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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
app.delete(
  "/api/specs/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

// Delete a variant by id (will cascade-delete store prices via FK)
app.delete(
  "/api/variant/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

// Delete a store price entry by id
app.delete(
  "/api/storeprice/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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

const pushFcmUnavailablePayload = {
  configured: false,
  message: "Push notifications are not configured on this app yet.",
};

app.get("/api/public/push/fcm/status", (_req, res) => {
  return res.json(pushFcmUnavailablePayload);
});

app.post("/api/public/push/fcm/register", (_req, res) => {
  return res.status(503).json(pushFcmUnavailablePayload);
});

app.post("/api/public/push/fcm/unregister", (_req, res) => {
  return res.json({
    ...pushFcmUnavailablePayload,
    ok: true,
  });
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

    const product = await db.query(
      "SELECT id, product_type FROM products WHERE id = $1",
      [productId],
    );

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

    if (product.rows[0]?.product_type === "smartphone") {
      scheduleSmartphoneCompetitorRefresh(
        `smartphone_${is_published ? "published" : "unpublished"}:${productId}`,
      );
    }

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

app.get("/api/brands/:id/products", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ message: "Invalid brand id" });
    }

    const brandRes = await db.query(
      `
      SELECT
        b.id,
        b.name,
        b.logo,
        b.status,
        MAX(to_jsonb(b)->>'website') AS website,
        b.description,
        b.category,
        b.created_at,
        COUNT(DISTINCT p.id)::int AS product_count,
        COUNT(DISTINCT p.id) FILTER (WHERE pp.is_published = true)::int AS published_products
      FROM brands b
      LEFT JOIN products p
        ON p.brand_id = b.id
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
      WHERE b.id = $1
      GROUP BY
        b.id,
        b.name,
        b.logo,
        b.status,
        b.description,
        b.category,
        b.created_at
      LIMIT 1
      `,
      [id],
    );

    if (!brandRes.rows.length) {
      return res.status(404).json({ message: "Brand not found" });
    }

    const productsRes = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        p.created_at AS product_created_at,
        COALESCE(pp.is_published, false) AS is_published,
        COALESCE(
          NULLIF(s.category, ''),
          NULLIF(t.category, ''),
          CASE
            WHEN p.product_type = 'smartphone' THEN 'Smartphones'
            WHEN p.product_type = 'laptop' THEN 'Laptops'
            WHEN p.product_type = 'tv' THEN 'TVs'
            WHEN p.product_type = 'networking' THEN 'Networking'
            WHEN p.product_type = 'accessories' THEN 'Accessories'
            ELSE initcap(replace(p.product_type, '_', ' '))
          END
        ) AS category,
        COALESCE(
          NULLIF(s.model, ''),
          NULLIF(t.model, ''),
          NULLIF(n.model_number, ''),
          NULLIF(l.meta->>'model_name', ''),
          NULLIF(l.meta->>'model', ''),
          NULLIF(l.meta->>'series', ''),
          p.name
        ) AS model,
        CASE
          WHEN s.launch_date IS NOT NULL
            THEN EXTRACT(YEAR FROM s.launch_date)::int
          WHEN n.release_year IS NOT NULL
            THEN n.release_year
          WHEN COALESCE(
            l.meta->>'release_year',
            l.meta->>'launch_year',
            l.meta->>'year',
            ''
          ) ~ '^[0-9]{4}$'
            THEN COALESCE(
              l.meta->>'release_year',
              l.meta->>'launch_year',
              l.meta->>'year'
            )::int
          WHEN COALESCE(
            t.basic_info_json->>'release_year',
            t.basic_info_json->>'launch_year',
            t.basic_info_json->>'year',
            ''
          ) ~ '^[0-9]{4}$'
            THEN COALESCE(
              t.basic_info_json->>'release_year',
              t.basic_info_json->>'launch_year',
              t.basic_info_json->>'year'
            )::int
          ELSE NULL
        END AS release_year,
        COALESCE(
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = p.id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          ),
          CASE
            WHEN jsonb_typeof(t.images_json) = 'array'
            THEN t.images_json->>0
            ELSE NULL
          END
        ) AS image_url
      FROM products p
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
      LEFT JOIN smartphones s
        ON s.product_id = p.id
      LEFT JOIN tvs t
        ON t.product_id = p.id
      LEFT JOIN laptop l
        ON l.product_id = p.id
      LEFT JOIN networking n
        ON n.product_id = p.id
      WHERE p.brand_id = $1
      ORDER BY
        COALESCE(pp.is_published, false) DESC,
        COALESCE(s.launch_date, t.created_at, l.created_at, p.created_at) DESC,
        p.id DESC
      `,
      [id],
    );

    return res.json({
      brand: brandRes.rows[0],
      products: productsRes.rows || [],
    });
  } catch (err) {
    console.error("GET /api/brands/:id/products error:", err);
    return res.status(500).json({ error: err.message });
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

app.delete(
  "/api/brands/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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

app.delete(
  "/api/admin/banners/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
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
  },
);

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

app.get("/api/reports/launch-timing", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const limitRaw = Number(req.query?.limit ?? 1000);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(5000, Math.max(1, Math.floor(limitRaw)))
      : 1000;

    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
    const result = await db.query(
      `
      WITH launch_rows AS (
        SELECT
          p.id AS product_id,
          p.name AS product_name,
          p.product_type,
          b.name AS brand_name,
          b.logo AS brand_logo,
          COALESCE(
            NULLIF(s.category, ''),
            NULLIF(t.category, ''),
            NULLIF(l.meta->>'category', ''),
            'Uncategorized'
          ) AS category,
          COALESCE(
            s.launch_date::date,
            l.created_at::date,
            t.created_at::date,
            p.created_at::date
          ) AS launch_date,
          (
            SELECT MIN(sp.sale_start_date)
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND sp.sale_start_date IS NOT NULL
          ) AS sale_start_date,
          (
            SELECT MIN(sp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND sp.price IS NOT NULL
          ) AS best_price,
          (
            SELECT pi.image_url
            FROM product_images pi
            WHERE pi.product_id = p.id
            ORDER BY pi.position ASC NULLS LAST, pi.id ASC
            LIMIT 1
          ) AS image_url,
          COALESCE(ts.trending_score, 0) AS trending_score,
          COALESCE(ts.views_7d, 0) AS views_7d,
          COALESCE(ts.compares_7d, 0) AS compares_7d,
          COALESCE(ts.views_prev_7d, 0) AS views_prev_7d,
          COALESCE(ts.velocity, 0) AS velocity,
          COALESCE(ts.manual_boost, false) AS manual_boost,
          COALESCE(ts.manual_priority, 0) AS manual_priority,
          ts.manual_badge,
          ts.calculated_at AS trending_calculated_at,
          COALESCE(ds.buyer_intent, 0) AS buyer_intent,
          COALESCE(ds.hook_score, 0) AS hook_score,
          COALESCE(ds.trend_velocity, 0) AS trend_velocity,
          COALESCE(ds.freshness, 0) AS freshness,
          ds.calculated_at AS dynamic_calculated_at
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN brands b
          ON b.id = p.brand_id
        LEFT JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN laptop l
          ON l.product_id = p.id
        LEFT JOIN tvs t
          ON t.product_id = p.id
        LEFT JOIN product_trending_score ts
          ON ts.product_id = p.id
        LEFT JOIN product_dynamic_score ds
          ON ds.product_id = p.id
        WHERE p.product_type IN ('smartphone', 'laptop', 'tv')
      )
      SELECT
        *,
        CASE
          WHEN launch_date IS NOT NULL AND sale_start_date IS NOT NULL
            THEN (sale_start_date::date - launch_date::date)
          ELSE NULL
        END AS sale_gap_days
      FROM launch_rows
      ORDER BY launch_date DESC NULLS LAST, product_id DESC
      LIMIT $1
      `,
      [limit],
    );

    const todayIndia = getIndiaDateOnly();
    const devices = (result.rows || []).map((row) => {
      const base = {
        id: Number(row.product_id),
        product_id: Number(row.product_id),
        product_type: row.product_type || "smartphone",
        brand_name: row.brand_name || null,
        brand_logo_url: row.brand_logo || null,
        brand_logo: row.brand_logo || null,
        category: row.category || "Uncategorized",
        name: row.product_name || "Device",
        product_name: row.product_name || "Device",
        launch_date: row.launch_date
          ? String(row.launch_date).slice(0, 10)
          : null,
        sale_start_date: row.sale_start_date
          ? String(row.sale_start_date).slice(0, 10)
          : null,
        sale_gap_days:
          row.sale_gap_days !== null && row.sale_gap_days !== undefined
            ? Number(row.sale_gap_days)
            : null,
        image_url: row.image_url || null,
        best_price: row.best_price !== null ? Number(row.best_price) : null,
        trending_score: Number(row.trending_score) || 0,
        views_7d: Number(row.views_7d) || 0,
        compares_7d: Number(row.compares_7d) || 0,
        views_prev_7d: Number(row.views_prev_7d) || 0,
        velocity: Number(row.velocity) || 0,
        manual_boost: Boolean(row.manual_boost),
        manual_priority: Number(row.manual_priority) || 0,
        manual_badge: row.manual_badge || null,
        trending_calculated_at: row.trending_calculated_at || null,
        buyer_intent: Number(row.buyer_intent) || 0,
        hook_score: Number(row.hook_score) || 0,
        trend_velocity: Number(row.trend_velocity) || 0,
        freshness: Number(row.freshness) || 0,
        dynamic_calculated_at: row.dynamic_calculated_at || null,
      };
      return applySmartphoneAvailabilityDetails(
        base,
        availabilityForecast,
        todayIndia,
      );
    });

    return res.json({
      generated_at: new Date().toISOString(),
      devices,
      data: devices,
    });
  } catch (err) {
    console.error("GET /api/reports/launch-timing error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load launch timing report" });
  }
});

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
        COALESCE(ct.compares_total, 0) AS compares_total,
        COALESCE(ds.hook_score, 0) AS hook_score
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
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      ${where}
      ORDER BY
        ts.manual_priority DESC,
        ts.manual_boost DESC,
        COALESCE(ds.hook_score, 0) DESC,
        ts.trending_score DESC,
        ts.calculated_at DESC,
        p.id DESC
      LIMIT $${params.length}
      `,
      params,
    );

    const results = (result.rows || []).map((row, index) => {
      const autoBadge = resolveAutomaticTrendBadge({
        rank: index + 1,
        hookScore: row.hook_score,
        trendScore: row.trending_score,
      });
      return {
        ...row,
        auto_badge: autoBadge,
        display_badge: resolvePublicTrendBadge({
          manualBoost: row.manual_boost,
          manualBadge: row.manual_badge,
          rank: index + 1,
          hookScore: row.hook_score,
          trendScore: row.trending_score,
        }),
      };
    });

    return res.json({
      success: true,
      type: typeRaw || "all",
      period: "7d",
      updated_at: results?.[0]?.updated_at || null,
      results,
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
    const manualBadge = normalizeManualTrendBadge(manualBadgeRaw);

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

app.get("/api/admin/spec-score-algorithms", authenticate, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const profileConfig = await readDeviceFieldProfilesConfig();
    return res.json(buildSpecScoreAlgorithmResponse(profileConfig));
  } catch (err) {
    console.error("GET /api/admin/spec-score-algorithms error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load spec score algorithms" });
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

// Popular feature clicks (public) - aggregated per day
app.post("/api/public/feature-click", async (req, res) => {
  try {
    const b = req.body || {};
    const normalize = (v) =>
      String(v || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-");

    const deviceType = normalize(b.device_type ?? b.deviceType ?? "");
    const featureId = normalize(b.feature_id ?? b.featureId ?? b.id ?? "");

    const isSafeId = (s) => /^[a-z0-9][a-z0-9-]{0,63}$/.test(String(s));

    if (!deviceType || !featureId) {
      return res
        .status(400)
        .json({ message: "device_type and feature_id are required" });
    }
    if (!isSafeId(deviceType) || !isSafeId(featureId)) {
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

// Popular feature ordering (public) - last N days
app.get("/api/public/popular-features", async (req, res) => {
  try {
    const q = req.query || {};
    const normalize = (v) =>
      String(v || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-");

    const deviceType = normalize(q.deviceType ?? q.device_type ?? "smartphone");
    const isSafeId = (s) => /^[a-z0-9][a-z0-9-]{0,63}$/.test(String(s));
    if (!isSafeId(deviceType)) {
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
        COALESCE(ds.hook_score, 0) AS hook_score,
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
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      WHERE p.product_type = $1
      ORDER BY
        ts.manual_priority DESC,
        ts.manual_boost DESC,
        COALESCE(ds.hook_score, 0) DESC,
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

    const rows = result.rows || [];
    const updatedAt = rows?.[0]?.updated_at || null;

    const trending = rows.map((r, index) => {
      const manualBoost = Boolean(r.manual_boost);
      const badge = resolvePublicTrendBadge({
        manualBoost,
        manualBadge: r.manual_badge,
        rank: index + 1,
        hookScore: r.hook_score,
        trendScore: r.trending_score,
      });

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
        MAX(ds.hook_score) AS hook_score,
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
      LEFT JOIN product_dynamic_score ds ON ds.product_id = p.id
      WHERE p.product_type = 'smartphone'
      GROUP BY
        p.id,
        p.name,
        b.name,
        b.logo,
        s.model,
        s.launch_date,
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
        COALESCE(MAX(ds.hook_score), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC
      LIMIT $1;
      `,
      [limit],
    );

    const rows = result.rows || [];
    const trending = applySpecScoreToRows(
      "smartphone",
      rows.map((row, index) => {
        const manualBoost = Boolean(Number(row?.manual_boost ?? 0));
        const trendScore =
          Number.isFinite(Number(row?.trending_score)) &&
          row?.trending_score !== null
            ? Number(row.trending_score)
            : null;

        return {
          id: row.product_id,
          product_id: row.product_id,
          name: row.name,
          brand: row.brand || null,
          brand_name: row.brand || null,
          brand_logo: row.brand_logo || null,
          brand_website: row.brand_website || null,
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
          trend_score: trendScore,
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
          trend_manual_boost: manualBoost,
          trend_manual_priority: Number.isFinite(Number(row?.manual_priority))
            ? Number(row.manual_priority)
            : 0,
          trend_manual_badge: row?.manual_badge || null,
          trend_badge: resolvePublicTrendBadge({
            manualBoost,
            manualBadge: row?.manual_badge,
            rank: index + 1,
            hookScore: row?.hook_score,
            trendScore,
          }),
          trend_calculated_at: row?.trending_calculated_at ?? null,
        };
      }),
      profileConfig.profiles,
    );

    const todayIndia = getIndiaDateOnly();
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
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
      applySmartphoneAvailabilityDetails(
        item,
        availabilityForecast,
        todayIndia,
      );
      const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
      const saleStage = resolveSmartphoneSaleStage(item, todayIndia);
      item.launch_status = launchStage;
      item.launchStatus = launchStage;
      item.sale_status = saleStage;
      item.saleStatus = saleStage;
      applySmartphoneLaunchPolicy(item, launchStage);
    }

    const publicTrending = trending.map((item) =>
      toPublicSmartphoneResponse(item),
    );

    return res.json({
      success: true,
      period: "7d",
      updated_at:
        (rows || []).find((r) => r?.trending_calculated_at)
          ?.trending_calculated_at ?? null,
      trending: publicTrending,
      smartphones: publicTrending,
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

app.get("/api/public/upcoming/smartphones", async (req, res) => {
  try {
    const profileConfig = await readDeviceFieldProfilesConfig();
    const limitRaw = Number(req.query?.limit ?? 40);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(80, Math.max(1, Math.floor(limitRaw)))
      : 40;

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
          SELECT MIN(v.base_price)
          FROM product_variants v
          WHERE v.product_id = p.id AND v.base_price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN smartphones s ON s.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'smartphone'
      ORDER BY COALESCE(s.launch_date, p.created_at) DESC, p.id DESC
      LIMIT 120;
    `);

    const items = applySpecScoreToRows(
      "smartphone",
      (result.rows || []).map((row) => ({
        ...row,
        images: row?.image ? [row.image] : [],
        variants: [],
      })),
      profileConfig.profiles,
    );

    const todayIndia = getIndiaDateOnly();
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
    for (const item of items) {
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
      applySmartphoneAvailabilityDetails(
        item,
        availabilityForecast,
        todayIndia,
      );
      const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
      item.launch_status = launchStage;
      item.launchStatus = launchStage;
      const saleStage = resolveSmartphoneSaleStage(item, todayIndia);
      item.sale_status = saleStage;
      item.saleStatus = saleStage;
      applySmartphoneLaunchPolicy(item, launchStage);
    }

    const upcoming = items
      .filter((item) => isSmartphoneUpcomingFeedItem(item, todayIndia))
      .sort((left, right) => {
        const leftDate =
          getSmartphoneFeedStartDate(left) || left.launch_date || "";
        const rightDate =
          getSmartphoneFeedStartDate(right) || right.launch_date || "";
        if (leftDate && rightDate && leftDate !== rightDate) {
          return String(leftDate).localeCompare(String(rightDate));
        }
        if (leftDate) return -1;
        if (rightDate) return 1;
        return Number(right.product_id || 0) - Number(left.product_id || 0);
      })
      .slice(0, limit)
      .map((item) => toPublicSmartphoneResponse(item));

    return res.json({ upcoming, smartphones: upcoming });
  } catch (err) {
    console.error("GET /api/public/upcoming/smartphones error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/public/smartphones/highlights", async (req, res) => {
  try {
    const limitRaw = Number(req.query?.limit ?? 5);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
      : 5;

    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        b.name AS brand_name,
        b.logo AS brand_logo,
        s.model,
        s.launch_date,
        s.launch_status_override,
        (
          SELECT MIN(sp.sale_start_date)
          FROM product_variants v
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = v.id
          WHERE v.product_id = p.id
            AND sp.sale_start_date IS NOT NULL
        ) AS sale_start_date,
        (
          SELECT MIN(sp.price)::numeric
          FROM product_variants v
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = v.id
          WHERE v.product_id = p.id
            AND sp.price IS NOT NULL
        ) AS best_price,
        COALESCE(
          (
            SELECT json_agg(
              jsonb_build_object(
                'id', v.id,
                'variant_id', v.id,
                'variant_key', v.variant_key,
                'ram', v.attributes->>'ram',
                'storage', v.attributes->>'storage',
                'base_price', v.base_price,
                'store_prices', (
                  SELECT COALESCE(
                    json_agg(to_jsonb(sp) ORDER BY sp.price ASC NULLS LAST, sp.id ASC),
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
        ) AS variants,
        COALESCE(MAX(ds.hook_score), 0) AS hook_score,
        COALESCE(MAX(ds.buyer_intent), 0) AS buyer_intent,
        COALESCE(MAX(ds.trend_velocity), 0) AS trend_velocity,
        COALESCE(MAX(ds.freshness), 0) AS freshness,
        EXISTS(
          SELECT 1
          FROM product_variants v
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = v.id
          WHERE v.product_id = p.id
            AND (
              COALESCE(sp.price, 0) > 0
              OR NULLIF(BTRIM(COALESCE(sp.url, '')), '') IS NOT NULL
            )
            AND (
              sp.sale_start_date IS NULL
              OR sp.sale_start_date <= CURRENT_DATE
            )
        ) AS has_purchase_signal
      FROM products p
      INNER JOIN smartphones s
        ON s.product_id = p.id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      WHERE p.product_type = 'smartphone'
      GROUP BY
        p.id,
        p.name,
        b.name,
        b.logo,
        s.model,
        s.launch_date,
        s.launch_status_override
      ORDER BY p.id DESC;
    `);

    const toFiniteNumber = (value) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    };
    const toDateMs = (value) => {
      if (!value) return null;
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
    };
    const todayIndia = getIndiaDateOnly();
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
    const rows = (result.rows || []).map((row) => {
      const variants = (Array.isArray(row?.variants) ? row.variants : []).map(
        (variant) => {
          const variantObj = toPlainObject(variant);
          return {
            ...variantObj,
            id: variantObj.id ?? variantObj.variant_id ?? null,
            variant_id: variantObj.variant_id ?? variantObj.id ?? null,
            store_prices: decorateStorePriceList(
              Array.isArray(variantObj.store_prices)
                ? variantObj.store_prices
                : [],
              todayIndia,
            ),
          };
        },
      );
      const bestPrice =
        row.best_price !== null && row.best_price !== undefined
          ? Number(row.best_price)
          : resolveEffectiveSmartphonePrice(variants, null);
      const base = {
        product_id: Number(row.product_id),
        id: Number(row.product_id),
        name: row.name || row.model || "Smartphone",
        model: row.model || row.name || "Smartphone",
        launch_date: row.launch_date || null,
        brand_name: row.brand_name || null,
        brand_logo: row.brand_logo || null,
        brand_logo_url: row.brand_logo || null,
        launch_status_override: row.launch_status_override || null,
        sale_start_date:
          getEarliestSaleStartDateFromVariants(variants) ||
          row.sale_start_date ||
          null,
        price: bestPrice,
        starting_price: bestPrice,
        best_price: bestPrice,
        variants,
        hook_score: toFiniteNumber(row.hook_score),
        buyer_intent: toFiniteNumber(row.buyer_intent),
        trend_velocity: toFiniteNumber(row.trend_velocity),
        freshness: toFiniteNumber(row.freshness),
        has_purchase_signal: Boolean(row.has_purchase_signal),
      };

      applySmartphoneAvailabilityDetails(
        base,
        availabilityForecast,
        todayIndia,
      );
      const launchStage = resolveSmartphoneLaunchStage(base, todayIndia);
      const saleStage = resolveSmartphoneSaleStage(base, todayIndia);
      base.launch_status = launchStage;
      base.launchStatus = launchStage;
      base.sale_status = saleStage;
      base.saleStatus = saleStage;
      applySmartphoneLaunchPolicy(base, launchStage);

      return base;
    });

    const compareDescendingSignals =
      (...selectors) =>
      (left, right) => {
        for (const selector of selectors) {
          const difference = selector(right) - selector(left);
          if (difference !== 0) return difference;
        }
        return String(left?.name || "").localeCompare(
          String(right?.name || ""),
        );
      };

    const sanitizePhones = (items) =>
      items.slice(0, limit).map((item) =>
        toPublicSmartphoneResponse({
          id: item.id,
          product_id: item.product_id,
          name: item.name,
          model: item.model,
          brand_name: item.brand_name,
          brand_logo: item.brand_logo,
          brand_logo_url: item.brand_logo_url,
          launch_date: item.launch_date,
          launch_status: item.launch_status,
          launch_status_override: item.launch_status_override,
          sale_start_date: item.sale_start_date,
          best_price: item.best_price,
          sale_status: item.sale_status,
          store_stage: item.store_stage,
          available_date: item.available_date,
          predicted_available_date: item.predicted_available_date,
          available_date_label: item.available_date_label,
        }),
      );

    const latestWindowStart = new Date(todayIndia);
    latestWindowStart.setDate(latestWindowStart.getDate() - 90);
    const latestWindowEnd = new Date(todayIndia);
    latestWindowEnd.setDate(latestWindowEnd.getDate() - 15);
    const latestWindowStartMs = latestWindowStart.getTime();
    const latestWindowEndMs = latestWindowEnd.getTime();

    const latestPhones = [...rows]
      .map((item) => ({
        item,
        dateMs: toDateMs(item.sale_start_date || item.launch_date || null),
      }))
      .filter(
        (entry) =>
          entry.dateMs != null &&
          entry.dateMs >= latestWindowStartMs &&
          entry.dateMs <= latestWindowEndMs &&
          isSmartphoneLatestFeedItem(entry.item, todayIndia),
      )
      .sort((left, right) => {
        if (left.dateMs !== right.dateMs) {
          return right.dateMs - left.dateMs;
        }
        return String(right.item.name || "").localeCompare(
          String(left.item.name || ""),
        );
      })
      .map((entry) => entry.item);

    const highlightRows = [
      {
        label: "Upcoming Phones",
        phones: sanitizePhones(
          [...rows]
            .filter((item) => isSmartphoneUpcomingFeedItem(item, todayIndia))
            .sort((left, right) => {
              const leftDate =
                getSmartphoneFeedStartDate(left) || left.launch_date || "";
              const rightDate =
                getSmartphoneFeedStartDate(right) || right.launch_date || "";
              if (leftDate && rightDate && leftDate !== rightDate) {
                return String(leftDate).localeCompare(String(rightDate));
              }
              if (leftDate) return -1;
              if (rightDate) return 1;
              return String(left.name || "").localeCompare(
                String(right.name || ""),
              );
            }),
        ),
      },
      {
        label: "Trending Phones",
        phones: sanitizePhones(
          [...rows]
            .filter((item) => item.trend_velocity > 0)
            .sort(
              compareDescendingSignals(
                (item) => item.trend_velocity,
                (item) => item.buyer_intent,
                (item) => item.hook_score,
                (item) => item.freshness,
              ),
            ),
        ),
      },
      {
        label: "Latest Phones",
        phones: sanitizePhones(latestPhones),
      },
    ].filter((row) => row.phones.length > 0);

    return res.json({
      generated_at: new Date().toISOString(),
      highlights: highlightRows,
    });
  } catch (err) {
    console.error("GET /api/public/smartphones/highlights error:", err);
    return res.status(500).json({ error: err.message });
  }
});

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
        MAX(ds.hook_score) AS hook_score,

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

      ORDER BY
        COALESCE(MAX(ts.manual_priority), 0) DESC,
        COALESCE(MAX((ts.manual_boost)::int), 0) DESC,
        COALESCE(MAX(ds.hook_score), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC

      LIMIT $1;
    `,
      [limit],
    );

    const laptops = applySpecScoreToRows(
      "laptop",
      (result.rows || []).map((row, index) => {
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
          trend_badge: resolvePublicTrendBadge({
            manualBoost,
            manualBadge,
            rank: index + 1,
            hookScore: row?.hook_score,
            trendScore,
          }),
          trend_calculated_at: row?.trending_calculated_at ?? null,
        };
      }),
      profileConfig.profiles,
    );

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
        MAX(ds.hook_score) AS hook_score,

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
          SELECT COALESCE(
            (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = p.id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            ),
            CASE
              WHEN jsonb_typeof(t.images_json) = 'array'
              THEN t.images_json->>0
              ELSE NULL
            END
          )
        ) AS image,
        COALESCE(
          (
            SELECT json_agg(pi.image_url ORDER BY pi.position ASC NULLS LAST, pi.id ASC)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          CASE
            WHEN jsonb_typeof(t.images_json) = 'array'
            THEN t.images_json::json
            ELSE '[]'::json
          END
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
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
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
        t.warranty_json,
        t.images_json
      ORDER BY
        COALESCE(MAX(ts.manual_priority), 0) DESC,
        COALESCE(MAX((ts.manual_boost)::int), 0) DESC,
        COALESCE(MAX(ds.hook_score), 0) DESC,
        COALESCE(MAX(ds.buyer_intent), 0) DESC,
        COALESCE(MAX(ds.trend_velocity), 0) DESC,
        COALESCE(MAX(ds.freshness), 0) DESC,
        COALESCE(MAX(ts.trending_score), 0) DESC,
        p.id DESC
      LIMIT $1;
      `,
      [limit],
    );

    const tvs = applySpecScoreToRows(
      "tv",
      (result.rows || []).map((row, index) => {
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
          trend_badge: resolvePublicTrendBadge({
            manualBoost,
            manualBadge,
            rank: index + 1,
            hookScore: row?.hook_score,
            trendScore,
          }),
          trend_calculated_at: row?.trending_calculated_at ?? null,
        };
      }),
      profileConfig.profiles,
    ).map((row) => toPublicTvResponse(row || {}));

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
      LIMIT 60;
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
    const availabilityForecast = await fetchSmartphoneAvailabilityForecast();
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
      applySmartphoneAvailabilityDetails(
        item,
        availabilityForecast,
        todayIndia,
      );
      const launchStage = resolveSmartphoneLaunchStage(item, todayIndia);
      item.launch_status = launchStage;
      item.launchStatus = launchStage;
      applySmartphoneLaunchPolicy(item, launchStage);
    }

    const parseDateMs = (value) => {
      if (!value) return null;
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
    };
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const latestWindowMinMs = today.getTime() - 90 * 24 * 60 * 60 * 1000;
    const latestWindowMaxMs = today.getTime() - 15 * 24 * 60 * 60 * 1000;

    const publicLaunches = launches
      .map((item) => {
        const releaseDateMs = parseDateMs(
          item.sale_start_date ?? item.launch_date ?? null,
        );

        return {
          item,
          releaseDateMs,
        };
      })
      .filter(
        (entry) =>
          entry.releaseDateMs != null &&
          entry.releaseDateMs >= latestWindowMinMs &&
          entry.releaseDateMs <= latestWindowMaxMs &&
          isSmartphoneLatestFeedItem(entry.item, todayIndia),
      )
      .sort((left, right) => {
        if (left.releaseDateMs !== right.releaseDateMs) {
          return right.releaseDateMs - left.releaseDateMs;
        }
        return (
          Number(right.item.product_id || 0) - Number(left.item.product_id || 0)
        );
      })
      .slice(0, 20)
      .map((entry) => entry.item)
      .map((item) => toPublicSmartphoneResponse(item));

    return res.json({ new: publicLaunches });
  } catch (err) {
    console.error("GET /api/public/new/smartphones error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Laptops
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
        l.created_at AS launch_date,
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
      ORDER BY COALESCE(l.created_at, p.created_at) DESC
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
    );

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
    ).map((item) => toPublicTvResponse(item || {}));

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
      compare_path: matched ? `/compare/${left.slug}-vs-${right.slug}` : null,
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
                'attributes', v.attributes
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

    const compareConfig = await readCompareScoringConfig();
    const variantSelection = Object.fromEntries(
      normalizedDevices.map((entry) => [String(entry.product_id), entry]),
    );
    const ranking = buildCompareRanking(
      productResult.rows,
      variantSelection,
      compareConfig,
    );

    return res.json({
      scores: ranking.map((row) => ({
        product_id: Number(row.productId),
        overall_score: row.overallScore,
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
    await maybePruneOldCompareData();

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
        const session = await recordCompareSession({
          req,
          body,
          productIds: [l, r],
          productType: body.product_type,
        });
        await recordPairwiseComparisons([l, r]);
        return res.json({
          message: "Comparison recorded",
          product_count: 2,
          session_id: session?.session_id || null,
        });
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

    const session = await recordCompareSession({
      req,
      body,
      productIds: filtered,
      productType: body.product_type,
    });
    await recordPairwiseComparisons(filtered);

    return res.json({
      message: "Comparison recorded",
      product_count: filtered.length,
      session_id: session?.session_id || null,
    });
  } catch (err) {
    console.error("POST /api/public/compare error:", err);
    return res.status(500).json({ message: "Failed to record comparison" });
  }
});

app.get("/api/public/trending/most-compared", async (req, res) => {
  try {
    const days = toSafeCompareWindowDays(
      req.query?.days,
      FRESH_COMPARE_WIDGET_DAYS,
    );
    const limit = toSafeCompareLimit(req.query?.limit, 100);
    const scope = String(req.query?.scope || req.query?.mode || "pairs")
      .trim()
      .toLowerCase();
    const useGroups = ["group", "groups", "session", "sessions"].includes(
      scope,
    );

    if (useGroups) {
      const result = await db.query(
        `
        WITH session_groups AS (
          SELECT
            cs.id AS session_id,
            cs.visitor_key,
            cs.compared_at,
            ARRAY_AGG(csp.product_id ORDER BY csp.product_id)::int[] AS product_ids
          FROM compare_sessions cs
          INNER JOIN compare_session_products csp
            ON csp.session_id = cs.id
          WHERE cs.compared_at >= now() - make_interval(days => $1::int)
          GROUP BY cs.id, cs.visitor_key, cs.compared_at
          HAVING COUNT(csp.product_id) BETWEEN 2 AND 4
        ),
        group_counts AS (
          SELECT
            product_ids,
            COUNT(*)::int AS compare_count,
            COUNT(DISTINCT COALESCE(visitor_key, session_id::text))::int AS unique_users,
            MAX(compared_at) AS last_compared_at
          FROM session_groups
          GROUP BY product_ids
        )
        SELECT
          gc.product_ids,
          gc.compare_count,
          gc.unique_users,
          gc.last_compared_at,
          jsonb_agg(
            jsonb_build_object(
              'product_id', p.id,
              'product_name', p.name,
              'product_type', p.product_type,
              'brand_name', b.name,
              'image_url', (
                SELECT pi.image_url
                FROM product_images pi
                WHERE pi.product_id = p.id
                ORDER BY pi.position ASC NULLS LAST, pi.id ASC
                LIMIT 1
              ),
              'best_price', COALESCE(
                (
                  SELECT MIN(sp.price)::numeric
                  FROM product_variants pv
                  INNER JOIN variant_store_prices sp
                    ON sp.variant_id = pv.id
                  WHERE pv.product_id = p.id
                    AND sp.price IS NOT NULL
                ),
                (
                  SELECT MIN(pv.base_price)::numeric
                  FROM product_variants pv
                  WHERE pv.product_id = p.id
                    AND pv.base_price IS NOT NULL
                )
              )
            )
            ORDER BY array_position(gc.product_ids, p.id)
          ) AS products
        FROM group_counts gc
        INNER JOIN products p
          ON p.id = ANY(gc.product_ids)
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN brands b
          ON b.id = p.brand_id
        WHERE p.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
        GROUP BY gc.product_ids, gc.compare_count, gc.unique_users, gc.last_compared_at
        HAVING COUNT(*) = cardinality(gc.product_ids)
        ORDER BY gc.compare_count DESC, gc.last_compared_at DESC
        LIMIT $2
        `,
        [days, limit],
      );

      const toMostComparedGroupRow = (row) => {
        const products = (Array.isArray(row.products) ? row.products : [])
          .map((product) => ({
            product_id: Number(product.product_id),
            id: Number(product.product_id),
            product_name: product.product_name || "Device",
            name: product.product_name || "Device",
            product_type: product.product_type || "unknown",
            brand_name: product.brand_name || null,
            image_url: product.image_url || null,
            image: product.image_url || null,
            best_price: toSafeNumeric(product.best_price),
            detail_path: buildPublicProductDetailPath(
              product.product_type,
              product.product_name,
              product.product_id,
            ),
          }))
          .filter((product) => product.product_id);
        const [left, right] = products;
        const canBuildCompareRoute =
          products.length >= 2 &&
          products.length <= 3 &&
          products.every(
            (product) => product.product_type === products[0]?.product_type,
          );
        const comparePage = canBuildCompareRoute
          ? buildComparePagePayload(products, {
              manualCompareCount: row.compare_count,
              lastComparedAt: row.last_compared_at,
              updatedAt: row.last_compared_at,
            })
          : null;

        return {
          products,
          product_count: products.length,
          product_ids: products.map((product) => product.product_id),
          compare_count: Number(row.compare_count) || 0,
          unique_users: Number(row.unique_users) || 0,
          last_compared_at: row.last_compared_at || null,
          route_path: comparePage?.route_path || "/compare",
          product_id: left?.product_id || null,
          product_name: left?.product_name || null,
          product_type: left?.product_type || null,
          product_image: left?.image_url || null,
          compared_product_id: right?.product_id || null,
          compared_product_name: right?.product_name || null,
          compared_product_type: right?.product_type || null,
          compared_product_image: right?.image_url || null,
        };
      };
      const sessionRows = (result.rows || []).map(toMostComparedGroupRow);

      const pairResult = await db.query(
        `
        WITH pair_counts AS (
          SELECT
            LEAST(pc.product_id, pc.compared_with) AS left_product_id,
            GREATEST(pc.product_id, pc.compared_with) AS right_product_id,
            COUNT(pc.id)::int AS compare_count,
            MAX(pc.compared_at) AS last_compared_at
          FROM product_comparisons pc
          WHERE pc.compared_at >= now() - make_interval(days => $1::int)
          GROUP BY 1, 2
        )
        SELECT
          ARRAY[p1.id, p2.id]::int[] AS product_ids,
          pair_counts.compare_count,
          0::int AS unique_users,
          pair_counts.last_compared_at,
          jsonb_build_array(
            jsonb_build_object(
              'product_id', p1.id,
              'product_name', p1.name,
              'product_type', p1.product_type,
              'brand_name', b1.name,
              'image_url', (
                SELECT image_url
                FROM product_images
                WHERE product_id = p1.id
                ORDER BY position ASC NULLS LAST, id ASC
                LIMIT 1
              )
            ),
            jsonb_build_object(
              'product_id', p2.id,
              'product_name', p2.name,
              'product_type', p2.product_type,
              'brand_name', b2.name,
              'image_url', (
                SELECT image_url
                FROM product_images
                WHERE product_id = p2.id
                ORDER BY position ASC NULLS LAST, id ASC
                LIMIT 1
              )
            )
          ) AS products
        FROM pair_counts
        INNER JOIN products p1 ON p1.id = pair_counts.left_product_id
        INNER JOIN products p2 ON p2.id = pair_counts.right_product_id
        INNER JOIN product_publish pub1
          ON pub1.product_id = p1.id
         AND pub1.is_published = true
        INNER JOIN product_publish pub2
          ON pub2.product_id = p2.id
         AND pub2.is_published = true
        LEFT JOIN brands b1 ON b1.id = p1.brand_id
        LEFT JOIN brands b2 ON b2.id = p2.brand_id
        WHERE p1.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
          AND p2.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
        ORDER BY pair_counts.compare_count DESC, pair_counts.last_compared_at DESC
        LIMIT $2
        `,
        [days, limit],
      );
      const pairRows = (pairResult.rows || []).map(toMostComparedGroupRow);
      const rowsByKey = new Map();
      for (const row of [...pairRows, ...sessionRows]) {
        const key = (row.product_ids || [])
          .map((value) => Number(value))
          .filter((value) => Number.isInteger(value) && value > 0)
          .sort((left, right) => left - right)
          .join("-");
        if (!key) continue;
        const existing = rowsByKey.get(key);
        if (!existing) {
          rowsByKey.set(key, row);
          continue;
        }
        existing.compare_count = Math.max(
          Number(existing.compare_count) || 0,
          Number(row.compare_count) || 0,
        );
        existing.unique_users = Math.max(
          Number(existing.unique_users) || 0,
          Number(row.unique_users) || 0,
        );
        existing.last_compared_at =
          maxIsoTimestamp(existing.last_compared_at, row.last_compared_at) ||
          existing.last_compared_at ||
          row.last_compared_at ||
          null;
      }
      const rows = Array.from(rowsByKey.values())
        .sort((left, right) => {
          const countDiff =
            (Number(right.compare_count) || 0) -
            (Number(left.compare_count) || 0);
          if (countDiff !== 0) return countDiff;
          return (
            new Date(right.last_compared_at || 0).getTime() -
            new Date(left.last_compared_at || 0).getTime()
          );
        })
        .slice(0, limit);

      return res.json({
        generated_at: new Date().toISOString(),
        days,
        limit,
        scope: "groups",
        mostCompared: rows,
      });
    }

    const result = await db.query(
      `
      WITH pair_counts AS (
        SELECT
          LEAST(pc.product_id, pc.compared_with) AS left_product_id,
          GREATEST(pc.product_id, pc.compared_with) AS right_product_id,
          COUNT(pc.id)::int AS compare_count,
          MAX(pc.compared_at) AS last_compared_at
        FROM product_comparisons pc
        WHERE pc.compared_at >= now() - make_interval(days => $1::int)
        GROUP BY 1, 2
      )
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
        pair_counts.compare_count,
        pair_counts.last_compared_at
      FROM pair_counts
      JOIN products p1 ON p1.id = pair_counts.left_product_id
      JOIN products p2 ON p2.id = pair_counts.right_product_id
      INNER JOIN product_publish pub1
        ON pub1.product_id = p1.id
       AND pub1.is_published = true
      INNER JOIN product_publish pub2
        ON pub2.product_id = p2.id
       AND pub2.is_published = true
      WHERE p1.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
        AND p2.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
      ORDER BY pair_counts.compare_count DESC, pair_counts.last_compared_at DESC
      LIMIT $2
      `,
      [days, limit],
    );

    return res.json({
      generated_at: new Date().toISOString(),
      days,
      limit,
      scope: "pairs",
      mostCompared: result.rows,
    });
  } catch (err) {
    console.error("Most compared error:", err);
    return res.status(500).json({ error: "Internal server error" });
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

const SMARTPHONE_PUBLIC_DETAIL_SUFFIX = "-price-in-india";

const normalizePopularityProductType = (value = "") => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  if (!normalized || normalized === "all") return "";
  if (
    normalized === "smartphone" ||
    normalized === "smartphones" ||
    normalized === "mobile" ||
    normalized === "mobiles"
  ) {
    return "smartphone";
  }
  if (
    normalized === "laptop" ||
    normalized === "laptops" ||
    normalized === "notebook" ||
    normalized === "notebooks"
  ) {
    return "laptop";
  }
  if (
    normalized === "tv" ||
    normalized === "tvs" ||
    normalized === "television" ||
    normalized === "televisions" ||
    normalized === "home-appliance" ||
    normalized === "home-appliances"
  ) {
    return "tv";
  }
  if (
    normalized === "networking" ||
    normalized === "network" ||
    normalized === "router" ||
    normalized === "routers"
  ) {
    return "networking";
  }
  return null;
};

const buildPublicProductDetailPath = (productType, name, productId) => {
  const normalizedType =
    normalizePopularityProductType(productType) || "smartphone";
  const slug = toProductSlug(name, productId);

  if (normalizedType === "smartphone") {
    const baseSlug = slug.replace(
      new RegExp(`${SMARTPHONE_PUBLIC_DETAIL_SUFFIX}$`, "i"),
      "",
    );
    return `/smartphones/${baseSlug}${SMARTPHONE_PUBLIC_DETAIL_SUFFIX}`;
  }
  if (normalizedType === "laptop") return `/laptops/${slug}`;
  if (normalizedType === "tv") return `/tvs/${slug}`;
  if (normalizedType === "networking") return `/networking/${slug}`;
  return `/${slug}`;
};

const buildCompareVisitorKey = (req, body = {}) => {
  const visitorIdRaw =
    body.visitor_id ??
    body.visitorId ??
    body.compare_visitor_id ??
    body.compareVisitorId ??
    req.headers["x-visitor-id"] ??
    "";
  const ipRaw =
    (req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.ip || "";
  const userAgent = req.headers["user-agent"] || "";
  const keySource = visitorIdRaw
    ? `vid:${String(visitorIdRaw).trim()}`
    : `ip:${String(ipRaw)}|ua:${String(userAgent)}`;

  return crypto
    .createHash("sha256")
    .update(keySource)
    .digest("hex")
    .slice(0, 32);
};

const normalizeCompareProductIds = (values = []) =>
  Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  ).slice(0, 4);

let lastComparePruneAt = 0;
const maybePruneOldCompareData = async () => {
  const now = Date.now();
  if (now - lastComparePruneAt < 24 * 60 * 60 * 1000) return;
  lastComparePruneAt = now;

  try {
    await db.query(
      `DELETE FROM product_comparisons
       WHERE compared_at < now() - make_interval(days => $1::int)`,
      [COMPARE_DATA_RETENTION_DAYS],
    );
    await db.query(
      `DELETE FROM compare_sessions
       WHERE compared_at < now() - make_interval(days => $1::int)`,
      [COMPARE_DATA_RETENTION_DAYS],
    );
  } catch (err) {
    // Analytics cleanup should never block public compare logging.
    console.warn("Compare retention cleanup skipped:", err?.message || err);
  }
};

const recordCompareSession = async ({
  req,
  body = {},
  productIds = [],
  productType = "",
} = {}) => {
  const ids = normalizeCompareProductIds(productIds);
  if (ids.length < 2) return null;

  const visitorKey = buildCompareVisitorKey(req, body);
  const sessionKey =
    String(
      body.compare_session_id ||
        body.compareSessionId ||
        req.headers["x-compare-session-id"] ||
        "",
    ).trim() ||
    (typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : crypto.randomBytes(16).toString("hex"));
  const normalizedType = normalizePopularityProductType(productType) || null;

  try {
    const sessionResult = await db.query(
      `
      INSERT INTO compare_sessions (
        session_key,
        visitor_key,
        product_type,
        product_count
      )
      VALUES ($1, $2, $3, $4)
      RETURNING id, compared_at
      `,
      [sessionKey, visitorKey, normalizedType, ids.length],
    );
    const sessionId = Number(sessionResult.rows?.[0]?.id);
    if (!Number.isInteger(sessionId) || sessionId <= 0) return null;

    for (let index = 0; index < ids.length; index += 1) {
      await db.query(
        `
        INSERT INTO compare_session_products (session_id, product_id, position)
        VALUES ($1, $2, $3)
        ON CONFLICT (session_id, product_id) DO UPDATE
        SET position = EXCLUDED.position
        `,
        [sessionId, ids[index], index + 1],
      );
    }

    return {
      session_id: sessionId,
      compared_at: sessionResult.rows?.[0]?.compared_at || null,
    };
  } catch (err) {
    console.warn("Compare session logging skipped:", err?.message || err);
    return null;
  }
};

const recordPairwiseComparisons = async (productIds = []) => {
  const ids = normalizeCompareProductIds(productIds);
  for (let i = 0; i < ids.length; i += 1) {
    for (let j = i + 1; j < ids.length; j += 1) {
      const [left, right] = [ids[i], ids[j]].sort((a, b) => a - b);
      await db.query(
        `INSERT INTO product_comparisons (product_id, compared_with)
         VALUES ($1, $2)`,
        [left, right],
      );
    }
  }
};

const toSafeCompareWindowDays = (
  value,
  fallback = FRESH_COMPARE_WIDGET_DAYS,
) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(PUBLIC_COMPARE_WINDOW_DAYS, Math.max(1, Math.floor(parsed)));
};

const toSafeCompareLimit = (value, fallback = 100) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(500, Math.max(1, Math.floor(parsed)));
};

const parseComparePageSlug = (value = "") => {
  let normalized = "";
  try {
    normalized = decodeURIComponent(String(value || ""));
  } catch {
    normalized = String(value || "");
  }

  normalized = normalized.trim().toLowerCase().replace(/\/+$/g, "");

  if (!normalized) return [];

  const legacyMatch = normalized.match(/^(.+)-vs-(.+)$/i);
  if (legacyMatch) {
    return [legacyMatch[1], legacyMatch[2]]
      .map((part) => toProductSlug(part))
      .filter(Boolean);
  }

  if (!normalized.endsWith("-comparison")) return [];

  return normalized
    .replace(/-comparison$/i, "")
    .split("-and-")
    .map((part) => toProductSlug(part))
    .filter(Boolean)
    .slice(0, 3);
};

const joinCompareNamesWithoutCommas = (names = []) => {
  const clean = (Array.isArray(names) ? names : [])
    .map((name) => String(name || "").trim())
    .filter(Boolean);
  if (!clean.length) return "";
  if (clean.length === 1) return clean[0];
  return clean.join(" and ");
};

const resolveCompareSegmentLabelFromPrices = (prices = []) => {
  const numericPrices = (Array.isArray(prices) ? prices : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (!numericPrices.length) return "";

  const averagePrice =
    numericPrices.reduce((sum, value) => sum + value, 0) / numericPrices.length;
  if (averagePrice <= 10000) return "Entry";
  if (averagePrice <= 20000) return "Budget";
  if (averagePrice <= 30000) return "Lower Mid Range";
  if (averagePrice <= 45000) return "Mid Range";
  if (averagePrice <= 65000) return "Upper Mid Range";
  if (averagePrice <= 90000) return "Premium";
  if (averagePrice <= 130000) return "Flagship";
  return "Ultra Flagship";
};

const buildCompareRouteSlug = (items = []) => {
  const parts = (Array.isArray(items) ? items : [])
    .map((item) =>
      toProductSlug(item?.product_name || item?.name || "", item?.product_id),
    )
    .filter(Boolean)
    .slice(0, 3);

  if (parts.length < 2) return "";
  return `${parts.join("-and-")}-comparison`;
};

const buildComparePageTitle = ({ items = [], segmentLabel = "" } = {}) => {
  const joinedNames = joinCompareNamesWithoutCommas(
    (Array.isArray(items) ? items : []).map(
      (item) => item?.product_name || item?.name || "",
    ),
  );
  if (!joinedNames) {
    return "Compare Smartphones Price Specifications and Features in India";
  }

  const segment = String(segmentLabel || "").trim();
  if (segment) {
    return `Compare ${joinedNames} in the ${segment} Segment Price Specifications and Features in India`;
  }

  return `Compare ${joinedNames} Price Specifications and Features in India`;
};

const buildComparePageDescription = ({
  items = [],
  segmentLabel = "",
} = {}) => {
  const joinedNames = joinCompareNamesWithoutCommas(
    (Array.isArray(items) ? items : []).map(
      (item) => item?.product_name || item?.name || "",
    ),
  );
  if (!joinedNames) {
    return "Compare smartphones with latest price specifications camera battery performance and features in India.";
  }

  const segment = String(segmentLabel || "").trim();
  if (segment) {
    return `Compare ${joinedNames} in the ${segment} Segment with latest price specifications camera battery performance and features in India`;
  }

  return `Compare ${joinedNames} with latest price specifications camera battery performance and features in India`;
};

const buildComparePagePayload = (items = [], options = {}) => {
  const normalizedItems = [];
  const seen = new Set();

  for (const item of Array.isArray(items) ? items : []) {
    const productId = Number(item?.product_id ?? item?.productId ?? item?.id);
    if (!Number.isInteger(productId) || productId <= 0 || seen.has(productId)) {
      continue;
    }
    seen.add(productId);
    normalizedItems.push({
      product_id: productId,
      product_name: String(item?.product_name || item?.name || "Device").trim(),
      product_type:
        normalizePopularityProductType(item?.product_type) || "smartphone",
      brand_name: String(item?.brand_name || item?.brand || "").trim(),
      best_price: toSafeNumeric(
        item?.best_price ?? item?.bestPrice ?? item?.price,
      ),
      launch_date: item?.launch_date
        ? String(item.launch_date).slice(0, 10)
        : null,
      image_url: String(item?.image_url || item?.image || "").trim() || null,
      detail_path:
        String(item?.detail_path || item?.detailPath || "").trim() ||
        buildPublicProductDetailPath(
          item?.product_type,
          item?.product_name || item?.name || "",
          productId,
        ),
    });
  }

  if (normalizedItems.length < 2) return null;

  const productType = normalizedItems[0]?.product_type || "smartphone";
  if (
    normalizedItems.some(
      (item) =>
        normalizePopularityProductType(item?.product_type) !== productType,
    )
  ) {
    return null;
  }

  const segmentLabel =
    productType === "smartphone"
      ? resolveCompareSegmentLabelFromPrices(
          normalizedItems.map((item) => item.best_price),
        )
      : "";
  const slug =
    String(options.slug || "").trim() || buildCompareRouteSlug(normalizedItems);
  const generatedAt = options.generatedAt || new Date().toISOString();
  const updatedAt =
    options.updatedAt ||
    options.lastComparedAt ||
    options.publishedAt ||
    generatedAt;

  return {
    id: Number(options.id) || null,
    items: normalizedItems.map((item, index) => ({
      ...item,
      position: index + 1,
    })),
    primary_product_id:
      Number(options.primaryProductId) || normalizedItems[0].product_id || null,
    compare_key:
      String(options.compareKey || "").trim() ||
      `${productType}:${normalizedItems.map((item) => item.product_id).join("-")}`,
    segment_label: segmentLabel,
    smartphone_type_label: String(options.smartphoneTypeLabel || "").trim(),
    launch_date:
      normalizedItems
        .map((item) => item.launch_date)
        .filter(Boolean)
        .sort()
        .slice(-1)[0] || null,
    slug,
    title:
      String(options.title || "").trim() ||
      buildComparePageTitle({ items: normalizedItems, segmentLabel }),
    meta_description:
      String(options.metaDescription || "").trim() ||
      buildComparePageDescription({ items: normalizedItems, segmentLabel }),
    status:
      String(options.status || "published")
        .trim()
        .toLowerCase() === "draft"
        ? "draft"
        : "published",
    source: String(options.source || "automatic")
      .trim()
      .toLowerCase(),
    generation_reason: String(
      options.generationReason || "User Comparison Trend",
    ).trim(),
    system_score: toSafeNumeric(options.systemScore) || 0,
    manual_compare_count: Number(options.manualCompareCount) || 0,
    last_compared_at: options.lastComparedAt || null,
    generated_at: generatedAt,
    route_path: slug ? `/compare/${slug}` : "/compare",
    updated_at: updatedAt,
    published_at: options.publishedAt || updatedAt,
  };
};

const resolveSearchFreshnessScore = (row = {}) => {
  const dynamicFreshness = Number(row?.dynamic_freshness);
  if (Number.isFinite(dynamicFreshness) && dynamicFreshness > 0) {
    return Math.max(0, Math.min(100, dynamicFreshness));
  }

  const referenceDate = row?.reference_date
    ? new Date(row.reference_date)
    : null;
  if (!referenceDate || Number.isNaN(referenceDate.getTime())) return 30;

  const ageDays = Math.max(
    0,
    Math.floor((Date.now() - referenceDate.getTime()) / (24 * 60 * 60 * 1000)),
  );
  if (ageDays <= 30) return 100;
  if (ageDays <= 90) return 82;
  if (ageDays <= 180) return 68;
  if (ageDays <= 365) return 52;
  return 36;
};

const scalePopularitySignal = (value, max) => {
  const numericValue = Math.max(0, Number(value) || 0);
  const numericMax = Math.max(0, Number(max) || 0);
  if (numericValue <= 0 || numericMax <= 0) return 0;
  return (Math.log(numericValue + 1) / Math.log(numericMax + 1)) * 100;
};

const getPopularityBadge = (score, manualBadge = "") => {
  const normalizedManualBadge = String(manualBadge || "").trim();
  if (normalizedManualBadge) return normalizedManualBadge;
  const numericScore = Number(score) || 0;
  if (numericScore >= 80) return "Hot";
  if (numericScore >= 65) return "Trending";
  if (numericScore >= 50) return "Rising";
  if (numericScore >= 35) return "Popular";
  return "Live";
};

const toIsoOrNull = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
};

const maxIsoTimestamp = (...values) => {
  const timestamps = values
    .map((value) => (value ? new Date(value) : null))
    .filter((value) => value && !Number.isNaN(value.getTime()))
    .map((value) => value.getTime());

  if (!timestamps.length) return null;
  return new Date(Math.max(...timestamps)).toISOString();
};

const fetchSearchPopularityRows = async ({
  productType = "",
  days = 30,
  limit = 50,
} = {}) => {
  const normalizedType = normalizePopularityProductType(productType);
  const params = [Math.min(180, Math.max(1, Math.floor(Number(days) || 30)))];
  let typeWhere = "";

  if (normalizedType) {
    params.push(normalizedType);
    typeWhere = `AND p.product_type = $${params.length}`;
  }

  const result = await db.query(
    `
    WITH view_stats AS (
      SELECT
        product_id,
        COUNT(*) FILTER (
          WHERE viewed_at >= now() - make_interval(days => $1::int)
        )::int AS views_30d,
        COUNT(DISTINCT COALESCE(visitor_key, id::text)) FILTER (
          WHERE viewed_at >= now() - make_interval(days => $1::int)
        )::int AS unique_visitors_30d,
        MAX(viewed_at) FILTER (
          WHERE viewed_at >= now() - make_interval(days => $1::int)
        ) AS last_view_at
      FROM product_views
      GROUP BY product_id
    ),
    compare_stats AS (
      SELECT
        product_id,
        COUNT(*)::int AS compares_30d,
        MAX(compared_at) AS last_compared_at
      FROM (
        SELECT product_id, compared_at
        FROM product_comparisons
        WHERE compared_at >= now() - make_interval(days => $1::int)
        UNION ALL
        SELECT compared_with AS product_id, compared_at
        FROM product_comparisons
        WHERE compared_at >= now() - make_interval(days => $1::int)
      ) compared
      GROUP BY product_id
    )
    SELECT
      p.id AS product_id,
      p.name,
      p.product_type,
      b.name AS brand_name,
      (
        SELECT pi.image_url
        FROM product_images pi
        WHERE pi.product_id = p.id
        ORDER BY pi.position ASC NULLS LAST, pi.id ASC
        LIMIT 1
      ) AS image_url,
      COALESCE(
        (
          SELECT MIN(sp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = pv.id
          WHERE pv.product_id = p.id
            AND sp.price IS NOT NULL
        ),
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p.id
            AND pv.base_price IS NOT NULL
        )
      ) AS best_price,
      COALESCE(vs.views_30d, 0) AS views_30d,
      COALESCE(vs.views_30d, 0) AS search_count_30d,
      COALESCE(vs.unique_visitors_30d, 0) AS unique_visitors_30d,
      vs.last_view_at,
      COALESCE(cs.compares_30d, 0) AS compares_30d,
      cs.last_compared_at,
      COALESCE(ts.trending_score, 0) AS trending_score,
      COALESCE(ts.views_7d, 0) AS views_7d,
      COALESCE(ts.velocity, 0) AS velocity,
      COALESCE(ts.manual_boost, false) AS manual_boost,
      COALESCE(ts.manual_priority, 0) AS manual_priority,
      ts.manual_badge,
      ts.calculated_at AS trend_calculated_at,
      COALESCE(ds.buyer_intent, 0) AS buyer_intent,
      COALESCE(ds.hook_score, 0) AS hook_score,
      COALESCE(ds.trend_velocity, 0) AS trend_velocity,
      COALESCE(ds.freshness, 0) AS dynamic_freshness,
      ds.calculated_at AS dynamic_calculated_at,
      COALESCE(
        s.launch_date::timestamp,
        l.created_at,
        t.created_at,
        n.created_at,
        p.created_at
      ) AS reference_date
    FROM products p
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
     AND pub.is_published = true
    LEFT JOIN brands b
      ON b.id = p.brand_id
    LEFT JOIN smartphones s
      ON s.product_id = p.id
    LEFT JOIN laptop l
      ON l.product_id = p.id
    LEFT JOIN tvs t
      ON t.product_id = p.id
    LEFT JOIN networking n
      ON n.product_id = p.id
    LEFT JOIN view_stats vs
      ON vs.product_id = p.id
    LEFT JOIN compare_stats cs
      ON cs.product_id = p.id
    LEFT JOIN product_trending_score ts
      ON ts.product_id = p.id
    LEFT JOIN product_dynamic_score ds
      ON ds.product_id = p.id
    WHERE p.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
      ${typeWhere}
      AND (
        COALESCE(vs.views_30d, 0) > 0
        OR COALESCE(vs.unique_visitors_30d, 0) > 0
        OR COALESCE(cs.compares_30d, 0) > 0
        OR COALESCE(ts.trending_score, 0) > 0
        OR COALESCE(ds.buyer_intent, 0) > 0
        OR COALESCE(ds.hook_score, 0) > 0
        OR COALESCE(ds.trend_velocity, 0) > 0
      )
    `,
    params,
  );

  const rows = result.rows || [];
  const maxima = rows.reduce(
    (acc, row) => ({
      searches: Math.max(acc.searches, Number(row?.search_count_30d) || 0),
      uniques: Math.max(acc.uniques, Number(row?.unique_visitors_30d) || 0),
      compares: Math.max(acc.compares, Number(row?.compares_30d) || 0),
    }),
    { searches: 0, uniques: 0, compares: 0 },
  );

  const scoredRows = rows
    .map((row) => {
      const searchWeight = scalePopularitySignal(
        row?.search_count_30d,
        maxima.searches,
      );
      const uniqueWeight = scalePopularitySignal(
        row?.unique_visitors_30d,
        maxima.uniques,
      );
      const compareWeight = scalePopularitySignal(
        row?.compares_30d,
        maxima.compares,
      );
      const freshnessScore = resolveSearchFreshnessScore(row);
      const trendingScore = Math.max(
        0,
        Math.min(100, Number(row?.trending_score) || 0),
      );
      const buyerIntent = Math.max(
        0,
        Math.min(100, Number(row?.buyer_intent) || 0),
      );
      const hookScore = Math.max(
        0,
        Math.min(100, Number(row?.hook_score) || 0),
      );
      const trendVelocity = Math.max(
        0,
        Math.min(100, Number(row?.trend_velocity) || 0),
      );
      const manualPriority = Math.max(
        0,
        Math.min(12, Number(row?.manual_priority) || 0),
      );
      const popularityScore = Number(
        Math.min(
          100,
          searchWeight * 0.4 +
            uniqueWeight * 0.12 +
            compareWeight * 0.1 +
            trendingScore * 0.16 +
            buyerIntent * 0.1 +
            hookScore * 0.07 +
            trendVelocity * 0.03 +
            freshnessScore * 0.02 +
            (row?.manual_boost ? 4 : 0) +
            manualPriority * 0.5,
        ).toFixed(2),
      );

      return {
        product_id: Number(row.product_id),
        id: Number(row.product_id),
        name: row.name || "Device",
        product_name: row.name || "Device",
        product_type: row.product_type,
        brand_name: row.brand_name || null,
        image_url: row.image_url || null,
        image: row.image_url || null,
        detail_path: buildPublicProductDetailPath(
          row.product_type,
          row.name,
          row.product_id,
        ),
        best_price: toSafeNumeric(row.best_price),
        search_count_30d: Number(row.search_count_30d) || 0,
        search_count: Number(row.search_count_30d) || 0,
        searches: Number(row.search_count_30d) || 0,
        views_30d: Number(row.views_30d) || 0,
        unique_visitors_30d: Number(row.unique_visitors_30d) || 0,
        compares_30d: Number(row.compares_30d) || 0,
        search_weight: Number(searchWeight.toFixed(2)),
        freshness_score: Number(freshnessScore.toFixed(2)),
        search_popularity_score: popularityScore,
        popularity_score: popularityScore,
        score: popularityScore,
        badge: getPopularityBadge(popularityScore, row.manual_badge),
        hero_rank: 0,
        avg_dwell_seconds: null,
        last_search_at: toIsoOrNull(row.last_view_at),
        last_view_at: toIsoOrNull(row.last_view_at),
        last_engagement_at: maxIsoTimestamp(
          row.last_view_at,
          row.last_compared_at,
          row.trend_calculated_at,
          row.dynamic_calculated_at,
        ),
        _manual_priority: Number(row.manual_priority) || 0,
        _manual_boost: Boolean(row.manual_boost),
      };
    })
    .sort((left, right) => {
      const priorityDiff =
        (right._manual_priority || 0) - (left._manual_priority || 0);
      if (priorityDiff !== 0) return priorityDiff;
      const boostDiff =
        Number(Boolean(right._manual_boost)) -
        Number(Boolean(left._manual_boost));
      if (boostDiff !== 0) return boostDiff;
      const scoreDiff =
        (right.search_popularity_score || 0) -
        (left.search_popularity_score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const searchesDiff =
        (right.search_count_30d || 0) - (left.search_count_30d || 0);
      if (searchesDiff !== 0) return searchesDiff;
      return String(left.name || "").localeCompare(String(right.name || ""));
    })
    .map((row, index) => ({
      ...row,
      hero_rank: index + 1,
      rank: index + 1,
      _manual_priority: undefined,
      _manual_boost: undefined,
    }));

  return scoredRows.slice(0, Math.max(1, Math.floor(Number(limit) || 50)));
};

app.get("/api/public/search-popularity", async (req, res) => {
  try {
    const productTypeRaw =
      req.query?.productType ?? req.query?.product_type ?? "all";
    const normalizedType = normalizePopularityProductType(productTypeRaw);
    if (normalizedType === null) {
      return res.status(400).json({ message: "Invalid productType" });
    }

    const daysRaw = Number(req.query?.days ?? 30);
    const limitRaw = Number(req.query?.limit ?? 50);
    const days = Number.isFinite(daysRaw)
      ? Math.min(180, Math.max(1, Math.floor(daysRaw)))
      : 30;
    const limit = Number.isFinite(limitRaw)
      ? Math.min(100, Math.max(1, Math.floor(limitRaw)))
      : 50;

    const devices = await fetchSearchPopularityRows({
      productType: normalizedType || "",
      days,
      limit,
    });

    return res.json({
      generated_at: new Date().toISOString(),
      product_type: normalizedType || "all",
      days,
      limit,
      devices,
      data: devices,
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

    const productTypeRaw =
      req.query?.productType ?? req.query?.product_type ?? "all";
    const normalizedType = normalizePopularityProductType(productTypeRaw);
    if (normalizedType === null) {
      return res.status(400).json({ message: "Invalid productType" });
    }

    const daysRaw = Number(req.query?.days ?? 30);
    const limitRaw = Number(req.query?.limit ?? 50);
    const days = Number.isFinite(daysRaw)
      ? Math.min(180, Math.max(1, Math.floor(daysRaw)))
      : 30;
    const limit = Number.isFinite(limitRaw)
      ? Math.min(250, Math.max(1, Math.floor(limitRaw)))
      : 50;

    const devices = await fetchSearchPopularityRows({
      productType: normalizedType || "",
      days,
      limit,
    });

    return res.json({
      generated_at: new Date().toISOString(),
      product_type: normalizedType || "all",
      days,
      limit,
      devices,
      data: devices,
    });
  } catch (err) {
    console.error("GET /api/admin/search-popularity error:", err);
    return res
      .status(500)
      .json({ message: "Failed to load search popularity" });
  }
});

const parseComparePageItems = (items) => {
  if (Array.isArray(items)) return items;
  if (typeof items === "string") {
    try {
      const parsed = JSON.parse(items);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
};

const normalizeComparePageItem = (item, positionFallback = 1) => {
  if (!item || typeof item !== "object") return null;

  const productId = Number(item?.product_id ?? item?.productId ?? item?.id);
  if (!Number.isInteger(productId) || productId <= 0) return null;

  const productType =
    normalizePopularityProductType(item?.product_type ?? item?.productType) ||
    "smartphone";
  const productName = String(
    item?.product_name || item?.name || item?.title || "Device",
  ).trim();

  return {
    product_id: productId,
    product_name: productName,
    product_type: productType,
    brand_name: String(item?.brand_name || item?.brand || "").trim(),
    best_price: toSafeNumeric(
      item?.best_price ?? item?.bestPrice ?? item?.price,
    ),
    image_url: String(item?.image_url || item?.image || "").trim() || null,
    detail_path:
      String(item?.detail_path || item?.detailPath || "").trim() ||
      buildPublicProductDetailPath(productType, productName, productId),
    position: Number(item?.position) || positionFallback,
  };
};

const normalizeComparePageRecord = (row) => {
  if (!row) return null;
  const items = parseComparePageItems(row.items)
    .map((item, index) => normalizeComparePageItem(item, index + 1))
    .filter(Boolean);

  const primaryProductId =
    Number(row.primary_product_id ?? row.primaryProductId) ||
    items[0]?.product_id ||
    null;
  const launchDates = items
    .map((item) => item.launch_date)
    .filter(Boolean)
    .sort();

  return {
    id: Number(row.id) || null,
    items,
    primary_product_id: primaryProductId,
    compare_key: String(row.compare_key || row.compareKey || "").trim(),
    segment_label: String(row.segment_label || row.segmentLabel || "").trim(),
    smartphone_type_label: String(
      row.smartphone_type_label || row.smartphoneTypeLabel || "",
    ).trim(),
    launch_date:
      row.launch_date ||
      row.launchDate ||
      launchDates[launchDates.length - 1] ||
      null,
    oldest_launch_date: launchDates[0] || null,
    latest_launch_date: launchDates[launchDates.length - 1] || null,
    slug: String(row.slug || "").trim(),
    title: String(row.title || "").trim(),
    meta_description: String(
      row.meta_description || row.metaDescription || "",
    ).trim(),
    status:
      String(row.status || "published")
        .trim()
        .toLowerCase() === "draft"
        ? "draft"
        : "published",
    source:
      String(row.source || "manual")
        .trim()
        .toLowerCase() === "automatic"
        ? "automatic"
        : "manual",
    generation_reason: String(
      row.generation_reason || row.generationReason || "",
    ).trim(),
    system_score: toSafeNumeric(row.system_score ?? row.systemScore) || 0,
    manual_compare_count:
      Number(row.manual_compare_count ?? row.manualCompareCount) || 0,
    last_compared_at: row.last_compared_at ?? row.lastComparedAt ?? null,
    generated_at: row.generated_at ?? row.generatedAt ?? null,
    route_path:
      String(row.route_path || row.routePath || "").trim() ||
      (row.slug ? `/compare/${String(row.slug).trim()}` : "/compare"),
    updated_at: row.updated_at ?? row.updatedAt ?? null,
    published_at: row.published_at ?? row.publishedAt ?? null,
  };
};

const fetchComparePageProductsByIds = async (productIds = []) => {
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
      p.name AS product_name,
      p.product_type,
      b.name AS brand_name,
      s.launch_date::date AS launch_date,
      COALESCE(
        (
          SELECT MIN(sp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = pv.id
          WHERE pv.product_id = p.id
            AND sp.price IS NOT NULL
        ),
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p.id
            AND pv.base_price IS NOT NULL
        )
      ) AS best_price,
      (
        SELECT pi.image_url
        FROM product_images pi
        WHERE pi.product_id = p.id
        ORDER BY pi.position ASC NULLS LAST, pi.id ASC
        LIMIT 1
      ) AS image_url
    FROM products p
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
     AND pub.is_published = true
    LEFT JOIN brands b
      ON b.id = p.brand_id
    LEFT JOIN smartphones s
      ON s.product_id = p.id
    WHERE p.product_type = 'smartphone'
      AND p.id = ANY($1::int[])
    ORDER BY array_position($1::int[], p.id)
    `,
    [ids],
  );

  return (result.rows || []).map((row) => ({
    product_id: Number(row.product_id),
    product_name: row.product_name || "Device",
    product_type: row.product_type || "smartphone",
    brand_name: row.brand_name || null,
    launch_date: row.launch_date ? String(row.launch_date).slice(0, 10) : null,
    best_price: toSafeNumeric(row.best_price),
    image_url: row.image_url || null,
  }));
};

const fetchAutomaticComparePageCandidates = async ({
  days = 180,
  limit = 100,
} = {}) => {
  const safeDays = Number.isFinite(Number(days))
    ? Math.min(365, Math.max(7, Math.floor(Number(days))))
    : 180;
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.min(300, Math.max(1, Math.floor(Number(limit))))
    : 100;

  const result = await db.query(
    `
    WITH pair_counts AS (
      SELECT
        LEAST(pc.product_id, pc.compared_with) AS left_product_id,
        GREATEST(pc.product_id, pc.compared_with) AS right_product_id,
        COUNT(*)::int AS compare_count,
        MAX(pc.compared_at) AS last_compared_at
      FROM product_comparisons pc
      WHERE pc.compared_at >= now() - make_interval(days => $1::int)
      GROUP BY 1, 2
    )
    SELECT
      pair_counts.left_product_id,
      pair_counts.right_product_id,
      pair_counts.compare_count,
      pair_counts.last_compared_at,
      p1.name AS left_product_name,
      p1.product_type AS left_product_type,
      b1.name AS left_brand_name,
      p2.name AS right_product_name,
      p2.product_type AS right_product_type,
      b2.name AS right_brand_name,
      COALESCE(
        (
          SELECT MIN(sp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = pv.id
          WHERE pv.product_id = p1.id
            AND sp.price IS NOT NULL
        ),
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p1.id
            AND pv.base_price IS NOT NULL
        )
      ) AS left_best_price,
      COALESCE(
        (
          SELECT MIN(sp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices sp
            ON sp.variant_id = pv.id
          WHERE pv.product_id = p2.id
            AND sp.price IS NOT NULL
        ),
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p2.id
            AND pv.base_price IS NOT NULL
        )
      ) AS right_best_price,
      (
        SELECT pi.image_url
        FROM product_images pi
        WHERE pi.product_id = p1.id
        ORDER BY pi.position ASC NULLS LAST, pi.id ASC
        LIMIT 1
      ) AS left_image_url,
      (
        SELECT pi.image_url
        FROM product_images pi
        WHERE pi.product_id = p2.id
        ORDER BY pi.position ASC NULLS LAST, pi.id ASC
        LIMIT 1
      ) AS right_image_url
    FROM pair_counts
    INNER JOIN products p1
      ON p1.id = pair_counts.left_product_id
    INNER JOIN products p2
      ON p2.id = pair_counts.right_product_id
    INNER JOIN product_publish pub1
      ON pub1.product_id = p1.id
     AND pub1.is_published = true
    INNER JOIN product_publish pub2
      ON pub2.product_id = p2.id
     AND pub2.is_published = true
    LEFT JOIN brands b1
      ON b1.id = p1.brand_id
    LEFT JOIN brands b2
      ON b2.id = p2.brand_id
    WHERE p1.product_type = 'smartphone'
      AND p2.product_type = 'smartphone'
    ORDER BY
      pair_counts.compare_count DESC,
      pair_counts.last_compared_at DESC,
      p1.id DESC,
      p2.id DESC
    LIMIT $2
    `,
    [safeDays, safeLimit],
  );

  return (result.rows || [])
    .map((row) =>
      buildComparePagePayload(
        [
          {
            product_id: row.left_product_id,
            product_name: row.left_product_name,
            product_type: row.left_product_type,
            brand_name: row.left_brand_name,
            best_price: row.left_best_price,
            image_url: row.left_image_url,
          },
          {
            product_id: row.right_product_id,
            product_name: row.right_product_name,
            product_type: row.right_product_type,
            brand_name: row.right_brand_name,
            best_price: row.right_best_price,
            image_url: row.right_image_url,
          },
        ],
        {
          source: "automatic",
          generationReason: "User Comparison Trend",
          manualCompareCount: row.compare_count,
          lastComparedAt: row.last_compared_at,
          generatedAt: new Date().toISOString(),
          updatedAt: row.last_compared_at,
          publishedAt: row.last_compared_at,
          systemScore: row.compare_count,
        },
      ),
    )
    .filter(Boolean)
    .slice(0, safeLimit);
};

const toComparePageDbValues = (page, nowIso = new Date().toISOString()) => [
  String(page.compare_key || "").trim(),
  Number.isInteger(Number(page.primary_product_id))
    ? Number(page.primary_product_id)
    : null,
  JSON.stringify(Array.isArray(page.items) ? page.items : []),
  String(page.segment_label || "").trim() || null,
  String(page.smartphone_type_label || "").trim() || null,
  String(page.slug || "").trim() || null,
  String(page.title || "").trim() || null,
  String(page.meta_description || "").trim() || null,
  String(page.status || "published")
    .trim()
    .toLowerCase() === "draft"
    ? "draft"
    : "published",
  String(page.source || "manual")
    .trim()
    .toLowerCase() === "automatic"
    ? "automatic"
    : "manual",
  String(page.generation_reason || "").trim() || null,
  Number.isFinite(Number(page.system_score)) ? Number(page.system_score) : 0,
  Number.isFinite(Number(page.manual_compare_count))
    ? Number(page.manual_compare_count)
    : 0,
  page.last_compared_at || null,
  page.generated_at || nowIso,
  page.route_path ||
    (page.slug ? `/compare/${String(page.slug).trim()}` : "/compare"),
  page.updated_at || nowIso,
  page.published_at || page.updated_at || page.generated_at || nowIso,
];

const persistComparePageRecord = async (page) => {
  if (!page || !page.compare_key) return null;

  const nowIso = new Date().toISOString();
  const values = toComparePageDbValues(page, nowIso);
  const pageId = Number(page.id);

  if (Number.isInteger(pageId) && pageId > 0) {
    const updateResult = await db.query(
      `
      UPDATE compare_pages
      SET
        compare_key = $2,
        primary_product_id = $3,
        items = $4::jsonb,
        segment_label = $5,
        smartphone_type_label = $6,
        slug = $7,
        title = $8,
        meta_description = $9,
        status = $10,
        source = $11,
        generation_reason = $12,
        system_score = $13,
        manual_compare_count = $14,
        last_compared_at = $15,
        generated_at = $16,
        route_path = $17,
        updated_at = $18,
        published_at = $19
      WHERE id = $1
      RETURNING *;
      `,
      [pageId, ...values],
    );

    if (updateResult.rows?.[0])
      return normalizeComparePageRecord(updateResult.rows[0]);
  }

  const result = await db.query(
    `
    INSERT INTO compare_pages (
      compare_key,
      primary_product_id,
      items,
      segment_label,
      smartphone_type_label,
      slug,
      title,
      meta_description,
      status,
      source,
      generation_reason,
      system_score,
      manual_compare_count,
      last_compared_at,
      generated_at,
      route_path,
      updated_at,
      published_at
    ) VALUES (
      $1,$2,$3::jsonb,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
    )
    ON CONFLICT (compare_key) DO UPDATE SET
      primary_product_id = EXCLUDED.primary_product_id,
      items = EXCLUDED.items,
      segment_label = EXCLUDED.segment_label,
      smartphone_type_label = EXCLUDED.smartphone_type_label,
      slug = EXCLUDED.slug,
      title = EXCLUDED.title,
      meta_description = EXCLUDED.meta_description,
      status = EXCLUDED.status,
      source = EXCLUDED.source,
      generation_reason = EXCLUDED.generation_reason,
      system_score = EXCLUDED.system_score,
      manual_compare_count = EXCLUDED.manual_compare_count,
      last_compared_at = EXCLUDED.last_compared_at,
      generated_at = EXCLUDED.generated_at,
      route_path = EXCLUDED.route_path,
      updated_at = EXCLUDED.updated_at,
      published_at = EXCLUDED.published_at
    WHERE compare_pages.source <> 'manual' OR EXCLUDED.source = 'manual'
    RETURNING *;
    `,
    values,
  );

  if (result.rows?.[0]) return normalizeComparePageRecord(result.rows[0]);

  const fallback = await db.query(
    `SELECT * FROM compare_pages WHERE compare_key = $1 LIMIT 1`,
    [values[0]],
  );
  return normalizeComparePageRecord(fallback.rows?.[0] || null);
};

const syncAutomaticComparePages = async ({ days = 180, limit = 100 } = {}) => {
  const pages = await fetchAutomaticComparePageCandidates({ days, limit });
  const generatedKeys = pages
    .map((page) => String(page.compare_key || "").trim())
    .filter(Boolean);

  for (const page of pages) {
    // Automatic pages stay fresh, but manual edits always win because the
    // upsert skips rows whose stored source is already manual.
    await persistComparePageRecord(page);
  }

  if (generatedKeys.length) {
    await db.query(
      `
      UPDATE compare_pages
      SET
        status = 'draft',
        updated_at = now()
      WHERE source = 'automatic'
        AND NOT (compare_key = ANY($1::text[]))
      `,
      [generatedKeys],
    );
  } else {
    await db.query(
      `
      UPDATE compare_pages
      SET
        status = 'draft',
        updated_at = now()
      WHERE source = 'automatic'
      `,
    );
  }

  return {
    generated: pages.length,
    keys: generatedKeys,
  };
};

const requireAdminAccess = (req, res) => {
  const role = normalizeRbacRole(req.user?.role || "");
  if (role !== "admin" && role !== "ceo") {
    res.status(403).json({ message: "Admin access required" });
    return false;
  }
  return true;
};

const extractComparePageProductIds = (body = {}) => {
  const sourceItems = Array.isArray(body.items) ? body.items : [];
  const rawIds = Array.isArray(body.product_ids)
    ? body.product_ids
    : Array.isArray(body.productIds)
      ? body.productIds
      : sourceItems.map(
          (item) => item?.product_id ?? item?.productId ?? item?.id,
        );

  return Array.from(
    new Set(
      rawIds
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0),
    ),
  );
};

const fetchComparePageRecordById = async (pageId) => {
  const id = Number(pageId);
  if (!Number.isInteger(id) || id <= 0) return null;

  const result = await db.query(
    `SELECT * FROM compare_pages WHERE id = $1 LIMIT 1`,
    [id],
  );
  return normalizeComparePageRecord(result.rows?.[0] || null);
};

const fetchComparePageRecordByProductId = async (productId) => {
  const id = Number(productId);
  if (!Number.isInteger(id) || id <= 0) return null;

  const result = await db.query(
    `
    SELECT *
    FROM compare_pages
    WHERE primary_product_id = $1
       OR EXISTS (
         SELECT 1
         FROM jsonb_array_elements(COALESCE(items, '[]'::jsonb)) item
         WHERE item->>'product_id' = $1::text
       )
    ORDER BY
      CASE WHEN primary_product_id = $1 THEN 0 ELSE 1 END,
      CASE WHEN source = 'manual' THEN 0 ELSE 1 END,
      updated_at DESC NULLS LAST,
      id DESC
    LIMIT 1
    `,
    [id],
  );

  return normalizeComparePageRecord(result.rows?.[0] || null);
};

const resolveComparePageAnalytics = async (
  page,
  days = PUBLIC_COMPARE_WINDOW_DAYS,
) => {
  const safeDays = Math.min(
    PUBLIC_COMPARE_WINDOW_DAYS,
    Math.max(7, Math.floor(Number(days) || PUBLIC_COMPARE_WINDOW_DAYS)),
  );
  const fallbackLastComparedAt = maxIsoTimestamp(page?.last_compared_at);
  const fallbackDeadAt = fallbackLastComparedAt
    ? new Date(
        new Date(fallbackLastComparedAt).getTime() +
          safeDays * 24 * 60 * 60 * 1000,
      ).toISOString()
    : null;
  const fallbackCompareCount =
    Number(
      page?.compare_count_180d ??
        page?.compare_count ??
        page?.manual_compare_count,
    ) || 0;
  const fallbackAnalytics = {
    ...page,
    compare_count_180d: fallbackCompareCount,
    compare_count: fallbackCompareCount,
    unique_users_180d:
      Number(
        page?.unique_users_180d ??
          page?.unique_user_count ??
          page?.unique_users,
      ) || 0,
    unique_user_count:
      Number(
        page?.unique_user_count ??
          page?.unique_users_180d ??
          page?.unique_users,
      ) || 0,
    unique_users:
      Number(
        page?.unique_users ??
          page?.unique_users_180d ??
          page?.unique_user_count,
      ) || 0,
    last_compared_at: fallbackLastComparedAt || page?.last_compared_at || null,
    alive_date: fallbackLastComparedAt || null,
    alive_at: fallbackLastComparedAt || null,
    dead_date: fallbackDeadAt,
    dead_at: fallbackDeadAt,
    is_alive:
      String(page?.source || "").toLowerCase() === "manual" ||
      (fallbackDeadAt
        ? new Date(fallbackDeadAt).getTime() >= Date.now()
        : false),
    analytics_window_days: safeDays,
  };

  const ids = normalizeCompareProductIds(
    (Array.isArray(page?.items) ? page.items : []).map(
      (item) => item?.product_id ?? item?.productId ?? item?.id,
    ),
  ).sort((left, right) => left - right);

  if (ids.length < 2) return fallbackAnalytics;

  let pairCompareCount = 0;
  let pairLastComparedAt = null;
  if (ids.length === 2) {
    try {
      const pairResult = await db.query(
        `
        SELECT
          COUNT(*)::int AS compare_count,
          MAX(compared_at) AS last_compared_at
        FROM product_comparisons
        WHERE compared_at >= now() - make_interval(days => $3::int)
          AND LEAST(product_id, compared_with) = LEAST($1, $2)
          AND GREATEST(product_id, compared_with) = GREATEST($1, $2)
        `,
        [ids[0], ids[1], safeDays],
      );
      pairCompareCount = Number(pairResult.rows?.[0]?.compare_count) || 0;
      pairLastComparedAt = pairResult.rows?.[0]?.last_compared_at || null;
    } catch (err) {
      console.warn("Compare pair analytics skipped:", {
        compare_page_id: page?.id,
        message: err?.message,
      });
    }
  }

  let sessionCompareCount = 0;
  let uniqueUsers = 0;
  let sessionLastComparedAt = null;
  try {
    const sessionResult = await db.query(
      `
      WITH session_groups AS (
        SELECT
          cs.id AS session_id,
          cs.visitor_key,
          cs.compared_at,
          ARRAY_AGG(csp.product_id ORDER BY csp.product_id)::int[] AS product_ids
        FROM compare_sessions cs
        INNER JOIN compare_session_products csp
          ON csp.session_id = cs.id
        WHERE cs.compared_at >= now() - make_interval(days => $2::int)
        GROUP BY cs.id, cs.visitor_key, cs.compared_at
        HAVING COUNT(csp.product_id) BETWEEN 2 AND 4
      )
      SELECT
        COUNT(*)::int AS compare_count,
        COUNT(DISTINCT COALESCE(visitor_key, session_id::text))::int AS unique_users,
        MAX(compared_at) AS last_compared_at
      FROM session_groups
      WHERE product_ids = $1::int[]
      `,
      [ids, safeDays],
    );
    sessionCompareCount = Number(sessionResult.rows?.[0]?.compare_count) || 0;
    uniqueUsers = Number(sessionResult.rows?.[0]?.unique_users) || 0;
    sessionLastComparedAt = sessionResult.rows?.[0]?.last_compared_at || null;
  } catch (err) {
    console.warn("Compare session analytics skipped:", {
      compare_page_id: page?.id,
      message: err?.message,
    });
  }

  let launchRow = {};
  try {
    const launchResult = await db.query(
      `
      WITH product_launches AS (
        SELECT
          COALESCE(
            s.launch_date::date,
            l.created_at::date,
            t.created_at::date,
            n.created_at::date,
            p.created_at::date
          ) AS launch_date
        FROM products p
        LEFT JOIN smartphones s
          ON s.product_id = p.id
        LEFT JOIN laptop l
          ON l.product_id = p.id
        LEFT JOIN tvs t
          ON t.product_id = p.id
        LEFT JOIN networking n
          ON n.product_id = p.id
        WHERE p.id = ANY($1::int[])
      )
      SELECT
        MIN(launch_date)::date AS oldest_launch_date,
        MAX(launch_date)::date AS latest_launch_date
      FROM product_launches
      `,
      [ids],
    );
    launchRow = launchResult.rows?.[0] || {};
  } catch (err) {
    console.warn("Compare launch analytics skipped:", {
      compare_page_id: page?.id,
      message: err?.message,
    });
  }

  const lastComparedAt = maxIsoTimestamp(
    pairLastComparedAt,
    sessionLastComparedAt,
    page?.last_compared_at,
  );
  const deadAt = lastComparedAt
    ? new Date(
        new Date(lastComparedAt).getTime() +
          PUBLIC_COMPARE_WINDOW_DAYS * 24 * 60 * 60 * 1000,
      ).toISOString()
    : null;
  const compareCount =
    ids.length === 2
      ? Math.max(pairCompareCount, sessionCompareCount)
      : sessionCompareCount;
  const latestLaunchDate =
    launchRow.latest_launch_date ||
    page?.latest_launch_date ||
    page?.launch_date ||
    null;
  const oldestLaunchDate =
    launchRow.oldest_launch_date || page?.oldest_launch_date || null;

  return {
    ...page,
    compare_count_180d: compareCount,
    compare_count: compareCount,
    unique_users_180d: uniqueUsers,
    unique_user_count: uniqueUsers,
    unique_users: uniqueUsers,
    last_compared_at: lastComparedAt || page?.last_compared_at || null,
    alive_date: lastComparedAt || null,
    alive_at: lastComparedAt || null,
    dead_date: deadAt,
    dead_at: deadAt,
    is_alive:
      String(page?.source || "").toLowerCase() === "manual" ||
      (deadAt ? new Date(deadAt).getTime() >= Date.now() : false),
    launch_date: latestLaunchDate
      ? String(latestLaunchDate).slice(0, 10)
      : null,
    latest_launch_date: latestLaunchDate
      ? String(latestLaunchDate).slice(0, 10)
      : null,
    oldest_launch_date: oldestLaunchDate
      ? String(oldestLaunchDate).slice(0, 10)
      : null,
    analytics_window_days: safeDays,
  };
};

const hydrateComparePagesWithAnalytics = async (
  pages = [],
  days = PUBLIC_COMPARE_WINDOW_DAYS,
) => {
  const normalizedPages = Array.isArray(pages) ? pages : [];
  return Promise.all(
    normalizedPages.map((page) =>
      resolveComparePageAnalytics(page, days).catch((err) => {
        console.warn("Compare page analytics hydration skipped:", {
          compare_page_id: page?.id,
          message: err?.message,
        });
        return page;
      }),
    ),
  );
};

const addDaysToIso = (value, days = PUBLIC_COMPARE_WINDOW_DAYS) => {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) return null;
  date.setDate(date.getDate() + days);
  return date.toISOString();
};

const normalizeCompareGroupKey = (productIds = []) =>
  normalizeCompareProductIds(productIds)
    .sort((left, right) => left - right)
    .join("-");

const normalizeUserCompareProducts = (products = []) =>
  (Array.isArray(products) ? products : [])
    .map((product) => {
      const productId = Number(product?.product_id ?? product?.id);
      if (!Number.isInteger(productId) || productId <= 0) return null;
      const productType =
        normalizePopularityProductType(product?.product_type) || "unknown";
      return {
        product_id: productId,
        id: productId,
        product_name: product?.product_name || product?.name || "Device",
        name: product?.product_name || product?.name || "Device",
        product_type: productType,
        brand_name: product?.brand_name || product?.brand || null,
        image_url: product?.image_url || product?.image || null,
        image: product?.image_url || product?.image || null,
        best_price: toSafeNumeric(product?.best_price ?? product?.price),
        launch_date: product?.launch_date
          ? String(product.launch_date).slice(0, 10)
          : null,
        detail_path:
          product?.detail_path ||
          buildPublicProductDetailPath(
            productType,
            product?.product_name || product?.name || "",
            productId,
          ),
      };
    })
    .filter(Boolean);

const fetchAdminUserCompareGroups = async ({
  days = COMPARE_DATA_RETENTION_DAYS,
  limit = 500,
} = {}) => {
  const safeDays = Number.isFinite(Number(days))
    ? Math.min(
        COMPARE_DATA_RETENTION_DAYS,
        Math.max(1, Math.floor(Number(days))),
      )
    : COMPARE_DATA_RETENTION_DAYS;
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.min(1000, Math.max(1, Math.floor(Number(limit))))
    : 500;

  const rowsByKey = new Map();
  const mergeRow = (row = {}, source = "session") => {
    const products = normalizeUserCompareProducts(row.products || []);
    const productIds = normalizeCompareProductIds(
      row.product_ids || products.map((product) => product.product_id),
    ).sort((left, right) => left - right);
    const key = normalizeCompareGroupKey(productIds);
    if (!key || productIds.length < 2) return;

    const existing = rowsByKey.get(key);
    const next = {
      group_key: key,
      product_ids: productIds,
      product_count: productIds.length,
      products,
      compare_count: Number(row.compare_count) || 0,
      unique_users: Number(row.unique_users) || 0,
      first_compared_at: row.first_compared_at || null,
      last_compared_at: row.last_compared_at || null,
      data_source: source,
    };

    if (!existing) {
      rowsByKey.set(key, next);
      return;
    }

    existing.compare_count = Math.max(
      existing.compare_count,
      next.compare_count,
    );
    existing.unique_users = Math.max(existing.unique_users, next.unique_users);
    existing.first_compared_at =
      existing.first_compared_at && next.first_compared_at
        ? new Date(existing.first_compared_at).getTime() <=
          new Date(next.first_compared_at).getTime()
          ? existing.first_compared_at
          : next.first_compared_at
        : existing.first_compared_at || next.first_compared_at || null;
    existing.last_compared_at =
      maxIsoTimestamp(existing.last_compared_at, next.last_compared_at) ||
      existing.last_compared_at ||
      next.last_compared_at ||
      null;
    existing.products = existing.products.length
      ? existing.products
      : next.products;
    existing.data_source =
      existing.data_source === "session" || source === "session"
        ? "session"
        : source;
  };

  try {
    const sessionResult = await db.query(
      `
      WITH session_groups AS (
        SELECT
          cs.id AS session_id,
          cs.visitor_key,
          cs.compared_at,
          ARRAY_AGG(csp.product_id ORDER BY csp.product_id)::int[] AS product_ids
        FROM compare_sessions cs
        INNER JOIN compare_session_products csp
          ON csp.session_id = cs.id
        WHERE cs.compared_at >= now() - make_interval(days => $1::int)
        GROUP BY cs.id, cs.visitor_key, cs.compared_at
        HAVING COUNT(csp.product_id) BETWEEN 2 AND 4
      ),
      group_counts AS (
        SELECT
          product_ids,
          COUNT(*)::int AS compare_count,
          COUNT(DISTINCT COALESCE(visitor_key, session_id::text))::int AS unique_users,
          MIN(compared_at) AS first_compared_at,
          MAX(compared_at) AS last_compared_at
        FROM session_groups
        GROUP BY product_ids
      )
      SELECT
        gc.product_ids,
        gc.compare_count,
        gc.unique_users,
        gc.first_compared_at,
        gc.last_compared_at,
        jsonb_agg(
          jsonb_build_object(
            'product_id', p.id,
            'product_name', p.name,
            'product_type', p.product_type,
            'brand_name', b.name,
            'image_url', (
              SELECT pi.image_url
              FROM product_images pi
              WHERE pi.product_id = p.id
              ORDER BY pi.position ASC NULLS LAST, pi.id ASC
              LIMIT 1
            ),
            'best_price', COALESCE(
              (
                SELECT MIN(sp.price)::numeric
                FROM product_variants pv
                INNER JOIN variant_store_prices sp
                  ON sp.variant_id = pv.id
                WHERE pv.product_id = p.id
                  AND sp.price IS NOT NULL
              ),
              (
                SELECT MIN(pv.base_price)::numeric
                FROM product_variants pv
                WHERE pv.product_id = p.id
                  AND pv.base_price IS NOT NULL
              )
            ),
            'launch_date', COALESCE(
              s.launch_date::date,
              l.created_at::date,
              t.created_at::date,
              n.created_at::date,
              p.created_at::date
            )
          )
          ORDER BY array_position(gc.product_ids, p.id)
        ) AS products
      FROM group_counts gc
      INNER JOIN products p
        ON p.id = ANY(gc.product_ids)
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN smartphones s
        ON s.product_id = p.id
      LEFT JOIN laptop l
        ON l.product_id = p.id
      LEFT JOIN tvs t
        ON t.product_id = p.id
      LEFT JOIN networking n
        ON n.product_id = p.id
      WHERE p.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
      GROUP BY gc.product_ids, gc.compare_count, gc.unique_users, gc.first_compared_at, gc.last_compared_at
      HAVING COUNT(*) = cardinality(gc.product_ids)
      ORDER BY gc.compare_count DESC, gc.last_compared_at DESC
      LIMIT $2
      `,
      [safeDays, safeLimit],
    );

    (sessionResult.rows || []).forEach((row) => mergeRow(row, "session"));
  } catch (err) {
    console.warn("Admin user compare session groups skipped:", err?.message);
  }

  try {
    const pairResult = await db.query(
      `
      WITH pair_counts AS (
        SELECT
          LEAST(pc.product_id, pc.compared_with) AS left_product_id,
          GREATEST(pc.product_id, pc.compared_with) AS right_product_id,
          COUNT(*)::int AS compare_count,
          MIN(pc.compared_at) AS first_compared_at,
          MAX(pc.compared_at) AS last_compared_at
        FROM product_comparisons pc
        WHERE pc.compared_at >= now() - make_interval(days => $1::int)
        GROUP BY 1, 2
      )
      SELECT
        ARRAY[p1.id, p2.id]::int[] AS product_ids,
        pair_counts.compare_count,
        0::int AS unique_users,
        pair_counts.first_compared_at,
        pair_counts.last_compared_at,
        jsonb_build_array(
          jsonb_build_object(
            'product_id', p1.id,
            'product_name', p1.name,
            'product_type', p1.product_type,
            'brand_name', b1.name,
            'image_url', (
              SELECT image_url
              FROM product_images
              WHERE product_id = p1.id
              ORDER BY position ASC NULLS LAST, id ASC
              LIMIT 1
            ),
            'best_price', COALESCE(
              (
                SELECT MIN(sp.price)::numeric
                FROM product_variants pv
                INNER JOIN variant_store_prices sp
                  ON sp.variant_id = pv.id
                WHERE pv.product_id = p1.id
                  AND sp.price IS NOT NULL
              ),
              (
                SELECT MIN(pv.base_price)::numeric
                FROM product_variants pv
                WHERE pv.product_id = p1.id
                  AND pv.base_price IS NOT NULL
              )
            ),
            'launch_date', COALESCE(s1.launch_date::date, l1.created_at::date, t1.created_at::date, n1.created_at::date, p1.created_at::date)
          ),
          jsonb_build_object(
            'product_id', p2.id,
            'product_name', p2.name,
            'product_type', p2.product_type,
            'brand_name', b2.name,
            'image_url', (
              SELECT image_url
              FROM product_images
              WHERE product_id = p2.id
              ORDER BY position ASC NULLS LAST, id ASC
              LIMIT 1
            ),
            'best_price', COALESCE(
              (
                SELECT MIN(sp.price)::numeric
                FROM product_variants pv
                INNER JOIN variant_store_prices sp
                  ON sp.variant_id = pv.id
                WHERE pv.product_id = p2.id
                  AND sp.price IS NOT NULL
              ),
              (
                SELECT MIN(pv.base_price)::numeric
                FROM product_variants pv
                WHERE pv.product_id = p2.id
                  AND pv.base_price IS NOT NULL
              )
            ),
            'launch_date', COALESCE(s2.launch_date::date, l2.created_at::date, t2.created_at::date, n2.created_at::date, p2.created_at::date)
          )
        ) AS products
      FROM pair_counts
      INNER JOIN products p1 ON p1.id = pair_counts.left_product_id
      INNER JOIN products p2 ON p2.id = pair_counts.right_product_id
      INNER JOIN product_publish pub1
        ON pub1.product_id = p1.id
       AND pub1.is_published = true
      INNER JOIN product_publish pub2
        ON pub2.product_id = p2.id
       AND pub2.is_published = true
      LEFT JOIN brands b1 ON b1.id = p1.brand_id
      LEFT JOIN brands b2 ON b2.id = p2.brand_id
      LEFT JOIN smartphones s1 ON s1.product_id = p1.id
      LEFT JOIN smartphones s2 ON s2.product_id = p2.id
      LEFT JOIN laptop l1 ON l1.product_id = p1.id
      LEFT JOIN laptop l2 ON l2.product_id = p2.id
      LEFT JOIN tvs t1 ON t1.product_id = p1.id
      LEFT JOIN tvs t2 ON t2.product_id = p2.id
      LEFT JOIN networking n1 ON n1.product_id = p1.id
      LEFT JOIN networking n2 ON n2.product_id = p2.id
      WHERE p1.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
        AND p2.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
      ORDER BY pair_counts.compare_count DESC, pair_counts.last_compared_at DESC
      LIMIT $2
      `,
      [safeDays, safeLimit],
    );

    (pairResult.rows || []).forEach((row) => mergeRow(row, "legacy_pair"));
  } catch (err) {
    console.warn("Admin user compare pair groups skipped:", err?.message);
  }

  const comparePageResult = await db.query(`
    SELECT *
    FROM compare_pages
    ORDER BY updated_at DESC NULLS LAST, id DESC
  `);
  const pagesByKey = new Map();
  for (const row of comparePageResult.rows || []) {
    const page = normalizeComparePageRecord(row);
    const key = normalizeCompareGroupKey(
      (Array.isArray(page?.items) ? page.items : []).map(
        (item) => item?.product_id ?? item?.productId ?? item?.id,
      ),
    );
    if (key && !pagesByKey.has(key)) pagesByKey.set(key, page);
  }

  return Array.from(rowsByKey.values())
    .map((row) => {
      const comparePage = pagesByKey.get(row.group_key) || null;
      const launchDates = row.products
        .map((product) => product.launch_date)
        .filter(Boolean)
        .sort();
      const lastComparedAt = maxIsoTimestamp(
        row.last_compared_at,
        comparePage?.last_compared_at,
      );
      const deadAt = addDaysToIso(lastComparedAt, PUBLIC_COMPARE_WINDOW_DAYS);
      const fallbackPage =
        row.products.length >= 2 &&
        row.products.length <= 3 &&
        row.products.every(
          (product) => product.product_type === row.products[0]?.product_type,
        )
          ? buildComparePagePayload(row.products, {
              manualCompareCount: row.compare_count,
              lastComparedAt,
              updatedAt: lastComparedAt,
            })
          : null;
      const isManual =
        String(comparePage?.source || "").toLowerCase() === "manual";

      return {
        ...row,
        compare_count_180d: row.compare_count,
        unique_users_180d: row.unique_users,
        first_compared_at: row.first_compared_at || null,
        last_compared_at: lastComparedAt || null,
        alive_date: lastComparedAt || null,
        alive_at: lastComparedAt || null,
        dead_date: deadAt,
        dead_at: deadAt,
        is_alive:
          isManual ||
          (deadAt ? new Date(deadAt).getTime() >= Date.now() : false),
        oldest_launch_date: launchDates[0] || null,
        latest_launch_date: launchDates[launchDates.length - 1] || null,
        launch_date: launchDates[launchDates.length - 1] || null,
        compare_page_id: comparePage?.id || null,
        compare_page: comparePage,
        route_path:
          comparePage?.route_path || fallbackPage?.route_path || "/compare",
        title:
          comparePage?.title || fallbackPage?.title || "User compare group",
        status: comparePage?.status || "not_created",
        source: comparePage?.source || "user_compare",
        can_create_page: Boolean(
          fallbackPage &&
          row.products.every(
            (product) => product.product_type === "smartphone",
          ),
        ),
        analytics_window_days: safeDays,
        public_window_days: PUBLIC_COMPARE_WINDOW_DAYS,
      };
    })
    .sort((left, right) => {
      const compareDiff =
        (Number(right.compare_count) || 0) - (Number(left.compare_count) || 0);
      if (compareDiff !== 0) return compareDiff;
      return (
        new Date(right.last_compared_at || 0).getTime() -
        new Date(left.last_compared_at || 0).getTime()
      );
    })
    .slice(0, safeLimit);
};

const buildComparePageFromBody = async (body = {}, existingPage = null) => {
  const existingItemIds = Array.isArray(existingPage?.items)
    ? existingPage.items
        .map((item) => Number(item?.product_id ?? item?.productId ?? item?.id))
        .filter((value) => Number.isInteger(value) && value > 0)
    : [];
  const productIds = extractComparePageProductIds(body);
  const selectedIds = productIds.length ? productIds : existingItemIds;
  const products = await fetchComparePageProductsByIds(selectedIds);

  if (products.length < 2) return null;

  const nowIso = new Date().toISOString();
  return buildComparePagePayload(products, {
    id: existingPage?.id ?? null,
    primaryProductId:
      body.primary_product_id ??
      body.primaryProductId ??
      existingPage?.primary_product_id ??
      null,
    segmentLabel:
      body.segment_label ||
      body.segmentLabel ||
      existingPage?.segment_label ||
      "",
    smartphoneTypeLabel:
      body.smartphone_type_label ||
      body.smartphoneTypeLabel ||
      existingPage?.smartphone_type_label ||
      "",
    slug: body.slug || existingPage?.slug || "",
    title: body.title || existingPage?.title || "",
    metaDescription:
      body.meta_description ||
      body.metaDescription ||
      existingPage?.meta_description ||
      "",
    status: body.status || existingPage?.status || "published",
    source: body.source || existingPage?.source || "manual",
    generationReason:
      body.generation_reason ||
      body.generationReason ||
      existingPage?.generation_reason ||
      "",
    systemScore:
      body.system_score ?? body.systemScore ?? existingPage?.system_score ?? 0,
    manualCompareCount:
      body.manual_compare_count ??
      body.manualCompareCount ??
      existingPage?.manual_compare_count ??
      0,
    lastComparedAt:
      body.last_compared_at ??
      body.lastComparedAt ??
      existingPage?.last_compared_at ??
      null,
    generatedAt:
      body.generated_at ??
      body.generatedAt ??
      existingPage?.generated_at ??
      nowIso,
    updatedAt: nowIso,
    publishedAt:
      body.published_at ??
      body.publishedAt ??
      existingPage?.published_at ??
      nowIso,
  });
};

const fetchComparePageSuggestionsForProduct = async ({
  productId,
  days = 180,
  limit = 5,
} = {}) => {
  const id = Number(productId);
  if (!Number.isInteger(id) || id <= 0) {
    return { existingPage: null, suggestions: [] };
  }

  const safeDays = Number.isFinite(Number(days))
    ? Math.min(365, Math.max(7, Math.floor(Number(days))))
    : 180;
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.min(10, Math.max(1, Math.floor(Number(limit))))
    : 5;

  const pairs = await db.query(
    `
    WITH pair_counts AS (
      SELECT
        CASE
          WHEN pc.product_id = $1 THEN pc.compared_with
          ELSE pc.product_id
        END AS other_product_id,
        COUNT(*)::int AS compare_count,
        MAX(pc.compared_at) AS last_compared_at
      FROM product_comparisons pc
      WHERE (pc.product_id = $1 OR pc.compared_with = $1)
        AND pc.compared_at >= now() - make_interval(days => $2::int)
      GROUP BY 1
    )
    SELECT other_product_id, compare_count, last_compared_at
    FROM pair_counts
    WHERE other_product_id IS NOT NULL
      AND other_product_id <> $1
    ORDER BY compare_count DESC, last_compared_at DESC, other_product_id DESC
    LIMIT $3
    `,
    [id, safeDays, safeLimit],
  );

  const candidateIds = (pairs.rows || [])
    .map((row) => Number(row.other_product_id))
    .filter((value) => Number.isInteger(value) && value > 0);
  const suggestions = await fetchComparePageProductsByIds(candidateIds);
  const existingPage = await fetchComparePageRecordByProductId(id);

  return { existingPage, suggestions };
};

app.get("/api/admin/compare-pages", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const limitRaw = Number(req.query?.limit ?? 100);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(500, Math.max(1, Math.floor(limitRaw)))
      : 100;

    const result = await db.query(
      `
      SELECT *
      FROM compare_pages
      ORDER BY updated_at DESC NULLS LAST, id DESC
      LIMIT $1
      `,
      [limit],
    );

    const pages = (result.rows || [])
      .map((row) => normalizeComparePageRecord(row))
      .filter(Boolean);
    const hydratedPages = await hydrateComparePagesWithAnalytics(pages);

    return res.json({
      generated_at: new Date().toISOString(),
      pages: hydratedPages,
      data: hydratedPages,
    });
  } catch (err) {
    console.error("GET /api/admin/compare-pages error:", err);
    return res.status(500).json({ message: "Failed to load compare pages" });
  }
});

app.get("/api/admin/user-compares", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const daysRaw = Number(req.query?.days ?? COMPARE_DATA_RETENTION_DAYS);
    const limitRaw = Number(req.query?.limit ?? 500);
    const days = Number.isFinite(daysRaw)
      ? Math.min(COMPARE_DATA_RETENTION_DAYS, Math.max(1, Math.floor(daysRaw)))
      : COMPARE_DATA_RETENTION_DAYS;
    const limit = Number.isFinite(limitRaw)
      ? Math.min(1000, Math.max(1, Math.floor(limitRaw)))
      : 500;
    const groups = await fetchAdminUserCompareGroups({ days, limit });

    return res.json({
      generated_at: new Date().toISOString(),
      days,
      limit,
      retention_days: COMPARE_DATA_RETENTION_DAYS,
      public_window_days: PUBLIC_COMPARE_WINDOW_DAYS,
      user_compares: groups,
      rows: groups,
      data: groups,
    });
  } catch (err) {
    console.error("GET /api/admin/user-compares error:", err);
    return res.status(500).json({ message: "Failed to load user compares" });
  }
});

app.get(
  "/api/admin/compare-pages/suggestions/:productId",
  authenticate,
  async (req, res) => {
    try {
      if (!requireAdminAccess(req, res)) return;

      const productId = Number(req.params.productId);
      if (!Number.isInteger(productId) || productId <= 0) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      const daysRaw = Number(req.query?.days ?? 180);
      const limitRaw = Number(req.query?.limit ?? 5);
      const { existingPage, suggestions } =
        await fetchComparePageSuggestionsForProduct({
          productId,
          days: Number.isFinite(daysRaw) ? daysRaw : 180,
          limit: Number.isFinite(limitRaw) ? limitRaw : 5,
        });
      const hydratedExistingPage = existingPage
        ? (await hydrateComparePagesWithAnalytics([existingPage]))[0]
        : null;

      return res.json({
        product_id: productId,
        existing_page: hydratedExistingPage,
        suggestions,
        generated_at: new Date().toISOString(),
      });
    } catch (err) {
      console.error(
        "GET /api/admin/compare-pages/suggestions/:productId error:",
        err,
      );
      return res
        .status(500)
        .json({ message: "Failed to load compare page suggestions" });
    }
  },
);

app.post(
  "/api/admin/compare-pages/auto-sync",
  authenticate,
  async (req, res) => {
    try {
      if (!requireAdminAccess(req, res)) return;

      const daysRaw = Number(req.body?.days ?? req.query?.days ?? 180);
      const limitRaw = Number(req.body?.limit ?? req.query?.limit ?? 100);
      const result = await syncAutomaticComparePages({
        days: Number.isFinite(daysRaw) ? daysRaw : 180,
        limit: Number.isFinite(limitRaw) ? limitRaw : 100,
      });

      return res.json({
        ok: true,
        generated_at: new Date().toISOString(),
        result,
      });
    } catch (err) {
      console.error("POST /api/admin/compare-pages/auto-sync error:", err);
      return res.status(500).json({ message: "Failed to sync compare pages" });
    }
  },
);

app.get("/api/admin/compare-pages/:id", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const page = await fetchComparePageRecordById(req.params.id);
    if (!page) {
      return res.status(404).json({ message: "Compare page not found" });
    }

    const hydratedPage = (await hydrateComparePagesWithAnalytics([page]))[0];

    return res.json({
      page: hydratedPage,
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error("GET /api/admin/compare-pages/:id error:", err);
    return res.status(500).json({ message: "Failed to load compare page" });
  }
});

app.post("/api/admin/compare-pages", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const page = await buildComparePageFromBody(req.body || {});
    if (!page) {
      return res
        .status(400)
        .json({ message: "Select at least 2 published smartphones." });
    }

    const savedPage = await persistComparePageRecord(page);
    if (!savedPage) {
      return res.status(500).json({ message: "Failed to save compare page" });
    }

    const hydratedPage = (
      await hydrateComparePagesWithAnalytics([savedPage])
    )[0];

    return res.status(201).json({
      page: hydratedPage,
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error("POST /api/admin/compare-pages error:", err);
    return res.status(500).json({ message: "Failed to save compare page" });
  }
});

app.put("/api/admin/compare-pages/:id", authenticate, async (req, res) => {
  try {
    if (!requireAdminAccess(req, res)) return;

    const existingPage = await fetchComparePageRecordById(req.params.id);
    if (!existingPage) {
      return res.status(404).json({ message: "Compare page not found" });
    }

    const page = await buildComparePageFromBody(req.body || {}, existingPage);
    if (!page) {
      return res
        .status(400)
        .json({ message: "Select at least 2 published smartphones." });
    }

    page.id = existingPage.id;
    const savedPage = await persistComparePageRecord(page);
    if (!savedPage) {
      return res.status(500).json({ message: "Failed to update compare page" });
    }

    const hydratedPage = (
      await hydrateComparePagesWithAnalytics([savedPage])
    )[0];

    return res.json({
      page: hydratedPage,
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error("PUT /api/admin/compare-pages/:id error:", err);
    return res.status(500).json({ message: "Failed to update compare page" });
  }
});

app.delete(
  "/api/admin/compare-pages/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
    try {
      if (!requireAdminAccess(req, res)) return;

      const pageId = Number(req.params.id);
      if (!Number.isInteger(pageId) || pageId <= 0) {
        return res.status(400).json({ message: "Invalid compare page id" });
      }

      const result = await db.query(
        `DELETE FROM compare_pages WHERE id = $1 RETURNING id`,
        [pageId],
      );

      if (!result.rows?.length) {
        return res.status(404).json({ message: "Compare page not found" });
      }

      return res.json({
        ok: true,
        deleted_id: pageId,
        generated_at: new Date().toISOString(),
      });
    } catch (err) {
      console.error("DELETE /api/admin/compare-pages/:id error:", err);
      return res.status(500).json({ message: "Failed to delete compare page" });
    }
  },
);

app.get("/api/public/compare-pages/routes", async (req, res) => {
  try {
    const limitRaw = Number(req.query?.limit ?? 100);
    const daysRaw = Number(req.query?.days ?? 180);
    const limit = Number.isFinite(limitRaw)
      ? Math.min(300, Math.max(1, Math.floor(limitRaw)))
      : 100;
    const days = Number.isFinite(daysRaw)
      ? Math.min(PUBLIC_COMPARE_WINDOW_DAYS, Math.max(7, Math.floor(daysRaw)))
      : PUBLIC_COMPARE_WINDOW_DAYS;

    const result = await db.query(
      `
      WITH pair_counts AS (
        SELECT
          LEAST(pc.product_id, pc.compared_with) AS left_product_id,
          GREATEST(pc.product_id, pc.compared_with) AS right_product_id,
          COUNT(*)::int AS compare_count,
          MAX(pc.compared_at) AS last_compared_at
        FROM product_comparisons pc
        WHERE pc.compared_at >= now() - make_interval(days => $1::int)
        GROUP BY 1, 2
      )
      SELECT
        pair_counts.left_product_id,
        pair_counts.right_product_id,
        pair_counts.compare_count,
        pair_counts.last_compared_at,
        p1.name AS left_product_name,
        p1.product_type AS left_product_type,
        b1.name AS left_brand_name,
        p2.name AS right_product_name,
        p2.product_type AS right_product_type,
        b2.name AS right_brand_name,
        COALESCE(
          (
            SELECT MIN(sp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p1.id
              AND sp.price IS NOT NULL
          ),
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p1.id
              AND pv.base_price IS NOT NULL
          )
        ) AS left_best_price,
        COALESCE(
          (
            SELECT MIN(sp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p2.id
              AND sp.price IS NOT NULL
          ),
          (
            SELECT MIN(pv.base_price)::numeric
            FROM product_variants pv
            WHERE pv.product_id = p2.id
              AND pv.base_price IS NOT NULL
          )
        ) AS right_best_price,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p1.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS left_image_url,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p2.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS right_image_url
      FROM pair_counts
      INNER JOIN products p1
        ON p1.id = pair_counts.left_product_id
      INNER JOIN products p2
        ON p2.id = pair_counts.right_product_id
      INNER JOIN product_publish pub1
        ON pub1.product_id = p1.id
       AND pub1.is_published = true
      INNER JOIN product_publish pub2
        ON pub2.product_id = p2.id
       AND pub2.is_published = true
      LEFT JOIN brands b1
        ON b1.id = p1.brand_id
      LEFT JOIN brands b2
        ON b2.id = p2.brand_id
      WHERE p1.product_type = 'smartphone'
        AND p2.product_type = 'smartphone'
      ORDER BY
        pair_counts.compare_count DESC,
        pair_counts.last_compared_at DESC,
        p1.id DESC,
        p2.id DESC
      LIMIT $2
      `,
      [days, limit],
    );

    const routes = (result.rows || [])
      .map((row) =>
        buildComparePagePayload(
          [
            {
              product_id: row.left_product_id,
              product_name: row.left_product_name,
              product_type: row.left_product_type,
              brand_name: row.left_brand_name,
              best_price: row.left_best_price,
              image_url: row.left_image_url,
            },
            {
              product_id: row.right_product_id,
              product_name: row.right_product_name,
              product_type: row.right_product_type,
              brand_name: row.right_brand_name,
              best_price: row.right_best_price,
              image_url: row.right_image_url,
            },
          ],
          {
            manualCompareCount: row.compare_count,
            lastComparedAt: row.last_compared_at,
            generatedAt: new Date().toISOString(),
            updatedAt: row.last_compared_at,
          },
        ),
      )
      .filter(Boolean)
      .map((page) => ({
        slug: page.slug,
        route_path: page.route_path,
        title: page.title,
        meta_description: page.meta_description,
        segment_label: page.segment_label,
        compare_count: page.manual_compare_count,
        updated_at: page.updated_at,
      }));

    return res.json({
      generated_at: new Date().toISOString(),
      days,
      limit,
      routes,
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
    const slug = String(req.query?.slug || "")
      .trim()
      .replace(/^\/+|\/+$/g, "");
    const slugParts = parseComparePageSlug(slug);

    if (slugParts.length < 2) {
      return res.json({ page: null });
    }

    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand_name,
        regexp_replace(
          regexp_replace(lower(coalesce(p.name, '')), '[^a-z0-9]+', '-', 'g'),
          '(^-|-$)',
          '',
          'g'
        ) AS slug,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image_url,
        COALESCE(
          (
            SELECT MIN(sp.price)::numeric
            FROM product_variants pv
            INNER JOIN variant_store_prices sp
              ON sp.variant_id = pv.id
            WHERE pv.product_id = p.id
              AND sp.price IS NOT NULL
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
      WHERE p.product_type IN ('smartphone', 'laptop', 'tv', 'networking')
        AND regexp_replace(
          regexp_replace(lower(coalesce(p.name, '')), '[^a-z0-9]+', '-', 'g'),
          '(^-|-$)',
          '',
          'g'
        ) = ANY($1::text[])
      ORDER BY p.id DESC
      `,
      [Array.from(new Set(slugParts))],
    );

    const bySlug = new Map();
    for (const row of result.rows || []) {
      const rowSlug = String(row?.slug || "").trim();
      if (!rowSlug || bySlug.has(rowSlug)) continue;
      bySlug.set(rowSlug, row);
    }

    const orderedRows = slugParts
      .map((part) => bySlug.get(part))
      .filter(Boolean);
    if (orderedRows.length < 2) {
      return res.json({ page: null });
    }

    const firstType = normalizePopularityProductType(
      orderedRows[0]?.product_type,
    );
    if (
      !firstType ||
      orderedRows.some(
        (row) =>
          normalizePopularityProductType(row?.product_type) !== firstType,
      )
    ) {
      return res.json({ page: null });
    }

    let compareCount = 0;
    let lastComparedAt = null;
    if (orderedRows.length === 2) {
      const leftId = Number(orderedRows[0]?.product_id);
      const rightId = Number(orderedRows[1]?.product_id);
      if (
        Number.isInteger(leftId) &&
        leftId > 0 &&
        Number.isInteger(rightId) &&
        rightId > 0
      ) {
        const compareRes = await db.query(
          `
          SELECT
            COUNT(*)::int AS compare_count,
            MAX(compared_at) AS last_compared_at
          FROM product_comparisons
          WHERE LEAST(product_id, compared_with) = LEAST($1, $2)
            AND GREATEST(product_id, compared_with) = GREATEST($1, $2)
            AND compared_at >= now() - make_interval(days => $3::int)
          `,
          [leftId, rightId, PUBLIC_COMPARE_WINDOW_DAYS],
        );
        compareCount = Number(compareRes.rows?.[0]?.compare_count) || 0;
        lastComparedAt = compareRes.rows?.[0]?.last_compared_at || null;
      }
    }

    const page = buildComparePagePayload(orderedRows, {
      slug: /-comparison$/i.test(slug) ? slug : "",
      manualCompareCount: compareCount,
      lastComparedAt,
      generatedAt: new Date().toISOString(),
      updatedAt: lastComparedAt,
    });

    return res.json({ page: page || null });
  } catch (err) {
    console.error("GET /api/public/compare-pages/resolve error:", err);
    return res.status(500).json({ message: "Failed to resolve compare page" });
  }
});

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

    const areCompetitorRowsStale = (items) => {
      if (!Array.isArray(items) || !items.length) return true;
      return items.some((row) => {
        const computedAt = new Date(row?.computed_at || 0).getTime();
        return (
          !Number.isFinite(computedAt) ||
          Date.now() - computedAt > COMPETITOR_ANALYSIS_MAX_AGE_MS
        );
      });
    };

    let rows = await fetchRows();
    if (!rows.length || areCompetitorRowsStale(rows)) {
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

    const publicCompetitors = competitors.map((item) =>
      toPublicSmartphoneResponse(item),
    );

    return res.json({
      product_id: id,
      product_name: product.name,
      generated_at:
        rows[0]?.computed_at != null
          ? new Date(rows[0].computed_at).toISOString()
          : new Date().toISOString(),
      top_competitor: publicCompetitors[0] || null,
      other_competitors: publicCompetitors.slice(1),
      competitors: publicCompetitors,
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

    const publicResponse =
      product.product_type === "smartphone"
        ? toPublicSmartphoneResponse(scoredResponse)
        : scoredResponse;

    res.json(publicResponse);
  } catch (err) {
    console.error("GET /api/public/product/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

async function runGlobalSearch(queryText, { publishedOnly = true } = {}) {
  const q = (queryText || "").trim();
  if (!q) return [];

  const term = `%${q}%`;

  const publishJoin = publishedOnly
    ? `
       INNER JOIN product_publish pub
         ON pub.product_id = p.id
        AND pub.is_published = true
      `
    : "";

  const brandExistsClause = publishedOnly
    ? `
         AND EXISTS (
           SELECT 1
           FROM products p
           INNER JOIN product_publish pub
             ON pub.product_id = p.id
            AND pub.is_published = true
           WHERE p.brand_id = b.id
         )
      `
    : `
         AND EXISTS (
           SELECT 1
           FROM products p
           WHERE p.brand_id = b.id
         )
      `;

  // Search products by name and brand with image
  const products = await db.query(
    `SELECT DISTINCT
      p.id,
      p.name,
      p.product_type,
      b.name AS brand_name,
      (SELECT image_url FROM product_images WHERE product_id = p.id AND position = 1 LIMIT 1) AS image_url
     FROM products p
     ${publishJoin}
     LEFT JOIN brands b ON b.id = p.brand_id
     WHERE p.name ILIKE $1
        OR b.name ILIKE $1
     ORDER BY p.name ASC
     LIMIT 10`,
    [term],
  );

  // Search brands only
  const brands = await db.query(
    `SELECT b.id, b.name
     FROM brands b
     WHERE b.name ILIKE $1
     ${brandExistsClause}
     ORDER BY b.name ASC
     LIMIT 6`,
    [term],
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

  // Add brands to results (avoid duplicates)
  for (const b of brands.rows) {
    const brandExists = results.some(
      (item) => item.type === "brand" && item.name === b.name,
    );
    const productExists = results.some(
      (item) => item.type === "product" && item.brand_name === b.name,
    );

    if (!brandExists && !productExists) {
      results.push({
        type: "brand",
        id: b.id,
        name: b.name,
      });
    }
  }

  return results;
}

// Public search: published products only
app.get("/api/search", async (req, res) => {
  try {
    const results = await runGlobalSearch(req.query.q, { publishedOnly: true });
    res.json({ results });
  } catch (err) {
    console.error("GET /api/search error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin search: includes published + unpublished products
app.get("/api/search/admin", authenticate, async (req, res) => {
  try {
    const results = await runGlobalSearch(req.query.q, {
      publishedOnly: false,
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
app.delete(
  "/api/ram-storage-config/:id",
  authenticate,
  dataDeletePinVerifyLimiter,
  requireDataDeleteApproval,
  async (req, res) => {
    try {
      const id = req.params.id;
      await db.query(`DELETE FROM ram_storage_long WHERE id = $1`, [id]);
      return res.json({ message: "Deleted" });
    } catch (err) {
      console.error("Delete ram-storage-config error:", err);
      return res.status(500).json({ message: "Internal server error" });
    }
  },
);

const importSmartphonesRouter = require("./routes/importSmartphones");
const importLaptopsRouter = require("./routes/importLaptop");
const smartphonesReqRouter = require("./routes/smartphonesReq");
app.use("/api/import", authenticate, importSmartphonesRouter);
app.use("/api/import", authenticate, importLaptopsRouter);
app.use("/api/smartphones", authenticate, smartphonesReqRouter);

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
        } catch (err) {
          console.error("Competitor analysis recompute failed:", err);
        }
      };

      void run();
      const timer = setInterval(run, intervalMs);
      if (typeof timer.unref === "function") timer.unref();
      console.log("Competitor analysis cron enabled:", { intervalMs });
    }
  } catch (err) {
    console.error("Migrations failed:", err);
    process.exit(1);
  }

  app.listen(PORT, async () => {
    console.log(`🚀 Server running at http://localhost:${PORT}`);
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
