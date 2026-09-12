"use strict";

const DEFAULT_PREFS = {
  performance: 40,
  battery: 25,
  display: 15,
  software: 10,
  camera: 5,
  value: 5,
};

const PRIMARY_USE_PREFS = {
  camera: {
    camera: 30,
    display: 15,
    performance: 10,
    battery: 15,
    software: 10,
    value: 5,
  },
  gaming: {
    performance: 30,
    battery: 20,
    display: 20,
    software: 5,
    camera: 5,
    value: 5,
  },
  battery: {
    battery: 35,
    performance: 15,
    display: 10,
    software: 10,
    camera: 5,
    value: 10,
  },
  performance: {
    performance: 35,
    battery: 20,
    display: 15,
    software: 10,
    camera: 5,
    value: 5,
  },
  entertainment: {
    display: 25,
    battery: 20,
    performance: 15,
    camera: 10,
    software: 10,
    value: 10,
  },
  work: {
    performance: 20,
    battery: 20,
    software: 20,
    display: 15,
    camera: 5,
    value: 10,
  },
  ai: {
    performance: 25,
    software: 20,
    battery: 15,
    display: 10,
    camera: 5,
    value: 10,
  },
  balanced: DEFAULT_PREFS,
};

const PRIORITY_KEYS = {
  camera: "camera",
  performance: "performance",
  battery: "battery",
  display: "display",
  gaming: "performance",
  ai: "software",
  software: "software",
  charging: "battery",
  value: "value",
};

function buildFinderPreferences(profile = {}) {
  const base =
    PRIMARY_USE_PREFS[profile.primaryUse] || PRIMARY_USE_PREFS.balanced;
  const preferences = { ...base };

  for (const priority of Array.isArray(profile.priorities)
    ? profile.priorities.slice(0, 3)
    : []) {
    const key = PRIORITY_KEYS[priority];
    if (key) preferences[key] += 10;
  }

  const total = Object.values(preferences).reduce(
    (sum, value) => sum + value,
    0,
  );
  return Object.fromEntries(
    Object.entries(preferences).map(([key, value]) => [
      key,
      Number(((value / total) * 100).toFixed(2)),
    ]),
  );
}

const normalizeNumber = (value, fallback = 0) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
};

const getRamValue = (phone) => {
  const candidate =
    phone?.ramOptions ??
    phone?.ram ??
    phone?.specs?.ram ??
    phone?.performance?.ram ??
    [];

  if (Array.isArray(candidate)) {
    const values = candidate
      .map((value) => normalizeNumber(String(value).replace(/[^\d.]/g, ""), 0))
      .filter((value) => value > 0);
    if (values.length) return Math.max(...values);
  }

  const single = normalizeNumber(
    String(candidate ?? "").replace(/[^\d.]/g, ""),
    0,
  );
  return single > 0 ? single : 0;
};

const getBatteryValue = (phone) => {
  const value =
    phone?.batteryMah ??
    phone?.battery ??
    phone?.specs?.battery ??
    phone?.battery?.mah ??
    phone?.battery?.capacity ??
    0;

  return normalizeNumber(String(value).replace(/[^\d.]/g, ""), 0);
};

const getPriceValue = (phone) => {
  const direct =
    phone?.numericPrice ??
    phone?.price ??
    phone?.starting_price ??
    phone?.bestPriceValue ??
    phone?.best_price ??
    0;

  if (typeof direct === "number" && Number.isFinite(direct)) return direct;
  if (typeof direct === "string")
    return normalizeNumber(direct.replace(/[^\d.]/g, ""), 0);

  return 0;
};

function applyPhoneFilters(phones = [], filters = {}) {
  if (!Array.isArray(phones)) return [];

  const maxPrice = normalizeNumber(filters.maxPrice, Number.POSITIVE_INFINITY);
  const minPrice = normalizeNumber(filters.minPrice, 0);
  const minRam = normalizeNumber(filters.minRam, 0);
  const minBattery = normalizeNumber(filters.minBattery, 0);
  const fiveGRequired = Boolean(filters.fiveG);

  return phones.filter((phone) => {
    const price = getPriceValue(phone);
    if (price < minPrice) return false;
    if (Number.isFinite(maxPrice) && price > maxPrice) return false;

    const ram = getRamValue(phone);
    if (ram < minRam) return false;

    const battery = getBatteryValue(phone);
    if (battery < minBattery) return false;

    if (fiveGRequired) {
      const isFiveG = Boolean(
        phone?.network5G ??
        phone?.has5G ??
        phone?.network?.includes?.("5G") ??
        phone?.connectivity?.includes?.("5G") ??
        phone?.specs?.network?.includes?.("5G") ??
        false,
      );
      if (!isFiveG) return false;
    }

    return true;
  });
}

function calculateMatchScore(phone = {}, preferences = DEFAULT_PREFS) {
  const effectivePrefs = { ...DEFAULT_PREFS, ...preferences };
  const totalWeight = Object.values(effectivePrefs).reduce(
    (sum, value) => sum + Number(value || 0),
    0,
  );

  const safe = (value) => Math.max(0, Math.min(100, normalizeNumber(value, 0)));

  const performance = safe(phone.performanceScore ?? phone.performance ?? 0);
  const battery = safe(phone.batteryScore ?? phone.battery ?? 0);
  const display = safe(phone.displayScore ?? phone.display ?? 0);
  const software = safe(phone.softwareScore ?? phone.software ?? 0);
  const camera = safe(phone.cameraScore ?? phone.camera ?? 0);
  const value = safe(phone.valueScore ?? phone.value ?? 0);

  const weighted =
    performance * (effectivePrefs.performance || 0) +
    battery * (effectivePrefs.battery || 0) +
    display * (effectivePrefs.display || 0) +
    software * (effectivePrefs.software || 0) +
    camera * (effectivePrefs.camera || 0) +
    value * (effectivePrefs.value || 0);

  if (totalWeight <= 0) return 0;
  return Number((weighted / totalWeight).toFixed(2));
}

function rankPhones(phones = []) {
  return [...phones]
    .map((phone) => ({
      ...phone,
      matchScore: normalizeNumber(phone.matchScore, calculateMatchScore(phone)),
    }))
    .sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));
}

function buildRecommendationReasons(phone = {}) {
  const reasons = [];

  const price = getPriceValue(phone);
  if (price > 0 && Number.isFinite(price)) {
    if (price <= 30000) reasons.push("Within your budget");
    else reasons.push("Budget is close to your preference");
  }

  const ram = getRamValue(phone);
  if (ram >= 8) reasons.push("8GB+ RAM");
  if (ram >= 12) reasons.push("12GB+ RAM");

  const battery = getBatteryValue(phone);
  if (battery >= 6000) reasons.push("6000mAh+ battery");
  if (battery >= 7000) reasons.push("7000mAh+ battery");

  const isFiveG = Boolean(
    phone?.network5G ??
    phone?.has5G ??
    phone?.network?.includes?.("5G") ??
    phone?.connectivity?.includes?.("5G") ??
    phone?.specs?.network?.includes?.("5G") ??
    false,
  );
  if (isFiveG) reasons.push("5G available");

  if ((phone.performanceScore ?? phone.performance ?? 0) >= 90) {
    reasons.push("Strong performance");
  }

  if (reasons.length === 0) reasons.push("Matches the requested profile");

  return reasons;
}

module.exports = {
  DEFAULT_PREFS,
  buildFinderPreferences,
  applyPhoneFilters,
  calculateMatchScore,
  rankPhones,
  buildRecommendationReasons,
};
