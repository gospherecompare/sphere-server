const WEIGHT_KEYS = ["performance", "display", "camera", "battery", "priceValue"];

const DEFAULT_COMPARE_WEIGHTS = Object.freeze({
  performance: 0.36,
  display: 0.2,
  camera: 0.2,
  battery: 0.14,
  priceValue: 0.1,
});

const DEFAULT_CHIPSET_RULES = Object.freeze([
  { keyword: "snapdragon 8 elite", score: 100 },
  { keyword: "snapdragon 8 gen 4", score: 98 },
  { keyword: "dimensity 9400", score: 97 },
  { keyword: "a18 pro", score: 98 },
  { keyword: "apple a18", score: 98 },
  { keyword: "snapdragon 8 gen 3", score: 95 },
  { keyword: "dimensity 9300", score: 92 },
  { keyword: "a17 pro", score: 98 },
  { keyword: "snapdragon 8 gen 2", score: 89 },
  { keyword: "dimensity 9200", score: 89 },
  { keyword: "apple a16", score: 89 },
  { keyword: "snapdragon 7 gen 3", score: 75 },
  { keyword: "dimensity 8300", score: 75 },
  { keyword: "snapdragon 7", score: 72 },
  { keyword: "dimensity 8", score: 72 },
  { keyword: "tensor g3", score: 74 },
  { keyword: "tensor g2", score: 72 },
  { keyword: "snapdragon 6", score: 62 },
  { keyword: "dimensity 7", score: 62 },
  { keyword: "exynos 13", score: 62 },
  { keyword: "snapdragon 4", score: 50 },
  { keyword: "helio", score: 50 },
  { keyword: "unisoc", score: 50 },
  { keyword: "exynos 8", score: 50 },
]);

const LAPTOP_COMPARE_WEIGHTS = Object.freeze({
  performance: 0.3,
  display: 0.22,
  battery: 0.16,
  memory: 0.12,
  portability: 0.08,
  connectivity: 0.06,
  priceValue: 0.06,
});

const TV_COMPARE_WEIGHTS = Object.freeze({
  display: 0.34,
  smart: 0.18,
  audio: 0.14,
  gaming: 0.14,
  connectivity: 0.12,
  priceValue: 0.08,
});

const NETWORKING_COMPARE_WEIGHTS = Object.freeze({
  performance: 0.32,
  coverage: 0.2,
  ports: 0.16,
  features: 0.14,
  security: 0.1,
  priceValue: 0.08,
});

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

const roundOne = (value) => Math.round((value + Number.EPSILON) * 10) / 10;

const roundTwo = (value) => Math.round((value + Number.EPSILON) * 100) / 100;

const toFiniteNumber = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

const toObject = (value) => {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value === "string") {
    const raw = value.trim();
    if (!raw) return {};
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed
        : {};
    } catch (_err) {
      return {};
    }
  }
  return {};
};

const toArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
};

const normalizeProductType = (value) => {
  const normalized = normalizeText(value);
  if (normalized === "smartphone") return "smartphone";
  if (normalized === "laptop") return "laptop";
  if (normalized === "tv") return "tv";
  if (normalized === "networking") return "networking";
  return normalized || "unknown";
};

const cloneDefaultChipsetRules = () =>
  DEFAULT_CHIPSET_RULES.map((rule) => ({ ...rule }));

const normalizeWeights = (input) => {
  const source = input && typeof input === "object" ? input : {};
  const raw = {};

  for (const key of WEIGHT_KEYS) {
    const value = toFiniteNumber(
      source[key] ?? (key === "priceValue" ? source.price_value : null),
    );
    raw[key] = value != null && value >= 0 ? value : DEFAULT_COMPARE_WEIGHTS[key];
  }

  let total = WEIGHT_KEYS.reduce((acc, key) => acc + raw[key], 0);
  if (total > 1.5) {
    for (const key of WEIGHT_KEYS) raw[key] = raw[key] / 100;
    total = WEIGHT_KEYS.reduce((acc, key) => acc + raw[key], 0);
  }

  if (!Number.isFinite(total) || total <= 0) {
    return { ...DEFAULT_COMPARE_WEIGHTS };
  }

  const normalized = {};
  for (const key of WEIGHT_KEYS) {
    normalized[key] = clamp(raw[key] / total, 0, 1);
  }

  const sum = WEIGHT_KEYS.reduce((acc, key) => acc + normalized[key], 0);
  if (sum > 0 && sum !== 1) {
    const adjustKey = WEIGHT_KEYS[0];
    normalized[adjustKey] = clamp(normalized[adjustKey] + (1 - sum), 0, 1);
  }

  return normalized;
};

const normalizeChipsetRules = (input) => {
  const source = Array.isArray(input) ? input : [];
  const seen = new Set();
  const rules = [];

  for (const row of source) {
    const keywordRaw = row?.keyword ?? row?.match ?? row?.pattern ?? "";
    const keyword = normalizeText(keywordRaw).slice(0, 120);
    if (!keyword || seen.has(keyword)) continue;

    const scoreRaw = toFiniteNumber(row?.score);
    const score = clamp(
      Math.round(scoreRaw == null ? 60 : Number(scoreRaw)),
      0,
      100,
    );

    seen.add(keyword);
    rules.push({ keyword, score });
  }

  if (!rules.length) return cloneDefaultChipsetRules();
  return rules.slice(0, 200);
};

const weightsToPercent = (weights) => ({
  performance: roundOne((weights.performance || 0) * 100),
  display: roundOne((weights.display || 0) * 100),
  camera: roundOne((weights.camera || 0) * 100),
  battery: roundOne((weights.battery || 0) * 100),
  priceValue: roundOne((weights.priceValue || 0) * 100),
});

const normalizeCompareScoreConfig = (raw) => {
  const source = raw && typeof raw === "object" ? raw : {};
  const weights = normalizeWeights(source.weights || source);
  const chipsetRules = normalizeChipsetRules(
    source.chipset_rules ?? source.chipsetRules ?? source.chipsets ?? [],
  );

  return {
    weights,
    chipsetRules,
  };
};

const collectTextFragments = (value, bucket = []) => {
  if (value == null) return bucket;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    const next = String(value).trim();
    if (next) bucket.push(next);
    return bucket;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectTextFragments(item, bucket));
    return bucket;
  }
  if (typeof value === "object") {
    Object.values(value).forEach((item) => collectTextFragments(item, bucket));
  }
  return bucket;
};

const buildTextBlob = (...values) =>
  normalizeText(
    values
      .flatMap((item) => collectTextFragments(item, []))
      .filter(Boolean)
      .join(" "),
  );

const collectNumbers = (value, bucket = []) => {
  if (value == null) return bucket;
  if (typeof value === "number" && Number.isFinite(value)) {
    bucket.push(value);
    return bucket;
  }
  if (typeof value === "string") {
    const matches = value.match(/-?\d+(?:\.\d+)?/g);
    if (matches) {
      matches.forEach((entry) => {
        const n = Number(entry);
        if (Number.isFinite(n)) bucket.push(n);
      });
    }
    return bucket;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectNumbers(item, bucket));
    return bucket;
  }
  if (typeof value === "object") {
    Object.values(value).forEach((item) => collectNumbers(item, bucket));
  }
  return bucket;
};

const readLargestNumber = (value, { min = null, max = null } = {}) => {
  const numbers = collectNumbers(value, []).filter((item) => {
    if (min != null && item < min) return false;
    if (max != null && item > max) return false;
    return true;
  });
  if (!numbers.length) return null;
  return Math.max(...numbers);
};

const readSmallestNumber = (value, { min = null, max = null } = {}) => {
  const numbers = collectNumbers(value, []).filter((item) => {
    if (min != null && item < min) return false;
    if (max != null && item > max) return false;
    return true;
  });
  if (!numbers.length) return null;
  return Math.min(...numbers);
};

const parseResolution = (value) => {
  const text = normalizeText(value);
  if (!text) return null;
  const match = text.match(/(\d{3,5})\s*[x*]\s*(\d{3,5})/i);
  if (!match) return null;
  const width = Number(match[1]);
  const height = Number(match[2]);
  if (!Number.isFinite(width) || !Number.isFinite(height)) return null;
  return { width, height, pixels: width * height };
};

const readSupportState = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "boolean") return value;
  const text = normalizeText(value);
  if (!text) return null;
  if (
    text === "no" ||
    text === "false" ||
    text === "0" ||
    text.includes("not supported") ||
    text.includes("unsupported") ||
    text.includes("not available") ||
    text.includes("none")
  ) {
    return false;
  }
  if (
    text === "yes" ||
    text === "true" ||
    text === "1" ||
    text.includes("supported") ||
    text.includes("available") ||
    text.includes("present")
  ) {
    return true;
  }
  return null;
};

const pickVariant = (variants, selection) => {
  const list = Array.isArray(variants) ? variants : [];
  if (!list.length) return null;

  const variantId = Number(selection?.variant_id ?? selection?.variantId);
  if (Number.isInteger(variantId) && variantId > 0) {
    const byId = list.find((variant) => Number(variant?.id) === variantId);
    if (byId) return byId;
  }

  const variantIndex = Number(selection?.variant_index ?? selection?.variantIndex);
  if (Number.isInteger(variantIndex) && variantIndex >= 0) {
    return list[variantIndex] || list[0];
  }

  return list[0];
};

const extractPrice = (device, variantSelection = {}) => {
  const productId = String(device?.product_id ?? device?.id ?? "");
  const selection = variantSelection[productId] || null;
  const variants = Array.isArray(device?.variants) ? device.variants : [];
  const selectedVariant = pickVariant(variants, selection);

  const selectedVariantPrice = toFiniteNumber(
    selectedVariant?.base_price ??
      selectedVariant?.price ??
      selectedVariant?.attributes?.base_price ??
      selectedVariant?.attributes?.price,
  );
  if (selectedVariantPrice != null && selectedVariantPrice > 0) {
    return selectedVariantPrice;
  }

  const minVariantPrice = variants.reduce((acc, variant) => {
    const candidate = toFiniteNumber(
      variant?.base_price ??
        variant?.price ??
        variant?.attributes?.base_price ??
        variant?.attributes?.price,
    );
    if (candidate == null || candidate <= 0) return acc;
    if (acc == null) return candidate;
    return Math.min(acc, candidate);
  }, null);

  if (minVariantPrice != null) return minVariantPrice;

  const devicePrice = toFiniteNumber(device?.min_price ?? device?.price);
  if (devicePrice != null && devicePrice > 0) return devicePrice;

  return null;
};

const extractProcessorText = (device) => {
  const performance = toObject(device?.performance);
  const cpu = toObject(device?.cpu);

  const candidates = [
    performance.processor,
    performance.chipset,
    performance.soc,
    performance.cpu,
    cpu.processor,
    cpu.chipset,
    cpu.model,
    cpu.name,
    device?.processor,
  ];

  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (text) return text;
  }

  return "";
};

const getSelectedVariantValue = (device, variantSelection = {}, keys = []) => {
  const productId = String(device?.product_id ?? device?.id ?? "");
  const selection = variantSelection[productId] || null;
  const selectedVariant = pickVariant(device?.variants, selection);
  if (!selectedVariant) return null;
  for (const key of keys) {
    const direct = selectedVariant?.[key];
    if (direct != null && direct !== "") return direct;
    const attr = selectedVariant?.attributes?.[key];
    if (attr != null && attr !== "") return attr;
  }
  return null;
};

const scoreChipset = (processorText, chipsetRules) => {
  const text = normalizeText(processorText);
  if (!text) return 45;

  for (const rule of chipsetRules) {
    if (!rule?.keyword) continue;
    if (text.includes(rule.keyword)) return clamp(Number(rule.score) || 0, 0, 100);
  }

  const snapdragonGenMatch = text.match(/snapdragon\s*([0-9])\s*gen\s*([0-9]+)/i);
  if (snapdragonGenMatch) {
    const series = Number(snapdragonGenMatch[1]);
    const gen = Number(snapdragonGenMatch[2]);
    return clamp(50 + series * 8 + gen * 2, 52, 96);
  }

  const dimensityMatch = text.match(/dimensity\s*([0-9]{4})/i);
  if (dimensityMatch) {
    const model = Number(dimensityMatch[1]);
    if (model >= 9400) return 97;
    if (model >= 9300) return 92;
    if (model >= 8300) return 78;
    if (model >= 8200) return 74;
    if (model >= 7300) return 66;
    return 58;
  }

  const appleMatch = text.match(/apple\s*a([0-9]{2})|a([0-9]{2})\s*(pro|bionic)?/i);
  if (appleMatch) {
    const chipNum = Number(appleMatch[1] || appleMatch[2]);
    return clamp(74 + (chipNum - 14) * 4, 70, 99);
  }

  if (text.includes("tensor")) return 74;
  if (text.includes("exynos")) return 68;
  if (text.includes("intel core ultra 9")) return 95;
  if (text.includes("intel core ultra 7")) return 88;
  if (text.includes("intel core ultra 5")) return 80;
  if (text.includes("intel core i9")) return 91;
  if (text.includes("intel core i7")) return 83;
  if (text.includes("intel core i5")) return 74;
  if (text.includes("intel core i3")) return 62;
  if (text.includes("ryzen 9")) return 92;
  if (text.includes("ryzen 7")) return 84;
  if (text.includes("ryzen 5")) return 76;
  if (text.includes("ryzen 3")) return 64;
  if (text.includes("m4")) return 97;
  if (text.includes("m3")) return 94;
  if (text.includes("m2")) return 90;
  if (text.includes("m1")) return 86;

  return 60;
};

const extractRefreshRate = (display) => {
  const source = toObject(display);
  const candidates = [
    source.refresh_rate,
    source.refreshRate,
    source.max_refresh_rate,
    source.screen_refresh_rate,
    source.frame_rate,
    source.refresh,
  ];

  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }

  const match = buildTextBlob(source).match(/(\d+(?:\.\d+)?)\s*hz/i);
  if (!match) return null;
  const refresh = Number(match[1]);
  return Number.isFinite(refresh) ? refresh : null;
};

const scoreRefreshRate = (refreshRate) => {
  if (refreshRate == null) return 28;
  const normalized = clamp(refreshRate, 60, 240);
  return Math.round(20 + ((normalized - 60) / (240 - 60)) * 40);
};

const detectPanelScore = (display) => {
  const source = toObject(display);
  const text = normalizeText(
    source.panel_type || source.panel || source.type || source.technology,
  );
  if (!text) return { score: 20, label: "Unknown" };
  if (text.includes("ltpo")) return { score: 40, label: "LTPO AMOLED" };
  if (text.includes("amoled")) return { score: 34, label: "AMOLED" };
  if (text.includes("oled")) return { score: 32, label: "OLED" };
  if (text.includes("mini led") || text.includes("mini-led")) {
    return { score: 33, label: "Mini LED" };
  }
  if (text.includes("qled")) return { score: 31, label: "QLED" };
  if (text.includes("ips") || text.includes("lcd")) return { score: 24, label: "IPS LCD" };
  if (text.includes("tft")) return { score: 16, label: "TFT" };
  if (text.includes("va")) return { score: 27, label: "VA" };
  return {
    score: 22,
    label: String(source.panel_type || source.panel || source.type || "Unknown"),
  };
};

const extractPeakBrightness = (display) => {
  const source = toObject(display);
  const candidates = [
    source.peak_brightness,
    source.peak_brightness_nits,
    source.brightness,
    source.brightness_nits,
    source.typical_brightness,
    source.hbm,
  ];
  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }
  return readLargestNumber(source, { min: 250, max: 10000 });
};

const scoreBrightness = (brightnessNits) => {
  if (brightnessNits == null) return null;
  if (brightnessNits <= 600) return 40;
  if (brightnessNits <= 1000) return 55;
  if (brightnessNits <= 1400) return 68;
  if (brightnessNits <= 2000) return 78;
  if (brightnessNits <= 3000) return 88;
  if (brightnessNits <= 4500) return 94;
  return 98;
};

const scoreResolution = (resolutionValue) => {
  const parsed = parseResolution(resolutionValue);
  if (!parsed) return null;
  const pixels = parsed.pixels;
  if (pixels >= 7680 * 4320) return 98;
  if (pixels >= 3840 * 2160) return 92;
  if (pixels >= 2880 * 1800) return 88;
  if (pixels >= 2560 * 1440) return 84;
  if (pixels >= 2400 * 1080) return 76;
  if (pixels >= 1920 * 1080) return 68;
  if (pixels >= 1600 * 900) return 58;
  return 48;
};

const scoreRamGb = (ramGb) => {
  if (ramGb == null) return null;
  if (ramGb <= 4) return 38;
  if (ramGb <= 6) return 52;
  if (ramGb <= 8) return 67;
  if (ramGb <= 12) return 84;
  if (ramGb <= 16) return 92;
  if (ramGb <= 24) return 96;
  return 99;
};

const scoreStorageSize = (storageGb) => {
  if (storageGb == null) return null;
  if (storageGb <= 64) return 38;
  if (storageGb <= 128) return 54;
  if (storageGb <= 256) return 70;
  if (storageGb <= 512) return 84;
  if (storageGb <= 1024) return 93;
  return 98;
};

const detectStorageTierScore = (value) => {
  const text = normalizeText(value);
  if (!text) return null;
  if (text.includes("ufs 4")) return 96;
  if (text.includes("ufs 3.1")) return 85;
  if (text.includes("ufs 3")) return 80;
  if (text.includes("ufs 2.2")) return 70;
  if (text.includes("ssd") && text.includes("pcie 4")) return 95;
  if (text.includes("ssd") && text.includes("pcie 3")) return 88;
  if (text.includes("ssd")) return 82;
  if (text.includes("emmc")) return 46;
  if (text.includes("hdd")) return 44;
  return 62;
};

const scoreBatteryCapacity = (capacity) => {
  if (capacity == null) return 35;
  if (capacity <= 3000) return 25;
  if (capacity <= 4000) return 45;
  if (capacity <= 4500) return 60;
  if (capacity <= 5000) return 75;
  if (capacity <= 5500) return 86;
  if (capacity <= 6000) return 94;
  return 100;
};

const scoreChargingWatt = (watt) => {
  if (watt == null) return null;
  if (watt <= 18) return 42;
  if (watt <= 33) return 56;
  if (watt <= 45) return 66;
  if (watt <= 67) return 76;
  if (watt <= 100) return 88;
  if (watt <= 150) return 95;
  return 99;
};

const collectMegapixelValues = (value, bucket) => {
  if (value == null) return;
  if (typeof value === "number" && Number.isFinite(value)) {
    bucket.push(value);
    return;
  }
  if (typeof value === "string") {
    const matches = value.match(/(\d+(?:\.\d+)?)\s*mp/gi);
    if (matches) {
      matches.forEach((entry) => {
        const n = toFiniteNumber(entry);
        if (n != null) bucket.push(n);
      });
      return;
    }
    const n = toFiniteNumber(value);
    if (n != null && n <= 250) bucket.push(n);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectMegapixelValues(item, bucket));
    return;
  }
  if (typeof value === "object") {
    Object.values(value).forEach((nested) => collectMegapixelValues(nested, bucket));
  }
};

const extractMainMegapixel = (camera) => {
  const source = toObject(camera);
  const directValues = [];
  collectMegapixelValues(source.main_camera_megapixels, directValues);
  collectMegapixelValues(source.main, directValues);
  collectMegapixelValues(source.primary, directValues);
  collectMegapixelValues(source?.rear_camera?.main_camera, directValues);
  collectMegapixelValues(source?.rear_camera?.main, directValues);
  if (directValues.length) return Math.max(...directValues);

  const fallbackValues = [];
  collectMegapixelValues(source.rear_camera, fallbackValues);
  if (!fallbackValues.length) return null;
  return Math.max(...fallbackValues);
};

const scoreMainMegapixel = (megapixel) => {
  if (megapixel == null) return null;
  if (megapixel <= 12) return 46;
  if (megapixel <= 32) return 58;
  if (megapixel <= 50) return 72;
  if (megapixel <= 64) return 78;
  if (megapixel <= 108) return 86;
  if (megapixel <= 200) return 91;
  return 94;
};

const detectVideoScore = (camera) => {
  const text = buildTextBlob(camera);
  if (!text) return null;
  if (/\b8k\b/.test(text)) return 96;
  if (/4k[^0-9]*120/.test(text)) return 92;
  if (/4k[^0-9]*60/.test(text)) return 86;
  if (/4k/.test(text)) return 78;
  if (/1080p[^0-9]*60/.test(text)) return 66;
  if (/1080p/.test(text)) return 58;
  return null;
};

const finalizeWeightedCategory = (metrics, { neutral = 55 } = {}) => {
  const all = Array.isArray(metrics) ? metrics : [];
  const totalWeight = all.reduce((acc, item) => acc + (item?.weight || 0), 0);
  if (!totalWeight) {
    return { score: neutral, coverage: 0 };
  }

  let knownWeight = 0;
  let weightedKnown = 0;
  for (const item of all) {
    const score = toFiniteNumber(item?.score);
    const weight = Number(item?.weight) || 0;
    if (score == null || weight <= 0) continue;
    knownWeight += weight;
    weightedKnown += score * weight;
  }

  if (!knownWeight) {
    return { score: neutral, coverage: 0 };
  }

  const normalizedKnown = weightedKnown / knownWeight;
  const coverage = clamp(knownWeight / totalWeight, 0, 1);
  const confidenceFactor = 0.45 + coverage * 0.55;
  const score = neutral + (normalizedKnown - neutral) * confidenceFactor;

  return {
    score: roundOne(clamp(score, 0, 100)),
    coverage: roundTwo(coverage),
  };
};

const averageCoverage = (values) => {
  const valid = (Array.isArray(values) ? values : []).filter((item) =>
    Number.isFinite(item),
  );
  if (!valid.length) return 0;
  return valid.reduce((sum, item) => sum + item, 0) / valid.length;
};

const finalizeConfidence = (coverages) =>
  roundTwo(0.35 + averageCoverage(coverages) * 0.65);

const buildReasonList = (reasons = [], limit = 3) => {
  const seen = new Set();
  return reasons
    .filter((item) => item && item.text)
    .sort((left, right) => (right.priority || 0) - (left.priority || 0))
    .filter((item) => {
      const key = item.text;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, limit)
    .map((item) => item.text);
};

const computeRelativeValueScores = (rows) => {
  const valid = rows.filter((row) => row.valueRaw != null);
  const minValue = valid.length
    ? Math.min(...valid.map((row) => row.valueRaw))
    : null;
  const maxValue = valid.length
    ? Math.max(...valid.map((row) => row.valueRaw))
    : null;

  const map = new Map();
  rows.forEach((row) => {
    let valueScore = 45;
    if (row.valueRaw == null) {
      valueScore = 45;
    } else if (minValue != null && maxValue != null) {
      if (maxValue === minValue) valueScore = 70;
      else valueScore = 35 + ((row.valueRaw - minValue) / (maxValue - minValue)) * 65;
    }
    map.set(row.productId, roundOne(clamp(valueScore, 0, 100)));
  });
  return map;
};

const SMARTPHONE_VALUE_BASELINES = [
  { maxPrice: 10000, expected: 50 },
  { maxPrice: 20000, expected: 58 },
  { maxPrice: 30000, expected: 65 },
  { maxPrice: 45000, expected: 72 },
  { maxPrice: 65000, expected: 79 },
  { maxPrice: 90000, expected: 85 },
  { maxPrice: 130000, expected: 89 },
];

const LAPTOP_VALUE_BASELINES = [
  { maxPrice: 40000, expected: 48 },
  { maxPrice: 65000, expected: 58 },
  { maxPrice: 90000, expected: 67 },
  { maxPrice: 130000, expected: 76 },
  { maxPrice: 180000, expected: 84 },
];

const TV_VALUE_BASELINES = [
  { maxPrice: 30000, expected: 45 },
  { maxPrice: 50000, expected: 55 },
  { maxPrice: 80000, expected: 66 },
  { maxPrice: 130000, expected: 77 },
  { maxPrice: 200000, expected: 86 },
];

const NETWORKING_VALUE_BASELINES = [
  { maxPrice: 5000, expected: 44 },
  { maxPrice: 10000, expected: 54 },
  { maxPrice: 18000, expected: 64 },
  { maxPrice: 30000, expected: 75 },
  { maxPrice: 50000, expected: 84 },
];

const resolveExpectedScoreByPrice = (price, baselines) => {
  if (!Number.isFinite(price) || price <= 0) return null;
  const list = Array.isArray(baselines) ? baselines : [];
  for (const item of list) {
    if (price <= item.maxPrice) return item.expected;
  }
  return list.length ? list[list.length - 1].expected + 2 : 70;
};

const computeAbsoluteValueScore = (productType, baseSpecScore, price) => {
  if (!Number.isFinite(baseSpecScore)) return 45;
  if (!Number.isFinite(price) || price <= 0) return 45;

  let baselines = SMARTPHONE_VALUE_BASELINES;
  if (productType === "laptop") baselines = LAPTOP_VALUE_BASELINES;
  else if (productType === "tv") baselines = TV_VALUE_BASELINES;
  else if (productType === "networking") baselines = NETWORKING_VALUE_BASELINES;

  const expected = resolveExpectedScoreByPrice(price, baselines);
  if (!Number.isFinite(expected)) return 45;

  const gap = baseSpecScore - expected;
  return roundOne(clamp(60 + gap * 1.7, 28, 98));
};

const computeExperienceAdjustment = (score) => {
  const normalized = toFiniteNumber(score);
  if (normalized == null) return 0;
  return roundOne(clamp((normalized - 55) * 0.08, -3, 4));
};

const scoreBooleanCapability = (state, positive = 84, negative = 38) => {
  if (state == null) return null;
  return state ? positive : negative;
};

const normalizeLensObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : null;

const extractCameraLensBag = (camera) => {
  const source = toObject(camera);
  const rear = normalizeLensObject(source.rear_camera);
  const lenses = [];
  if (rear) {
    Object.values(rear).forEach((lens) => {
      if (lens != null) lenses.push(lens);
    });
  }
  [
    source.main,
    source.primary,
    source.ultra_wide,
    source.telephoto,
    source.periscope,
    source.periscope_telephoto,
    source.front_camera,
  ].forEach((lens) => {
    if (lens != null) lenses.push(lens);
  });
  return lenses;
};

const detectOisState = (camera) => {
  const source = toObject(camera);
  const lenses = extractCameraLensBag(source);
  for (const lens of lenses) {
    if (lens == null) continue;
    const direct = readSupportState(lens?.ois ?? lens?.OIS ?? lens?.stabilization ?? lens?.eis);
    if (direct != null) return direct;
  }
  const blob = buildTextBlob(source);
  if (/\bois\b/.test(blob)) return true;
  if (/no ois|without ois/.test(blob)) return false;
  return null;
};

const detectTelephotoState = (camera) => {
  const source = toObject(camera);
  if (source?.rear_camera?.telephoto || source?.telephoto) return true;
  const blob = buildTextBlob(source);
  if (/\btelephoto\b/.test(blob)) return true;
  return null;
};

const detectPeriscopeState = (camera) => {
  const source = toObject(camera);
  if (source?.rear_camera?.periscope_telephoto || source?.periscope_telephoto) {
    return true;
  }
  const blob = buildTextBlob(source);
  if (/\bperiscope\b/.test(blob)) return true;
  return null;
};

const detectUltrawideState = (camera) => {
  const source = toObject(camera);
  if (source?.rear_camera?.ultra_wide || source?.ultra_wide) return true;
  const blob = buildTextBlob(source);
  if (/\bultra\s*wide\b|\bultrawide\b/.test(blob)) return true;
  return null;
};

const scoreLensVersatility = ({ ultrawide, telephoto, periscope }) => {
  const booleans = [ultrawide, telephoto, periscope].filter((item) => item === true);
  if (!booleans.length) return ultrawide === false && telephoto === false && periscope === false ? 34 : null;
  if (periscope) return 96;
  if (telephoto && ultrawide) return 88;
  if (telephoto || ultrawide) return 72;
  return 56;
};

const detectImagingPartnerScore = (camera) => {
  const text = buildTextBlob(camera);
  if (!text) return null;
  if (/\bhasselblad\b|\bleica\b|\bzeiss\b/.test(text)) return 86;
  if (/\btuned by\b|\bco-engineered\b/.test(text)) return 74;
  return null;
};

const detectAiImagingScore = (camera) => {
  const text = buildTextBlob(camera);
  if (!text || !/\bai\b/.test(text)) return null;
  if (/\bai edit\b|\bai erase\b|\bai imaging\b|\bai photo\b/.test(text)) {
    return 78;
  }
  return 68;
};

const extractBatteryCapacity = (battery) => {
  const source = toObject(battery);
  const candidates = [
    source.battery_capacity_mah,
    source.capacity_mah,
    source.capacity,
    source.mAh,
    source.value,
  ];

  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }

  return null;
};

const extractChargingWatt = (battery) => {
  const source = toObject(battery);
  const candidates = [
    source.charging_speed_watt,
    source.fast_charging_watt,
    source.charging_wattage,
    source.charging_power,
    source.wired_charging,
    source.fast_charging,
    source.charging,
  ];
  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }
  const text = buildTextBlob(source);
  const match = text.match(/(\d+(?:\.\d+)?)\s*w\b/i);
  if (!match) return null;
  const watt = Number(match[1]);
  return Number.isFinite(watt) ? watt : null;
};

const extractWirelessChargingWatt = (battery) => {
  const source = toObject(battery);
  const raw = source.wireless_charging ?? source.wirelessCharging ?? null;
  if (raw == null || raw === "") return null;
  if (typeof raw === "boolean") return raw ? 15 : 0;
  const value = toFiniteNumber(raw);
  if (value != null) return value;
  return readSupportState(raw) === true ? 15 : null;
};

const extractReverseChargingState = (battery) => {
  const source = toObject(battery);
  const candidates = [
    source.reverse_charging,
    source.reverse_wireless_charging,
    source.reverse_wireless,
    source.wireless_reverse_charging,
  ];
  for (const candidate of candidates) {
    const direct = readSupportState(candidate);
    if (direct != null) return direct;
  }
  const blob = buildTextBlob(source);
  if (/\breverse charging\b|\breverse wireless\b/.test(blob)) return true;
  return null;
};

const detectAiSupportScore = (device) => {
  const sections = [
    device?.performance,
    device?.camera,
    device?.multimedia,
    device?.features,
    device?.software,
    device?.smart_features,
    device?.networking_features,
  ];
  const text = buildTextBlob(sections);
  if (!text || !/\bai\b/.test(text)) return null;
  if (
    /\bai note\b|\bai edit\b|\bai translate\b|\bcall summary\b|\bwriting tools\b|\bgalaxy ai\b|\bapple intelligence\b/.test(
      text,
    )
  ) {
    return 82;
  }
  return 70;
};

const detectIpRatingScore = (buildDesign) => {
  const text = buildTextBlob(buildDesign);
  if (!text) return null;
  const match = text.match(/\bip(\d)(\d)\b/i);
  if (!match) return null;
  const digits = Number(match[1]) * 10 + Number(match[2]);
  if (digits >= 68) return 92;
  if (digits >= 67) return 88;
  if (digits >= 65) return 80;
  return 72;
};

const detectWifiSupportScore = (connectivity) => {
  const text = buildTextBlob(connectivity);
  if (!text) return null;
  if (/\bwi-?fi\s*7\b|802\.11be/.test(text)) return 94;
  if (/\bwi-?fi\s*6e\b/.test(text)) return 88;
  if (/\bwi-?fi\s*6\b|802\.11ax/.test(text)) return 80;
  if (/\bwi-?fi\s*5\b|802\.11ac/.test(text)) return 66;
  return null;
};

const detectNfcScore = (connectivity) => {
  const source = toObject(connectivity);
  const state = readSupportState(source.nfc);
  return scoreBooleanCapability(state, 78, 40);
};

const detectEsimScore = (connectivity, network) => {
  const text = buildTextBlob(connectivity, network);
  if (!text) return null;
  if (/\besim\b/.test(text)) return 76;
  return null;
};

const detectStereoScore = (audio) => {
  const text = buildTextBlob(audio);
  if (!text) return null;
  if (/\bstereo\b|\bdolby atmos\b|\bdual speaker\b/.test(text)) return 80;
  return null;
};

const extractRamGbFromText = (value) => {
  const text = normalizeText(value);
  if (!text) return null;
  const match = text.match(/(\d+(?:\.\d+)?)\s*gb/);
  if (!match) return toFiniteNumber(value);
  const ram = Number(match[1]);
  return Number.isFinite(ram) ? ram : null;
};

const extractStorageGbFromText = (value) => {
  const text = normalizeText(value);
  if (!text) return null;
  const tbMatch = text.match(/(\d+(?:\.\d+)?)\s*tb/);
  if (tbMatch) {
    const tb = Number(tbMatch[1]);
    return Number.isFinite(tb) ? tb * 1024 : null;
  }
  const gbMatch = text.match(/(\d+(?:\.\d+)?)\s*gb/);
  if (gbMatch) {
    const gb = Number(gbMatch[1]);
    return Number.isFinite(gb) ? gb : null;
  }
  return toFiniteNumber(value);
};

const scoreWeightKg = (weightKg) => {
  if (weightKg == null) return null;
  if (weightKg <= 1.1) return 96;
  if (weightKg <= 1.4) return 88;
  if (weightKg <= 1.8) return 76;
  if (weightKg <= 2.2) return 62;
  if (weightKg <= 2.8) return 50;
  return 40;
};

const extractWeightKg = (physical) => {
  const source = toObject(physical);
  const text = buildTextBlob(source.weight, source);
  if (!text) return null;
  const kgMatch = text.match(/(\d+(?:\.\d+)?)\s*kg/);
  if (kgMatch) return Number(kgMatch[1]);
  const gramMatch = text.match(/(\d+(?:\.\d+)?)\s*g\b/);
  if (gramMatch) {
    const grams = Number(gramMatch[1]);
    return Number.isFinite(grams) ? grams / 1000 : null;
  }
  return null;
};

const extractBatteryWh = (battery) => {
  const source = toObject(battery);
  const text = buildTextBlob(source);
  if (!text) return null;
  const whMatch = text.match(/(\d+(?:\.\d+)?)\s*wh/);
  if (whMatch) return Number(whMatch[1]);
  const mah = extractBatteryCapacity(source);
  return mah;
};

const scoreBatteryWh = (batteryWh) => {
  if (batteryWh == null) return null;
  if (batteryWh <= 40) return 46;
  if (batteryWh <= 50) return 58;
  if (batteryWh <= 60) return 70;
  if (batteryWh <= 75) return 82;
  if (batteryWh <= 90) return 91;
  return 96;
};

const detectGpuScore = (device) => {
  const text = buildTextBlob(
    device?.cpu?.gpu,
    device?.performance?.gpu,
    device?.features?.graphics,
    device?.meta?.graphics,
  );
  if (!text) return null;
  if (/rtx\s*4090|rtx\s*5090/.test(text)) return 99;
  if (/rtx\s*4080|rtx\s*5080/.test(text)) return 96;
  if (/rtx\s*4070|rtx\s*5070/.test(text)) return 92;
  if (/rtx\s*4060|rtx\s*5060/.test(text)) return 87;
  if (/rtx\s*4050|rtx\s*3050/.test(text)) return 78;
  if (/arc|radeon\s*780m|radeon\s*890m/.test(text)) return 74;
  if (/iris|integrated/.test(text)) return 62;
  return 68;
};

const detectThunderboltScore = (connectivity) => {
  const text = buildTextBlob(connectivity);
  if (!text) return null;
  if (/thunderbolt\s*4|usb4/.test(text)) return 92;
  if (/thunderbolt\s*3/.test(text)) return 82;
  if (/usb\s*3\.2/.test(text)) return 70;
  return null;
};

const detectWifiLaptopScore = (connectivity) => {
  const text = buildTextBlob(connectivity);
  if (!text) return null;
  if (/wi-?fi\s*7|802\.11be/.test(text)) return 92;
  if (/wi-?fi\s*6e/.test(text)) return 86;
  if (/wi-?fi\s*6|802\.11ax/.test(text)) return 78;
  if (/wi-?fi\s*5|802\.11ac/.test(text)) return 66;
  return null;
};

const detectFingerprintLaptopScore = (features) => {
  const text = buildTextBlob(features);
  if (!text) return null;
  if (/fingerprint/.test(text)) return 72;
  return null;
};

const detectResolutionTierScore = (value) => {
  const parsed = parseResolution(value);
  if (!parsed) return null;
  if (parsed.pixels >= 7680 * 4320) return 99;
  if (parsed.pixels >= 3840 * 2160) return 92;
  if (parsed.pixels >= 2560 * 1440) return 82;
  if (parsed.pixels >= 1920 * 1080) return 68;
  return 54;
};

const detectHdrTvScore = (display, smartFeatures) => {
  const text = buildTextBlob(display, smartFeatures);
  if (!text) return null;
  if (/dolby vision/.test(text)) return 92;
  if (/hdr10\+/.test(text)) return 88;
  if (/hdr10|hlg|hdr/.test(text)) return 78;
  return null;
};

const detectSmartOsScore = (smartFeatures) => {
  const text = buildTextBlob(smartFeatures);
  if (!text) return null;
  if (/google tv|android tv|webos|tizen|fire tv/.test(text)) return 84;
  if (/smart tv/.test(text)) return 70;
  return null;
};

const detectAudioPowerScore = (audio) => {
  const output = readLargestNumber(audio, { min: 5, max: 300 });
  if (output == null) return null;
  if (output <= 20) return 58;
  if (output <= 30) return 68;
  if (output <= 40) return 76;
  if (output <= 60) return 86;
  return 94;
};

const detectDolbyAudioScore = (audio) => {
  const text = buildTextBlob(audio);
  if (!text) return null;
  if (/dolby atmos/.test(text)) return 90;
  if (/dolby audio/.test(text)) return 82;
  return null;
};

const detectGamingTvScore = (gaming, display, ports) => {
  const text = buildTextBlob(gaming, display, ports);
  if (!text) return null;
  let score = 58;
  if (/hdmi\s*2\.1/.test(text)) score += 10;
  if (/\bvrr\b/.test(text)) score += 10;
  if (/\ballm\b/.test(text)) score += 8;
  if (/120\s*hz|144\s*hz/.test(text)) score += 8;
  return clamp(score, 58, 96);
};

const detectTvConnectivityScore = (connectivity, ports) => {
  const text = buildTextBlob(connectivity, ports);
  if (!text) return null;
  let score = 55;
  if (/wi-?fi\s*6|802\.11ax/.test(text)) score += 10;
  if (/bluetooth\s*5/.test(text)) score += 6;
  if (/hdmi/.test(text)) score += 8;
  if (/usb/.test(text)) score += 6;
  if (/earc/.test(text)) score += 8;
  return clamp(score, 55, 92);
};

const detectWifiStandardScore = (connectivity, specifications) => {
  const text = buildTextBlob(connectivity, specifications);
  if (!text) return null;
  if (/wi-?fi\s*7|802\.11be/.test(text)) return 96;
  if (/wi-?fi\s*6e/.test(text)) return 90;
  if (/wi-?fi\s*6|802\.11ax/.test(text)) return 82;
  if (/wi-?fi\s*5|802\.11ac/.test(text)) return 68;
  return null;
};

const detectThroughputScore = (performance, specifications) => {
  const highest = readLargestNumber([performance, specifications], {
    min: 100,
    max: 100000,
  });
  if (highest == null) return null;
  if (highest >= 30000) return 98;
  if (highest >= 10000) return 92;
  if (highest >= 6000) return 84;
  if (highest >= 3000) return 74;
  if (highest >= 1200) return 64;
  return 54;
};

const detectCoverageScore = (performance, specifications, features) => {
  const text = buildTextBlob(performance, specifications, features);
  if (!text) return null;
  let score = 55;
  if (/tri-?band/.test(text)) score += 12;
  if (/quad-?band/.test(text)) score += 16;
  if (/mesh/.test(text)) score += 10;
  if (/beamforming/.test(text)) score += 6;
  if (/antennas?/.test(text)) score += 6;
  return clamp(score, 55, 95);
};

const detectPortScore = (specifications, connectivity) => {
  const text = buildTextBlob(specifications, connectivity);
  if (!text) return null;
  let score = 48;
  const portCount = (text.match(/\brj45\b|\blan\b|\bwan\b|\bethernet\b/g) || [])
    .length;
  score += Math.min(18, portCount * 4);
  if (/2\.5g/.test(text)) score += 12;
  if (/10g/.test(text)) score += 18;
  if (/usb/.test(text)) score += 6;
  return clamp(score, 48, 96);
};

const detectNetworkingFeatureScore = (features) => {
  const text = buildTextBlob(features);
  if (!text) return null;
  let score = 54;
  if (/\bmu-?mimo\b/.test(text)) score += 10;
  if (/\bofdma\b/.test(text)) score += 10;
  if (/\bqos\b/.test(text)) score += 8;
  if (/parental control/.test(text)) score += 6;
  if (/vpn/.test(text)) score += 6;
  if (/mesh/.test(text)) score += 6;
  return clamp(score, 54, 96);
};

const detectNetworkingSecurityScore = (features, connectivity) => {
  const text = buildTextBlob(features, connectivity);
  if (!text) return null;
  let score = 54;
  if (/wpa3/.test(text)) score += 16;
  if (/firewall/.test(text)) score += 8;
  if (/guest network/.test(text)) score += 8;
  if (/parental control/.test(text)) score += 6;
  return clamp(score, 54, 94);
};

const scoreSmartphoneRow = (device, variantSelection, config) => {
  const chipsetRules = config.chipsetRules;
  const performance = toObject(device?.performance);
  const display = toObject(device?.display);
  const camera = toObject(device?.camera);
  const battery = toObject(device?.battery);
  const connectivity = toObject(device?.connectivity);
  const network = toObject(device?.network);
  const buildDesign = toObject(device?.build_design);
  const audio = toObject(device?.audio);
  const multimedia = toObject(device?.multimedia);
  const sensors = toObject(device?.sensors);
  const price = extractPrice(device, variantSelection);
  const processorText = extractProcessorText(device);
  const chipsetScore = scoreChipset(processorText, chipsetRules);
  const selectedRam = extractRamGbFromText(
    getSelectedVariantValue(device, variantSelection, ["ram", "memory", "memory_ram"]) ??
      performance.ram ??
      performance.ram_options,
  );
  const selectedStorageSize = extractStorageGbFromText(
    getSelectedVariantValue(device, variantSelection, ["storage", "rom"]) ??
      performance.storage,
  );
  const storageTierScore = detectStorageTierScore(
    getSelectedVariantValue(device, variantSelection, ["storage_type"]) ??
      performance.storage_type ??
      performance.storageType,
  );
  const aiSupportScore = detectAiSupportScore({
    performance,
    camera,
    multimedia,
  });

  const performanceCategory = finalizeWeightedCategory(
    [
      { score: chipsetScore, weight: 0.72 },
      { score: scoreRamGb(selectedRam), weight: 0.16 },
      { score: storageTierScore, weight: 0.08 },
      { score: aiSupportScore, weight: 0.04 },
    ],
    { neutral: 58 },
  );

  const refreshRate = extractRefreshRate(display);
  const panel = detectPanelScore(display);
  const brightnessScore = scoreBrightness(extractPeakBrightness(display));
  const resolutionScore = scoreResolution(
    display.resolution ?? display.screen_resolution ?? display.pixel_resolution,
  );
  const ltpoScore = /ltpo/.test(buildTextBlob(display)) ? 92 : null;
  const hdrScore = /\bdolby vision\b|\bhdr10\+?\b|\bhdr\b/.test(buildTextBlob(display))
    ? 82
    : null;

  const displayCategory = finalizeWeightedCategory(
    [
      { score: scoreRefreshRate(refreshRate), weight: 0.24 },
      { score: panel.score, weight: 0.22 },
      { score: brightnessScore, weight: 0.18 },
      { score: resolutionScore, weight: 0.16 },
      { score: ltpoScore, weight: 0.1 },
      { score: hdrScore, weight: 0.1 },
    ],
    { neutral: 57 },
  );

  const mainMegapixel = extractMainMegapixel(camera);
  const oisState = detectOisState(camera);
  const telephotoState = detectTelephotoState(camera);
  const periscopeState = detectPeriscopeState(camera);
  const ultrawideState = detectUltrawideState(camera);
  const videoScore = detectVideoScore(camera);
  const cameraCategory = finalizeWeightedCategory(
    [
      { score: scoreMainMegapixel(mainMegapixel), weight: 0.28 },
      {
        score: scoreLensVersatility({
          ultrawide: ultrawideState,
          telephoto: telephotoState,
          periscope: periscopeState,
        }),
        weight: 0.24,
      },
      { score: scoreBooleanCapability(oisState, 88, 40), weight: 0.18 },
      { score: videoScore, weight: 0.16 },
      { score: detectAiImagingScore(camera), weight: 0.08 },
      { score: detectImagingPartnerScore(camera), weight: 0.06 },
    ],
    { neutral: 55 },
  );

  const batteryCapacity = extractBatteryCapacity(battery);
  const chargingWatt = extractChargingWatt(battery);
  const wirelessWatt = extractWirelessChargingWatt(battery);
  const reverseChargingState = extractReverseChargingState(battery);
  const batteryCategory = finalizeWeightedCategory(
    [
      { score: scoreBatteryCapacity(batteryCapacity), weight: 0.45 },
      { score: scoreChargingWatt(chargingWatt), weight: 0.25 },
      {
        score:
          wirelessWatt == null
            ? null
            : wirelessWatt > 0
              ? clamp(68 + Math.min(24, wirelessWatt), 68, 96)
              : 42,
        weight: 0.12,
      },
      { score: scoreBooleanCapability(reverseChargingState, 74, 42), weight: 0.08 },
      {
        score:
          chipsetScore >= 90 && refreshRate != null && refreshRate >= 120
            ? 74
            : chipsetScore >= 80
              ? 66
              : null,
        weight: 0.1,
      },
    ],
    { neutral: 56 },
  );

  const experienceCategory = finalizeWeightedCategory(
    [
      { score: detectIpRatingScore(buildDesign), weight: 0.22 },
      { score: detectWifiSupportScore(connectivity), weight: 0.18 },
      { score: detectNfcScore(connectivity), weight: 0.12 },
      { score: detectEsimScore(connectivity, network), weight: 0.12 },
      { score: detectStereoScore(audio), weight: 0.14 },
      { score: aiSupportScore, weight: 0.12 },
      {
        score:
          buildTextBlob(sensors, connectivity).includes("fingerprint") ? 66 : null,
        weight: 0.1,
      },
    ],
    { neutral: 56 },
  );

  const baseSpecScore =
    performanceCategory.score * 0.36 +
    displayCategory.score * 0.22 +
    cameraCategory.score * 0.24 +
    batteryCategory.score * 0.18;

  const reasonCandidates = [];
  if (chipsetScore >= 92) {
    reasonCandidates.push({ text: "Flagship chipset headroom", priority: 96 });
  } else if (chipsetScore >= 80) {
    reasonCandidates.push({ text: "Strong upper-tier chipset", priority: 82 });
  }
  if (selectedRam != null && selectedRam >= 12) {
    reasonCandidates.push({ text: "Higher RAM headroom for multitasking", priority: 70 });
  }
  if (storageTierScore != null && storageTierScore >= 90) {
    reasonCandidates.push({ text: "Fast modern storage tier", priority: 74 });
  }
  if (ltpoScore != null) {
    reasonCandidates.push({ text: "LTPO adaptive display", priority: 84 });
  }
  if (brightnessScore != null && brightnessScore >= 88) {
    reasonCandidates.push({ text: "Very high display brightness", priority: 80 });
  }
  if (oisState === true && periscopeState === true) {
    reasonCandidates.push({ text: "Versatile stabilized zoom camera", priority: 90 });
  } else if (oisState === true) {
    reasonCandidates.push({ text: "OIS-backed main camera", priority: 74 });
  }
  if (videoScore != null && videoScore >= 86) {
    reasonCandidates.push({ text: "Strong video recording support", priority: 76 });
  }
  if (batteryCapacity != null && batteryCapacity >= 6000) {
    reasonCandidates.push({ text: "Large battery capacity", priority: 82 });
  }
  if (chargingWatt != null && chargingWatt >= 80) {
    reasonCandidates.push({ text: "Fast wired charging", priority: 78 });
  }
  if (wirelessWatt != null && wirelessWatt > 0) {
    reasonCandidates.push({ text: "Wireless charging support", priority: 68 });
  }
  if (experienceCategory.score >= 74) {
    reasonCandidates.push({ text: "Better modern convenience features", priority: 66 });
  }

  return {
    productType: "smartphone",
    productId: String(device?.product_id ?? device?.id ?? ""),
    deviceName: String(device?.name || device?.model || "Device"),
    price,
    baseSpecScore: roundOne(baseSpecScore),
    valueRaw: price && price > 0 ? baseSpecScore / price : null,
    categoryOrder: ["performance", "display", "camera", "battery", "priceValue"],
    experienceAdjustment: computeExperienceAdjustment(experienceCategory.score),
    breakdown: {
      performance: performanceCategory.score,
      display: displayCategory.score,
      camera: cameraCategory.score,
      battery: batteryCategory.score,
    },
    categoryReasons: {
      performance:
        chipsetScore >= 90
          ? "Leads on raw chipset headroom and memory setup."
          : "Balanced performance package with sensible headroom.",
      display:
        ltpoScore != null
          ? "Adaptive display stack adds a premium viewing edge."
          : "Display score blends refresh rate, panel quality, and brightness.",
      camera:
        oisState === true
          ? "Camera score benefits from stabilization and lens versatility."
          : "Camera score reflects sensor class, lens mix, and video support.",
      battery:
        chargingWatt != null && chargingWatt >= 80
          ? "Battery score is lifted by capacity and faster charging."
          : "Battery score blends capacity, charging, and convenience features.",
      experience:
        "Cross-cutting quality signals like IP rating, wireless features, and AI tooling.",
    },
    confidence: finalizeConfidence([
      performanceCategory.coverage,
      displayCategory.coverage,
      cameraCategory.coverage,
      batteryCategory.coverage,
      experienceCategory.coverage,
    ]),
    reasons: buildReasonList(reasonCandidates),
    details: {
      processorText,
      panelType: panel.label,
      refreshRate,
      mainMegapixel,
      batteryCapacity,
      chargingWatt,
      wirelessWatt,
      ois: oisState,
      telephoto: telephotoState,
      periscope: periscopeState,
      aiSupportScore,
      experienceScore: experienceCategory.score,
    },
  };
};

const scoreLaptopRow = (device, variantSelection, config) => {
  const cpu = toObject(device?.cpu);
  const memory = toObject(device?.memory);
  const storage = toObject(device?.storage);
  const display = toObject(device?.display);
  const battery = toObject(device?.battery);
  const connectivity = toObject(device?.connectivity);
  const physical = toObject(device?.physical);
  const software = toObject(device?.software);
  const features = toObject(device?.features);
  const price = extractPrice(device, variantSelection);
  const processorText = extractProcessorText(device);
  const chipsetScore = scoreChipset(processorText, config.chipsetRules);
  const gpuScore = detectGpuScore({ cpu, performance: cpu, features, meta: device?.meta });
  const ramScore = scoreRamGb(
    extractRamGbFromText(
      getSelectedVariantValue(device, variantSelection, ["ram", "memory", "memory_ram"]) ??
        memory.ram,
    ),
  );
  const storageTierScore = detectStorageTierScore(storage.type ?? storage.storage_type ?? storage);
  const storageSizeScore = scoreStorageSize(extractStorageGbFromText(storage.capacity));

  const performanceCategory = finalizeWeightedCategory(
    [
      { score: chipsetScore, weight: 0.56 },
      { score: gpuScore, weight: 0.18 },
      { score: ramScore, weight: 0.14 },
      { score: storageTierScore, weight: 0.12 },
    ],
    { neutral: 58 },
  );

  const displayCategory = finalizeWeightedCategory(
    [
      { score: scoreRefreshRate(extractRefreshRate(display)), weight: 0.2 },
      { score: detectPanelScore(display).score, weight: 0.22 },
      {
        score: detectResolutionTierScore(
          display.resolution ?? display.screen_resolution ?? display,
        ),
        weight: 0.24,
      },
      { score: scoreBrightness(extractPeakBrightness(display)), weight: 0.16 },
      {
        score:
          /\boled\b|\bmini led\b/.test(buildTextBlob(display)) ? 84 : null,
        weight: 0.18,
      },
    ],
    { neutral: 56 },
  );

  const batteryCategory = finalizeWeightedCategory(
    [
      { score: scoreBatteryWh(extractBatteryWh(battery)), weight: 0.72 },
      { score: scoreChargingWatt(extractChargingWatt(battery)), weight: 0.18 },
      {
        score: /\busb-?c charging\b|\bpd charging\b/.test(buildTextBlob(battery))
          ? 74
          : null,
        weight: 0.1,
      },
    ],
    { neutral: 55 },
  );

  const memoryCategory = finalizeWeightedCategory(
    [
      { score: ramScore, weight: 0.56 },
      { score: storageSizeScore, weight: 0.24 },
      { score: storageTierScore, weight: 0.2 },
    ],
    { neutral: 55 },
  );

  const portabilityCategory = finalizeWeightedCategory(
    [
      { score: scoreWeightKg(extractWeightKg(physical)), weight: 0.72 },
      {
        score: /metal|aluminium|aluminum|magnesium/.test(buildTextBlob(physical))
          ? 76
          : null,
        weight: 0.16,
      },
      {
        score: /backlit/.test(buildTextBlob(features)) ? 66 : null,
        weight: 0.12,
      },
    ],
    { neutral: 56 },
  );

  const connectivityCategory = finalizeWeightedCategory(
    [
      { score: detectThunderboltScore(connectivity), weight: 0.34 },
      { score: detectWifiLaptopScore(connectivity), weight: 0.28 },
      { score: detectFingerprintLaptopScore(features), weight: 0.16 },
      {
        score: /hdmi/.test(buildTextBlob(connectivity)) ? 68 : null,
        weight: 0.12,
      },
      {
        score: /sd card/.test(buildTextBlob(connectivity)) ? 62 : null,
        weight: 0.1,
      },
    ],
    { neutral: 54 },
  );

  const baseSpecScore =
    performanceCategory.score * 0.32 +
    displayCategory.score * 0.22 +
    batteryCategory.score * 0.16 +
    memoryCategory.score * 0.16 +
    portabilityCategory.score * 0.08 +
    connectivityCategory.score * 0.06;

  const reasonCandidates = [];
  if (chipsetScore >= 88) {
    reasonCandidates.push({ text: "Strong processor class", priority: 88 });
  }
  if (gpuScore != null && gpuScore >= 86) {
    reasonCandidates.push({ text: "Capable graphics stack", priority: 82 });
  }
  if (displayCategory.score >= 82) {
    reasonCandidates.push({ text: "Better display quality", priority: 80 });
  }
  if (batteryCategory.score >= 82) {
    reasonCandidates.push({ text: "Larger battery envelope", priority: 74 });
  }
  if (portabilityCategory.score >= 80) {
    reasonCandidates.push({ text: "Lighter and more portable design", priority: 70 });
  }

  return {
    productType: "laptop",
    productId: String(device?.product_id ?? device?.id ?? ""),
    deviceName: String(device?.name || device?.model || "Device"),
    price,
    baseSpecScore: roundOne(baseSpecScore),
    valueRaw: price && price > 0 ? baseSpecScore / price : null,
    categoryOrder: [
      "performance",
      "display",
      "battery",
      "memory",
      "portability",
      "connectivity",
      "priceValue",
    ],
    experienceAdjustment: 0,
    breakdown: {
      performance: performanceCategory.score,
      display: displayCategory.score,
      battery: batteryCategory.score,
      memory: memoryCategory.score,
      portability: portabilityCategory.score,
      connectivity: connectivityCategory.score,
    },
    categoryReasons: {
      performance: "Performance score blends CPU, GPU, RAM, and storage speed.",
      display: "Display score reflects refresh rate, panel type, and resolution.",
      battery: "Battery score reflects battery size and charging convenience.",
      memory: "Memory score considers RAM capacity and storage setup.",
      portability: "Portability score favors lighter travel-friendly machines.",
      connectivity: "Connectivity score rewards modern ports, wireless, and biometrics.",
    },
    confidence: finalizeConfidence([
      performanceCategory.coverage,
      displayCategory.coverage,
      batteryCategory.coverage,
      memoryCategory.coverage,
      portabilityCategory.coverage,
      connectivityCategory.coverage,
    ]),
    reasons: buildReasonList(reasonCandidates),
    details: {
      processorText,
    },
  };
};

const scoreTvRow = (device, variantSelection) => {
  const display = toObject(device?.display);
  const smartFeatures = toObject(device?.smart_features);
  const audio = toObject(device?.audio);
  const gaming = toObject(device?.gaming);
  const ports = toObject(device?.ports);
  const connectivity = toObject(device?.connectivity);
  const price = extractPrice(device, variantSelection);

  const displayCategory = finalizeWeightedCategory(
    [
      {
        score: detectResolutionTierScore(
          display.resolution ?? display.screen_resolution ?? display,
        ),
        weight: 0.34,
      },
      { score: detectPanelScore(display).score, weight: 0.2 },
      { score: scoreRefreshRate(extractRefreshRate(display)), weight: 0.18 },
      { score: detectHdrTvScore(display, smartFeatures), weight: 0.18 },
      { score: scoreBrightness(extractPeakBrightness(display)), weight: 0.1 },
    ],
    { neutral: 58 },
  );

  const smartCategory = finalizeWeightedCategory(
    [
      { score: detectSmartOsScore(smartFeatures), weight: 0.42 },
      {
        score:
          /assistant|alexa|google assistant|bixby/.test(buildTextBlob(smartFeatures))
            ? 76
            : null,
        weight: 0.2,
      },
      {
        score: /netflix|prime video|youtube/.test(buildTextBlob(smartFeatures))
          ? 72
          : null,
        weight: 0.2,
      },
      {
        score: /chromecast|airplay|screen share/.test(buildTextBlob(smartFeatures))
          ? 72
          : null,
        weight: 0.18,
      },
    ],
    { neutral: 56 },
  );

  const audioCategory = finalizeWeightedCategory(
    [
      { score: detectAudioPowerScore(audio), weight: 0.58 },
      { score: detectDolbyAudioScore(audio), weight: 0.24 },
      {
        score: /subwoofer/.test(buildTextBlob(audio)) ? 74 : null,
        weight: 0.18,
      },
    ],
    { neutral: 54 },
  );

  const gamingCategory = finalizeWeightedCategory(
    [
      { score: detectGamingTvScore(gaming, display, ports), weight: 0.72 },
      {
        score: /game mode/.test(buildTextBlob(gaming)) ? 70 : null,
        weight: 0.28,
      },
    ],
    { neutral: 55 },
  );

  const connectivityCategory = finalizeWeightedCategory(
    [
      { score: detectTvConnectivityScore(connectivity, ports), weight: 0.7 },
      {
        score:
          /bluetooth\s*5|wi-?fi\s*6/.test(buildTextBlob(connectivity)) ? 74 : null,
        weight: 0.3,
      },
    ],
    { neutral: 54 },
  );

  const baseSpecScore =
    displayCategory.score * 0.4 +
    smartCategory.score * 0.2 +
    audioCategory.score * 0.15 +
    gamingCategory.score * 0.15 +
    connectivityCategory.score * 0.1;

  const reasonCandidates = [];
  if (displayCategory.score >= 84) {
    reasonCandidates.push({ text: "Stronger display package", priority: 88 });
  }
  if (smartCategory.score >= 78) {
    reasonCandidates.push({ text: "Better smart TV software stack", priority: 76 });
  }
  if (gamingCategory.score >= 82) {
    reasonCandidates.push({ text: "More gaming-friendly inputs", priority: 78 });
  }
  if (audioCategory.score >= 76) {
    reasonCandidates.push({ text: "Stronger built-in audio setup", priority: 68 });
  }

  return {
    productType: "tv",
    productId: String(device?.product_id ?? device?.id ?? ""),
    deviceName: String(device?.name || device?.model || "Device"),
    price,
    baseSpecScore: roundOne(baseSpecScore),
    valueRaw: price && price > 0 ? baseSpecScore / price : null,
    categoryOrder: [
      "display",
      "smart",
      "audio",
      "gaming",
      "connectivity",
      "priceValue",
    ],
    experienceAdjustment: 0,
    breakdown: {
      display: displayCategory.score,
      smart: smartCategory.score,
      audio: audioCategory.score,
      gaming: gamingCategory.score,
      connectivity: connectivityCategory.score,
    },
    categoryReasons: {
      display: "Display score blends resolution, panel, HDR, and motion handling.",
      smart: "Smart score rewards OS quality, apps, and assistant support.",
      audio: "Audio score reflects output power and surround support.",
      gaming: "Gaming score favors HDMI 2.1-era features and higher refresh options.",
      connectivity: "Connectivity score considers wireless support and useful ports.",
    },
    confidence: finalizeConfidence([
      displayCategory.coverage,
      smartCategory.coverage,
      audioCategory.coverage,
      gamingCategory.coverage,
      connectivityCategory.coverage,
    ]),
    reasons: buildReasonList(reasonCandidates),
    details: {},
  };
};

const scoreNetworkingRow = (device, variantSelection) => {
  const specifications = toObject(device?.specifications);
  const networkingFeatures = toObject(device?.networking_features);
  const networkingPerformance = toObject(device?.networking_performance);
  const connectivity = toObject(device?.connectivity);
  const price = extractPrice(device, variantSelection);

  const performanceCategory = finalizeWeightedCategory(
    [
      {
        score: detectWifiStandardScore(connectivity, specifications),
        weight: 0.38,
      },
      {
        score: detectThroughputScore(networkingPerformance, specifications),
        weight: 0.42,
      },
      {
        score: /multi-gig|2\.5g|10g/.test(buildTextBlob(specifications, connectivity))
          ? 78
          : null,
        weight: 0.2,
      },
    ],
    { neutral: 56 },
  );

  const coverageCategory = finalizeWeightedCategory(
    [
      { score: detectCoverageScore(networkingPerformance, specifications, networkingFeatures), weight: 0.72 },
      {
        score: /mesh/.test(buildTextBlob(networkingFeatures, specifications)) ? 82 : null,
        weight: 0.28,
      },
    ],
    { neutral: 54 },
  );

  const portsCategory = finalizeWeightedCategory(
    [
      { score: detectPortScore(specifications, connectivity), weight: 1 },
    ],
    { neutral: 52 },
  );

  const featuresCategory = finalizeWeightedCategory(
    [
      { score: detectNetworkingFeatureScore(networkingFeatures), weight: 1 },
    ],
    { neutral: 54 },
  );

  const securityCategory = finalizeWeightedCategory(
    [
      { score: detectNetworkingSecurityScore(networkingFeatures, connectivity), weight: 1 },
    ],
    { neutral: 54 },
  );

  const baseSpecScore =
    performanceCategory.score * 0.34 +
    coverageCategory.score * 0.22 +
    portsCategory.score * 0.16 +
    featuresCategory.score * 0.16 +
    securityCategory.score * 0.12;

  const reasonCandidates = [];
  if (performanceCategory.score >= 82) {
    reasonCandidates.push({ text: "Higher wireless performance class", priority: 86 });
  }
  if (coverageCategory.score >= 80) {
    reasonCandidates.push({ text: "Better coverage-oriented feature set", priority: 76 });
  }
  if (portsCategory.score >= 80) {
    reasonCandidates.push({ text: "Stronger wired port configuration", priority: 72 });
  }
  if (securityCategory.score >= 76) {
    reasonCandidates.push({ text: "More complete security features", priority: 66 });
  }

  return {
    productType: "networking",
    productId: String(device?.product_id ?? device?.id ?? ""),
    deviceName: String(device?.name || device?.model || "Device"),
    price,
    baseSpecScore: roundOne(baseSpecScore),
    valueRaw: price && price > 0 ? baseSpecScore / price : null,
    categoryOrder: [
      "performance",
      "coverage",
      "ports",
      "features",
      "security",
      "priceValue",
    ],
    experienceAdjustment: 0,
    breakdown: {
      performance: performanceCategory.score,
      coverage: coverageCategory.score,
      ports: portsCategory.score,
      features: featuresCategory.score,
      security: securityCategory.score,
    },
    categoryReasons: {
      performance: "Performance score blends wireless standard and throughput class.",
      coverage: "Coverage score rewards mesh and broader signal-oriented features.",
      ports: "Port score reflects ethernet count and faster wired ports.",
      features: "Features score rewards QoS, MU-MIMO, OFDMA, and VPN tools.",
      security: "Security score reflects WPA3-era protection and isolation features.",
    },
    confidence: finalizeConfidence([
      performanceCategory.coverage,
      coverageCategory.coverage,
      portsCategory.coverage,
      featuresCategory.coverage,
      securityCategory.coverage,
    ]),
    reasons: buildReasonList(reasonCandidates),
    details: {},
  };
};

const applyValueAndOverallScoring = (rows, config) => {
  const productType = rows[0]?.productType || "unknown";
  const relativeValueScores = computeRelativeValueScores(rows);

  const weightedRows = rows.map((row) => {
    const absoluteValue = computeAbsoluteValueScore(
      productType,
      row.baseSpecScore,
      row.price,
    );
    const relativeValue = relativeValueScores.get(row.productId) ?? 45;
    const priceValue = roundOne(
      clamp(absoluteValue * 0.7 + relativeValue * 0.3, 0, 100),
    );

    const reasonCandidates = [...(row.reasons || []).map((text) => ({ text, priority: 50 }))];
    if (priceValue >= 82) {
      reasonCandidates.push({ text: "Strong value for the selected price", priority: 72 });
    }

    const breakdown = {
      ...row.breakdown,
      priceValue,
    };

    let overallScore = 0;
    if (productType === "smartphone") {
      const weights = config.weights || DEFAULT_COMPARE_WEIGHTS;
      overallScore =
        breakdown.performance * weights.performance +
        breakdown.display * weights.display +
        breakdown.camera * weights.camera +
        breakdown.battery * weights.battery +
        breakdown.priceValue * weights.priceValue +
        (row.experienceAdjustment || 0);
    } else if (productType === "laptop") {
      overallScore =
        breakdown.performance * LAPTOP_COMPARE_WEIGHTS.performance +
        breakdown.display * LAPTOP_COMPARE_WEIGHTS.display +
        breakdown.battery * LAPTOP_COMPARE_WEIGHTS.battery +
        breakdown.memory * LAPTOP_COMPARE_WEIGHTS.memory +
        breakdown.portability * LAPTOP_COMPARE_WEIGHTS.portability +
        breakdown.connectivity * LAPTOP_COMPARE_WEIGHTS.connectivity +
        breakdown.priceValue * LAPTOP_COMPARE_WEIGHTS.priceValue;
    } else if (productType === "tv") {
      overallScore =
        breakdown.display * TV_COMPARE_WEIGHTS.display +
        breakdown.smart * TV_COMPARE_WEIGHTS.smart +
        breakdown.audio * TV_COMPARE_WEIGHTS.audio +
        breakdown.gaming * TV_COMPARE_WEIGHTS.gaming +
        breakdown.connectivity * TV_COMPARE_WEIGHTS.connectivity +
        breakdown.priceValue * TV_COMPARE_WEIGHTS.priceValue;
    } else if (productType === "networking") {
      overallScore =
        breakdown.performance * NETWORKING_COMPARE_WEIGHTS.performance +
        breakdown.coverage * NETWORKING_COMPARE_WEIGHTS.coverage +
        breakdown.ports * NETWORKING_COMPARE_WEIGHTS.ports +
        breakdown.features * NETWORKING_COMPARE_WEIGHTS.features +
        breakdown.security * NETWORKING_COMPARE_WEIGHTS.security +
        breakdown.priceValue * NETWORKING_COMPARE_WEIGHTS.priceValue;
    } else {
      const values = Object.values(breakdown).filter((value) => Number.isFinite(value));
      overallScore = values.length
        ? values.reduce((sum, value) => sum + value, 0) / values.length
        : 55;
    }

    return {
      ...row,
      breakdown,
      overallScore: roundOne(clamp(overallScore, 0, 100)),
      reasons: buildReasonList(reasonCandidates),
    };
  });

  const ranked = [...weightedRows].sort((a, b) => {
    if (b.overallScore !== a.overallScore) return b.overallScore - a.overallScore;
    if (b.confidence !== a.confidence) return b.confidence - a.confidence;
    const aPrice = a.price ?? Number.POSITIVE_INFINITY;
    const bPrice = b.price ?? Number.POSITIVE_INFINITY;
    if (aPrice !== bPrice) return aPrice - bPrice;
    return a.deviceName.localeCompare(b.deviceName);
  });

  return ranked.map((row, index) => ({
    ...row,
    rank: index + 1,
  }));
};

const pickWinnerRow = (rows, getter) => {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return null;
  const sorted = [...list].sort((a, b) => {
    const aValue = getter(a);
    const bValue = getter(b);
    if (bValue !== aValue) return bValue - aValue;
    if (b.overallScore !== a.overallScore) return b.overallScore - a.overallScore;
    if (b.confidence !== a.confidence) return b.confidence - a.confidence;
    const aPrice = a.price ?? Number.POSITIVE_INFINITY;
    const bPrice = b.price ?? Number.POSITIVE_INFINITY;
    if (aPrice !== bPrice) return aPrice - bPrice;
    return a.deviceName.localeCompare(b.deviceName);
  });
  return sorted[0] || null;
};

const toWinnerPayload = (row, score, reason) => {
  if (!row) return null;
  return {
    product_id: Number(row.productId),
    product_name: row.deviceName,
    score: roundOne(score),
    confidence: row.confidence,
    reason: String(reason || row.reasons?.[0] || "Best balance across the current comparison."),
  };
};

const buildCategoryWinners = (rows) => {
  const ranking = Array.isArray(rows) ? rows : [];
  if (!ranking.length) return {};
  const keys = new Set();
  ranking.forEach((row) => {
    Object.keys(row.breakdown || {}).forEach((key) => keys.add(key));
  });

  const winners = {};
  keys.forEach((key) => {
    const winner = pickWinnerRow(ranking, (row) =>
      Number(row?.breakdown?.[key]) || 0,
    );
    if (!winner) return;
    winners[key] = toWinnerPayload(
      winner,
      Number(winner.breakdown?.[key]) || 0,
      winner.categoryReasons?.[key] || winner.reasons?.[0],
    );
  });

  return winners;
};

const validateTypes = (devices) => {
  const types = [...new Set((devices || []).map((item) => normalizeProductType(item?.product_type)).filter(Boolean))];
  if (!types.length) return { productType: "unknown", mixed: false };
  return { productType: types[0], mixed: types.length > 1 };
};

const buildCompareRanking = (devices = [], variantSelection = {}, config = {}) => {
  const normalizedConfig = normalizeCompareScoreConfig(config);
  const rows = Array.isArray(devices) ? devices : [];
  const typeInfo = validateTypes(rows);

  if (!rows.length) {
    return {
      productType: typeInfo.productType,
      ranking: [],
      overallWinner: null,
      categoryWinners: {},
      warnings: ["No valid devices supplied for comparison."],
    };
  }

  if (typeInfo.mixed) {
    return {
      productType: "mixed",
      ranking: [],
      overallWinner: null,
      categoryWinners: {},
      warnings: ["Comparison scoring requires devices from the same product type."],
    };
  }

  const productType = typeInfo.productType;
  const scoredRows = rows.map((device) => {
    if (productType === "smartphone") {
      return scoreSmartphoneRow(device, variantSelection, normalizedConfig);
    }
    if (productType === "laptop") {
      return scoreLaptopRow(device, variantSelection, normalizedConfig);
    }
    if (productType === "tv") {
      return scoreTvRow(device, variantSelection, normalizedConfig);
    }
    if (productType === "networking") {
      return scoreNetworkingRow(device, variantSelection, normalizedConfig);
    }

    return {
      productType,
      productId: String(device?.product_id ?? device?.id ?? ""),
      deviceName: String(device?.name || device?.model || "Device"),
      price: extractPrice(device, variantSelection),
      baseSpecScore: 55,
      valueRaw: null,
      categoryOrder: ["priceValue"],
      experienceAdjustment: 0,
      breakdown: {},
      categoryReasons: {},
      confidence: 0.35,
      reasons: [],
      details: {},
    };
  });

  const ranking = applyValueAndOverallScoring(scoredRows, normalizedConfig);
  const overallWinnerRow = ranking[0] || null;
  const overallWinner = overallWinnerRow
    ? toWinnerPayload(
        overallWinnerRow,
        overallWinnerRow.overallScore,
        overallWinnerRow.reasons?.[0] ||
          "Best overall score across the current comparison.",
      )
    : null;

  return {
    productType,
    ranking,
    overallWinner,
    categoryWinners: buildCategoryWinners(ranking),
    warnings: [],
  };
};

module.exports = {
  DEFAULT_COMPARE_WEIGHTS,
  DEFAULT_CHIPSET_RULES,
  normalizeWeights,
  normalizeChipsetRules,
  normalizeCompareScoreConfig,
  buildCompareRanking,
  weightsToPercent,
};
