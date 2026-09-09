const LAPTOP_SPEC_SCORE_VERSION = "laptop_spec_score_v1";

const LAPTOP_CATEGORY_WEIGHTS = Object.freeze({
  performance: 0.26,
  graphics: 0.16,
  display: 0.17,
  memoryStorage: 0.15,
  batteryPortability: 0.12,
  connectivityPorts: 0.09,
  practical: 0.05,
});

const PERFORMANCE_FEATURE_RULES = Object.freeze([
  {
    key: "snapdragon_x2_elite_extreme",
    score: 99,
    aliases: [/\bsnapdragon\s*x2\s*elite\s*extreme\b/],
  },
  {
    key: "snapdragon_x2_elite",
    score: 96,
    aliases: [/\bsnapdragon\s*x2\s*elite\b/],
  },
  {
    key: "snapdragon_x2_plus",
    score: 91,
    aliases: [/\bsnapdragon\s*x2\s*plus\b/],
  },
  {
    key: "ryzen_ai_max_plus_400",
    score: 98,
    aliases: [/\bryzen\s*ai\s*max\+?\s*(?:pro\s*)?400\b/],
  },
  {
    key: "ryzen_ai_400",
    score: 94,
    aliases: [/\bryzen\s*ai\s*(?:pro\s*)?400\b/],
  },
  {
    key: "intel_core_ultra_series_3",
    score: 96,
    aliases: [
      /\bcore\s*ultra\s*(?:series\s*)?3\b/,
      /\bcore\s*ultra\s*[x]?[579]\s*3\d{2}[a-z]*\b/,
    ],
  },
  {
    key: "apple_m5_max",
    score: 99,
    aliases: [/\b(?:apple\s*)?m5\s*max\b/],
  },
  {
    key: "apple_m5_pro",
    score: 97,
    aliases: [/\b(?:apple\s*)?m5\s*pro\b/],
  },
  { key: "apple_m5", score: 93, aliases: [/\b(?:apple\s*)?m5\b/] },
  {
    key: "snapdragon_x_elite",
    score: 88,
    aliases: [/\bsnapdragon\s*x\s*elite\b/],
  },
  {
    key: "ryzen_ai",
    score: 87,
    aliases: [/\bryzen\s*ai\b/, /\bryzen\s*9\s*hx\b/],
  },
  {
    key: "intel_core_ultra",
    score: 86,
    aliases: [/\bcore\s*ultra\b/],
  },
  {
    key: "apple_m4",
    score: 88,
    aliases: [/\b(?:apple\s*)?m4(?:\s*(?:pro|max))?\b/],
  },
  {
    key: "modern_high_performance_cpu",
    score: 80,
    aliases: [
      /\bcore\s*i[79]\b/,
      /\bryzen\s*[79]\b/,
      /\bapple\s*m[23]\b/,
      /\bsnapdragon\s*x\s*plus\b/,
    ],
  },
]);

const GRAPHICS_FEATURE_RULES = Object.freeze([
  { key: "rtx_5090_laptop", score: 100, aliases: [/\brtx\s*5090\b/] },
  { key: "rtx_5080_laptop", score: 97, aliases: [/\brtx\s*5080\b/] },
  { key: "rtx_5070_ti_laptop", score: 93, aliases: [/\brtx\s*5070\s*ti\b/] },
  { key: "rtx_5070_laptop", score: 90, aliases: [/\brtx\s*5070\b/] },
  { key: "rtx_5060_laptop", score: 86, aliases: [/\brtx\s*5060\b/] },
  { key: "rtx_5050_laptop", score: 81, aliases: [/\brtx\s*5050\b/] },
  {
    key: "rtx_50_series_laptop",
    score: 85,
    aliases: [/\brtx\s*50(?:\s*series|\d{2})\b/],
  },
  { key: "rtx_4090_laptop", score: 93, aliases: [/\brtx\s*4090\b/] },
  { key: "rtx_4080_laptop", score: 89, aliases: [/\brtx\s*4080\b/] },
  {
    key: "rtx_40_series_laptop",
    score: 80,
    aliases: [/\brtx\s*40(?:\s*series|\d{2})\b/],
  },
  {
    key: "modern_radeon_graphics",
    score: 78,
    aliases: [/\bradeon\s*(?:rx|8060s|8050s|780m|890m)\b/],
  },
  {
    key: "intel_arc_graphics",
    score: 70,
    aliases: [/\bintel\s*arc\b/, /\barc\s*(?:graphics|gpu)\b/],
  },
  {
    key: "integrated_graphics",
    score: 56,
    aliases: [
      /\bintegrated\s*graphics\b/,
      /\bintel\s*(?:iris|uhd)\b/,
      /\bradeon\s*(?:graphics|igpu)\b/,
    ],
  },
  { key: "gddr7", score: 95, aliases: [/\bgddr7\b/] },
  { key: "dlss_4_5", score: 96, aliases: [/\bdlss\s*4\.5\b/] },
  { key: "dlss_4", score: 92, aliases: [/\bdlss\s*4\b/] },
  {
    key: "ai_frame_generation",
    score: 88,
    aliases: [/\bframe\s*generation\b/, /\bmulti\s*frame\s*generation\b/],
  },
  { key: "ray_tracing", score: 78, aliases: [/\bray\s*tracing\b/] },
]);

const DISPLAY_FEATURE_RULES = Object.freeze([
  { key: "tandem_oled", score: 99, aliases: [/\btandem\s*oled\b/] },
  { key: "oled", score: 94, aliases: [/\boled\b/] },
  { key: "mini_led", score: 91, aliases: [/\bmini[\s-]?led\b/] },
  {
    key: "liquid_retina_xdr",
    score: 90,
    aliases: [/\bliquid\s*retina\s*xdr\b/],
  },
  { key: "ips", score: 72, aliases: [/\bips\b/] },
  { key: "dolby_vision", score: 90, aliases: [/\bdolby\s*vision\b/] },
  { key: "hdr", score: 78, aliases: [/\bhdr(?:10|\s*true\s*black)?\b/] },
  {
    key: "wide_color_gamut",
    score: 80,
    aliases: [/\bdci[\s-]?p3\b/, /\bdisplay\s*p3\b/, /\b100%\s*srgb\b/],
  },
  {
    key: "adaptive_sync",
    score: 82,
    aliases: [/\bvrr\b/, /\bg[\s-]?sync\b/, /\bfreesync\b/, /\badaptive[\s-]?sync\b/],
  },
  {
    key: "touch_stylus",
    score: 74,
    aliases: [/\btouch(?:screen|\s*screen)?\b/, /\bstylus\b/, /\bpen\s*support\b/],
  },
]);

const MEMORY_STORAGE_FEATURE_RULES = Object.freeze([
  { key: "lpcamm2", score: 96, aliases: [/\blpcamm2\b/] },
  { key: "lpddr5x", score: 90, aliases: [/\blpddr5x\b/] },
  { key: "ddr5", score: 84, aliases: [/\bddr5\b/] },
  {
    key: "unified_memory",
    score: 88,
    aliases: [/\bunified\s*memory\b/],
  },
  {
    key: "upgradeable_memory",
    score: 76,
    aliases: [/\b(?:ram|memory)\s*(?:upgradeable|expandable)\b/, /\bsodimm\b/],
  },
  {
    key: "pcie_5_nvme",
    score: 95,
    aliases: [/\bpcie?\s*(?:gen\s*)?5(?:\.0)?\b/, /\bnvme\s*(?:gen\s*)?5\b/],
  },
  {
    key: "pcie_4_nvme",
    score: 84,
    aliases: [/\bpcie?\s*(?:gen\s*)?4(?:\.0)?\b/, /\bnvme\s*(?:gen\s*)?4\b/],
  },
  { key: "nvme_ssd", score: 76, aliases: [/\bnvme\b/, /\bssd\b/] },
  {
    key: "multiple_m2_slots",
    score: 78,
    aliases: [/\b(?:dual|2)\s*m\.?2\b/, /\bm\.?2\s*(?:slots?)?\s*x?\s*2\b/],
  },
]);

const BATTERY_PORTABILITY_FEATURE_RULES = Object.freeze([
  {
    key: "usb_c_power_delivery",
    score: 86,
    aliases: [/\busb[\s-]?c\s*(?:pd|power\s*delivery)\b/, /\bpower\s*delivery\b/],
  },
  {
    key: "fast_charging",
    score: 78,
    aliases: [/\bfast\s*charg(?:e|ing)\b/, /\brapid\s*charg(?:e|ing)\b/],
  },
  {
    key: "mil_std",
    score: 76,
    aliases: [/\bmil[\s-]?std\b/, /\bmilitary[\s-]?grade\b/],
  },
  {
    key: "metal_build",
    score: 72,
    aliases: [/\baluminium\b/, /\baluminum\b/, /\bmetal\s*(?:body|build|chassis)\b/],
  },
]);

const CONNECTIVITY_PORT_FEATURE_RULES = Object.freeze([
  { key: "wifi_7", score: 98, aliases: [/\bwi[\s-]?fi\s*7\b/, /\b802\.11be\b/] },
  { key: "wifi_6e", score: 89, aliases: [/\bwi[\s-]?fi\s*6e\b/] },
  { key: "wifi_6", score: 82, aliases: [/\bwi[\s-]?fi\s*6\b/, /\b802\.11ax\b/] },
  {
    key: "wifi_7_mlo",
    score: 96,
    aliases: [/\bmulti[\s-]?link\s*operation\b/, /\bmlo\b/],
  },
  {
    key: "wifi_7_320mhz",
    score: 95,
    aliases: [/\b320\s*mhz\b/],
  },
  { key: "bluetooth_6", score: 96, aliases: [/\bbluetooth\s*6(?:\.0)?\b/, /\bbt\s*6(?:\.0)?\b/] },
  { key: "bluetooth_5_4", score: 86, aliases: [/\bbluetooth\s*5\.4\b/, /\bbt\s*5\.4\b/] },
  { key: "thunderbolt_5", score: 98, aliases: [/\bthunderbolt\s*5\b/, /\btb\s*5\b/] },
  { key: "thunderbolt_4", score: 88, aliases: [/\bthunderbolt\s*4\b/, /\btb\s*4\b/] },
  { key: "usb4_v2", score: 97, aliases: [/\busb\s*4\s*(?:version|v)?\s*2\b/, /\busb4\s*2\.0\b/] },
  { key: "usb4", score: 88, aliases: [/\busb\s*4\b/, /\busb4\b/] },
  { key: "hdmi_2_1", score: 89, aliases: [/\bhdmi\s*2\.1\b/] },
  { key: "displayport_2_1", score: 91, aliases: [/\bdisplayport\s*2\.1\b/, /\bdp\s*2\.1\b/] },
  { key: "sd_uhs_ii", score: 82, aliases: [/\buhs[\s-]?ii\b/] },
  { key: "ethernet", score: 72, aliases: [/\bethernet\b/, /\brj[\s-]?45\b/] },
]);

const PRACTICAL_FEATURE_RULES = Object.freeze([
  { key: "webcam_4k", score: 96, aliases: [/\b4k\s*(?:webcam|camera)\b/] },
  { key: "webcam_1440p", score: 90, aliases: [/\b1440p\s*(?:webcam|camera)\b/] },
  { key: "webcam_1080p", score: 82, aliases: [/\b1080p\s*(?:webcam|camera)\b/, /\bfhd\s*(?:webcam|camera)\b/] },
  {
    key: "windows_hello",
    score: 84,
    aliases: [/\bwindows\s*hello\b/, /\bir\s*(?:webcam|camera)\b/],
  },
  {
    key: "privacy_shutter",
    score: 76,
    aliases: [/\bprivacy\s*shutter\b/, /\bcamera\s*shutter\b/],
  },
  {
    key: "fingerprint",
    score: 74,
    aliases: [/\bfingerprint\b/],
  },
  {
    key: "business_security",
    score: 86,
    aliases: [/\btpm\s*2\.0\b/, /\bpluton\b/, /\bvpro\b/, /\bamd\s*pro\b/],
  },
  {
    key: "premium_keyboard",
    score: 74,
    aliases: [/\bbacklit\s*keyboard\b/, /\brgb\s*keyboard\b/],
  },
  {
    key: "premium_audio",
    score: 80,
    aliases: [/\bdolby\s*atmos\b/, /\bstudio\s*(?:quality\s*)?mics?\b/],
  },
]);

const ALL_RULES = [
  ...PERFORMANCE_FEATURE_RULES,
  ...GRAPHICS_FEATURE_RULES,
  ...DISPLAY_FEATURE_RULES,
  ...MEMORY_STORAGE_FEATURE_RULES,
  ...BATTERY_PORTABILITY_FEATURE_RULES,
  ...CONNECTIVITY_PORT_FEATURE_RULES,
  ...PRACTICAL_FEATURE_RULES,
];

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
const roundOne = (value) => Number(Number(value).toFixed(1));
const roundTwo = (value) => Number(Number(value).toFixed(2));

const parseJsonObject = (value) => {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return {};

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed
      : {};
  } catch (err) {
    return {};
  }
};

const mergeObjects = (...values) =>
  values.reduce((result, value) => ({ ...result, ...parseJsonObject(value) }), {});

const normalizeText = (value) => {
  if (value == null) return "";
  if (Array.isArray(value)) return value.map(normalizeText).join(" ");
  if (typeof value === "object") {
    return Object.values(value).map(normalizeText).join(" ");
  }
  return String(value).toLowerCase();
};

const toFiniteNumber = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const match = String(value).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
};

const toFiniteScore100 = (value) => {
  const parsed = toFiniteNumber(value);
  return parsed == null ? null : clamp(parsed, 0, 100);
};

const readLargestNumber = (values, { min = null, max = null } = {}) => {
  const flattened = Array.isArray(values) ? values.flat(Infinity) : [values];
  const numbers = [];

  flattened.forEach((value) => {
    if (value == null) return;
    const matches = String(value).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/g);
    (matches || []).forEach((match) => {
      const parsed = Number(match);
      if (!Number.isFinite(parsed)) return;
      if (min != null && parsed < min) return;
      if (max != null && parsed > max) return;
      numbers.push(parsed);
    });
  });

  return numbers.length ? Math.max(...numbers) : null;
};

const matchFeatureRules = (text, rules) => {
  if (!text) return [];
  return rules.filter((rule) =>
    rule.aliases.some((pattern) => pattern.test(text)),
  );
};

const scoreFeatureSet = (
  text,
  rules,
  { diversityBoost = 1.5, maxScore = 100 } = {},
) => {
  const matches = matchFeatureRules(text, rules);
  if (!matches.length) return { score: null, keys: [] };
  const best = Math.max(...matches.map((rule) => rule.score));
  return {
    score: roundOne(
      clamp(best + Math.max(0, matches.length - 1) * diversityBoost, 0, maxScore),
    ),
    keys: matches.map((rule) => rule.key),
  };
};

const finalizeWeightedCategory = (metrics, { neutral = 56 } = {}) => {
  const items = Array.isArray(metrics) ? metrics : [];
  const totalWeight = items.reduce((sum, item) => sum + (item?.weight || 0), 0);
  if (!totalWeight) return { score: neutral, coverage: 0 };

  let knownWeight = 0;
  let weightedKnown = 0;
  items.forEach((item) => {
    const score = toFiniteScore100(item?.score);
    const weight = Number(item?.weight) || 0;
    if (score == null || weight <= 0) return;
    knownWeight += weight;
    weightedKnown += score * weight;
  });

  if (!knownWeight) return { score: neutral, coverage: 0 };
  const knownAverage = weightedKnown / knownWeight;
  const coverage = clamp(knownWeight / totalWeight, 0, 1);
  const confidenceFactor = 0.46 + coverage * 0.54;

  return {
    score: roundOne(clamp(neutral + (knownAverage - neutral) * confidenceFactor, 0, 100)),
    coverage: roundTwo(coverage),
  };
};

const scoreByThresholds = (value, thresholds) => {
  const parsed = toFiniteNumber(value);
  if (parsed == null) return null;
  for (const [minimum, score] of thresholds) {
    if (parsed >= minimum) return score;
  }
  return thresholds.length ? thresholds[thresholds.length - 1][1] : null;
};

const normalizeLaptopSections = (source) => {
  const item = parseJsonObject(source);
  const metadata = mergeObjects(item.metadata, item.meta);
  const specSections = mergeObjects(item.spec_sections, metadata.spec_sections);
  return {
    item,
    metadata,
    basicInfo: mergeObjects(item.basic_info, item.basicInfo, specSections.basic_info_json),
    performance: mergeObjects(item.cpu, item.performance, item.performance_json, specSections.performance_json),
    display: mergeObjects(item.display, item.display_json, specSections.display_json),
    memory: mergeObjects(item.memory, item.memory_json, specSections.memory_json),
    storage: mergeObjects(item.storage, item.storage_json, specSections.storage_json),
    battery: mergeObjects(item.battery, item.battery_json, specSections.battery_json),
    connectivity: mergeObjects(
      metadata.connectivity,
      item.connectivity,
      item.connectivity_json,
      specSections.connectivity_json,
    ),
    ports: mergeObjects(item.ports, item.ports_json, specSections.ports_json),
    multimedia: mergeObjects(item.multimedia, item.multimedia_json, specSections.multimedia_json),
    software: mergeObjects(item.software, item.software_json, specSections.software_json),
    security: mergeObjects(item.security, item.security_json, specSections.security_json),
    physical: mergeObjects(item.physical, item.physical_json, specSections.physical_json),
    camera: mergeObjects(item.camera, item.camera_json, specSections.camera_json),
    warranty: mergeObjects(metadata.warranty, item.warranty, item.warranty_json, specSections.warranty_json),
    variants: Array.isArray(item.variants)
      ? item.variants
      : Array.isArray(metadata.variants)
        ? metadata.variants
        : Array.isArray(specSections.variants_json)
          ? specSections.variants_json
          : [],
  };
};

const readLaptopPrice = ({ item, variants }) => {
  const direct = [
    item.price,
    item.base_price,
    item.starting_price,
    item.min_store_price,
    item.min_base_price,
  ];
  const prices = direct
    .map(toFiniteNumber)
    .filter((value) => value != null && value > 0);

  variants.forEach((variant) => {
    const basePrice = toFiniteNumber(
      variant?.base_price ?? variant?.price ?? variant?.amount,
    );
    if (basePrice != null && basePrice > 0) prices.push(basePrice);

    const stores = Array.isArray(variant?.store_prices)
      ? variant.store_prices
      : [];
    stores.forEach((store) => {
      const storePrice = toFiniteNumber(store?.price ?? store?.amount);
      if (storePrice != null && storePrice > 0) prices.push(storePrice);
    });
  });

  return prices.length ? Math.min(...prices) : null;
};

const getLaptopPriceBand = (price) => {
  const value = Number(price);
  if (!Number.isFinite(value) || value <= 0) return "unknown";
  if (value <= 30000) return "under_30000";
  if (value <= 50000) return "under_50000";
  if (value <= 70000) return "under_70000";
  if (value <= 100000) return "under_100000";
  if (value <= 150000) return "under_150000";
  return "above_150000";
};

const readRamGb = ({ memory, variants }) =>
  readLargestNumber(
    [
      memory.ram,
      memory.capacity,
      memory.size,
      memory.ram_capacity,
      ...variants.map((variant) => variant?.ram ?? variant?.memory),
    ],
    { min: 1, max: 256 },
  );

const readStorageGb = ({ storage, variants }) => {
  const values = [
    storage.capacity,
    storage.storage,
    storage.size,
    storage.ssd_capacity,
    ...variants.map((variant) => variant?.storage ?? variant?.storage_size),
  ];
  const sizes = [];
  values.forEach((value) => {
    const text = String(value || "").toLowerCase().replace(/,/g, "");
    const matches = text.matchAll(/(\d+(?:\.\d+)?)\s*(tb|gb)/g);
    for (const match of matches) {
      const amount = Number(match[1]);
      if (Number.isFinite(amount)) sizes.push(match[2] === "tb" ? amount * 1024 : amount);
    }
  });
  return sizes.length ? Math.max(...sizes) : readLargestNumber(values, { min: 32, max: 8192 });
};

const readBatteryWh = (battery, text) => {
  const direct = readLargestNumber(
    [battery.capacity_wh, battery.wh, battery.capacity, battery.battery_capacity],
    { min: 20, max: 200 },
  );
  if (direct != null) return direct;
  const match = text.match(/\b(\d+(?:\.\d+)?)\s*wh\b/);
  return match ? Number(match[1]) : null;
};

const readBatteryLifeHours = (battery, text) => {
  const direct = readLargestNumber(
    [battery.battery_life, battery.life, battery.backup_time],
    { min: 1, max: 40 },
  );
  if (direct != null) return direct;
  const match = text.match(/\b(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)\b/);
  return match ? Number(match[1]) : null;
};

const readWeightKg = (physical, text) => {
  const candidate = physical.weight ?? physical.weight_kg;
  const direct = toFiniteNumber(candidate);
  if (direct != null && direct > 0) return direct > 50 ? direct / 1000 : direct;
  const kgMatch = text.match(/\b(\d+(?:\.\d+)?)\s*kg\b/);
  if (kgMatch) return Number(kgMatch[1]);
  const gramMatch = text.match(/\b(\d+(?:\.\d+)?)\s*g\b/);
  return gramMatch ? Number(gramMatch[1]) / 1000 : null;
};

const readResolutionScore = (display, text) => {
  const resolutionText = normalizeText([
    display.resolution,
    display.display_resolution,
    display.pixel_resolution,
    text,
  ]);
  if (/\b(?:8k|7680\s*[x×]\s*4320)\b/.test(resolutionText)) return 100;
  if (/\b(?:4k|uhd|3840\s*[x×]\s*2160)\b/.test(resolutionText)) return 94;
  if (/\b(?:3\.2k|3200\s*[x×]\s*2000)\b/.test(resolutionText)) return 89;
  if (/\b(?:2\.8k|2880\s*[x×]\s*1800)\b/.test(resolutionText)) return 86;
  if (/\b(?:2\.5k|qhd|2560\s*[x×]\s*1600|2560\s*[x×]\s*1440)\b/.test(resolutionText)) return 82;
  if (/\b(?:fhd|full\s*hd|1920\s*[x×]\s*1200|1920\s*[x×]\s*1080)\b/.test(resolutionText)) return 68;
  return null;
};

const readNpuTops = (performance, text) => {
  const direct = readLargestNumber(
    [
      performance.npu_tops,
      performance.ai_tops,
      performance.npu,
      performance.neural_processing_unit,
    ],
    { min: 1, max: 300 },
  );
  if (direct != null) return direct;
  const match = text.match(/\b(\d+(?:\.\d+)?)\s*(?:npu\s*)?tops\b/);
  return match ? Number(match[1]) : null;
};

const collectMatchedFeatures = (...texts) => {
  const keys = new Set();
  texts.forEach((text) => {
    matchFeatureRules(text, ALL_RULES).forEach((rule) => keys.add(rule.key));
  });
  return [...keys];
};

const computeLaptopRawSpecScoreV2 = (source) => {
  const sections = normalizeLaptopSections(source);
  const {
    item,
    basicInfo,
    performance,
    display,
    memory,
    storage,
    battery,
    connectivity,
    ports,
    multimedia,
    software,
    security,
    physical,
    camera,
    warranty,
    variants,
  } = sections;

  const allText = normalizeText(sections);
  const performanceText = normalizeText([basicInfo, performance, software]);
  const graphicsText = normalizeText([performance, display]);
  const displayText = normalizeText(display);
  const memoryStorageText = normalizeText([memory, storage, variants]);
  const batteryPortabilityText = normalizeText([battery, physical]);
  const connectivityPortsText = normalizeText([connectivity, ports]);
  const practicalText = normalizeText([multimedia, security, camera, warranty, physical]);

  const npuTops = readNpuTops(performance, performanceText);
  const cpuCores = readLargestNumber(
    [performance.cores, performance.cpu_cores, performance.core_count],
    { min: 1, max: 128 },
  );
  const graphicsMemoryGb = readLargestNumber(
    [performance.vram, performance.graphics_memory, performance.gpu_memory],
    { min: 1, max: 48 },
  );
  const refreshRate = readLargestNumber(
    [display.refresh_rate, display.refresh_rate_hz, display.hz],
    { min: 30, max: 600 },
  );
  const brightnessNits = readLargestNumber(
    [display.brightness, display.brightness_nits, display.peak_brightness],
    { min: 100, max: 4000 },
  );
  const ramGb = readRamGb({ memory, variants });
  const storageGb = readStorageGb({ storage, variants });
  const batteryWh = readBatteryWh(battery, batteryPortabilityText);
  const batteryLifeHours = readBatteryLifeHours(battery, batteryPortabilityText);
  const weightKg = readWeightKg(physical, batteryPortabilityText);

  const performanceFeatures = scoreFeatureSet(performanceText, PERFORMANCE_FEATURE_RULES);
  const graphicsFeatures = scoreFeatureSet(graphicsText, GRAPHICS_FEATURE_RULES);
  const displayFeatures = scoreFeatureSet(displayText, DISPLAY_FEATURE_RULES);
  const memoryStorageFeatures = scoreFeatureSet(memoryStorageText, MEMORY_STORAGE_FEATURE_RULES);
  const batteryPortabilityFeatures = scoreFeatureSet(
    batteryPortabilityText,
    BATTERY_PORTABILITY_FEATURE_RULES,
  );
  const connectivityPortsFeatures = scoreFeatureSet(
    connectivityPortsText,
    CONNECTIVITY_PORT_FEATURE_RULES,
  );
  const practicalFeatures = scoreFeatureSet(practicalText, PRACTICAL_FEATURE_RULES);

  const performanceCategory = finalizeWeightedCategory(
    [
      { score: performanceFeatures.score, weight: 0.52 },
      {
        score: scoreByThresholds(npuTops, [
          [80, 100],
          [60, 96],
          [50, 91],
          [40, 86],
          [1, 68],
        ]),
        weight: 0.32,
      },
      {
        score: scoreByThresholds(cpuCores, [
          [20, 96],
          [16, 90],
          [12, 84],
          [8, 74],
          [4, 62],
          [1, 52],
        ]),
        weight: 0.16,
      },
    ],
    { neutral: 57 },
  );

  const graphicsCategory = finalizeWeightedCategory(
    [
      { score: graphicsFeatures.score, weight: 0.72 },
      {
        score: scoreByThresholds(graphicsMemoryGb, [
          [24, 100],
          [16, 96],
          [12, 90],
          [8, 82],
          [6, 74],
          [4, 66],
          [1, 56],
        ]),
        weight: 0.28,
      },
    ],
    { neutral: 55 },
  );

  const displayCategory = finalizeWeightedCategory(
    [
      { score: displayFeatures.score, weight: 0.34 },
      { score: readResolutionScore(display, displayText), weight: 0.28 },
      {
        score: scoreByThresholds(refreshRate, [
          [240, 98],
          [165, 93],
          [144, 89],
          [120, 84],
          [90, 75],
          [60, 62],
        ]),
        weight: 0.22,
      },
      {
        score: scoreByThresholds(brightnessNits, [
          [1200, 98],
          [800, 91],
          [600, 84],
          [500, 76],
          [400, 69],
          [250, 58],
        ]),
        weight: 0.16,
      },
    ],
    { neutral: 56 },
  );

  const memoryStorageCategory = finalizeWeightedCategory(
    [
      {
        score: scoreByThresholds(ramGb, [
          [64, 100],
          [32, 94],
          [24, 88],
          [16, 80],
          [8, 66],
          [4, 52],
        ]),
        weight: 0.36,
      },
      { score: memoryStorageFeatures.score, weight: 0.24 },
      {
        score: scoreByThresholds(storageGb, [
          [4096, 100],
          [2048, 94],
          [1024, 86],
          [512, 76],
          [256, 62],
          [128, 52],
        ]),
        weight: 0.4,
      },
    ],
    { neutral: 55 },
  );

  const batteryPortabilityCategory = finalizeWeightedCategory(
    [
      {
        score: scoreByThresholds(batteryWh, [
          [95, 96],
          [80, 90],
          [70, 84],
          [60, 76],
          [50, 68],
          [35, 56],
        ]),
        weight: 0.38,
      },
      {
        score: scoreByThresholds(batteryLifeHours, [
          [24, 100],
          [20, 94],
          [16, 88],
          [12, 79],
          [8, 68],
          [4, 56],
        ]),
        weight: 0.22,
      },
      {
        score:
          weightKg == null
            ? null
            : weightKg <= 1
              ? 98
              : weightKg <= 1.3
                ? 90
                : weightKg <= 1.6
                  ? 80
                  : weightKg <= 2
                    ? 70
                    : weightKg <= 2.5
                      ? 60
                      : 50,
        weight: 0.24,
      },
      { score: batteryPortabilityFeatures.score, weight: 0.16 },
    ],
    { neutral: 56 },
  );

  const connectivityPortsCategory = finalizeWeightedCategory(
    [{ score: connectivityPortsFeatures.score, weight: 1 }],
    { neutral: 55 },
  );

  const practicalCategory = finalizeWeightedCategory(
    [
      { score: practicalFeatures.score, weight: 0.72 },
      {
        score: /\b(?:2|3|4|5)\s*years?\b/.test(practicalText) ? 78 : null,
        weight: 0.28,
      },
    ],
    { neutral: 55 },
  );

  const categoryScores = {
    performance: performanceCategory.score,
    graphics: graphicsCategory.score,
    display: displayCategory.score,
    memory_storage: memoryStorageCategory.score,
    battery_portability: batteryPortabilityCategory.score,
    connectivity_ports: connectivityPortsCategory.score,
    practical: practicalCategory.score,
  };
  const categoryCoverage = {
    performance: performanceCategory.coverage,
    graphics: graphicsCategory.coverage,
    display: displayCategory.coverage,
    memory_storage: memoryStorageCategory.coverage,
    battery_portability: batteryPortabilityCategory.coverage,
    connectivity_ports: connectivityPortsCategory.coverage,
    practical: practicalCategory.coverage,
  };

  const weightedScore =
    performanceCategory.score * LAPTOP_CATEGORY_WEIGHTS.performance +
    graphicsCategory.score * LAPTOP_CATEGORY_WEIGHTS.graphics +
    displayCategory.score * LAPTOP_CATEGORY_WEIGHTS.display +
    memoryStorageCategory.score * LAPTOP_CATEGORY_WEIGHTS.memoryStorage +
    batteryPortabilityCategory.score * LAPTOP_CATEGORY_WEIGHTS.batteryPortability +
    connectivityPortsCategory.score * LAPTOP_CATEGORY_WEIGHTS.connectivityPorts +
    practicalCategory.score * LAPTOP_CATEGORY_WEIGHTS.practical;

  const coverageRatio =
    performanceCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.performance +
    graphicsCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.graphics +
    displayCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.display +
    memoryStorageCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.memoryStorage +
    batteryPortabilityCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.batteryPortability +
    connectivityPortsCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.connectivityPorts +
    practicalCategory.coverage * LAPTOP_CATEGORY_WEIGHTS.practical;

  const price = readLaptopPrice({ item, variants });
  if (coverageRatio <= 0) {
    return {
      rawScore: null,
      source: `${LAPTOP_SPEC_SCORE_VERSION}_unavailable`,
      price,
      priceBand: getLaptopPriceBand(price),
      featureCoverage: 0,
      breakdown: categoryScores,
      categoryCoverage,
      matchedFeatures: [],
      version: LAPTOP_SPEC_SCORE_VERSION,
    };
  }

  const completenessMultiplier = 0.88 + clamp(coverageRatio, 0, 1) * 0.12;
  return {
    rawScore: roundOne(clamp(weightedScore * completenessMultiplier, 0, 100)),
    source: `${LAPTOP_SPEC_SCORE_VERSION}_feature_raw`,
    price,
    priceBand: getLaptopPriceBand(price),
    featureCoverage: roundOne(coverageRatio * 100),
    breakdown: categoryScores,
    categoryCoverage,
    matchedFeatures: collectMatchedFeatures(
      allText,
      performanceText,
      graphicsText,
      displayText,
      memoryStorageText,
      batteryPortabilityText,
      connectivityPortsText,
      practicalText,
    ),
    version: LAPTOP_SPEC_SCORE_VERSION,
  };
};

module.exports = {
  LAPTOP_SPEC_SCORE_VERSION,
  computeLaptopRawSpecScoreV2,
};
