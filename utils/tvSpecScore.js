const TV_SPEC_SCORE_VERSION = "tv_spec_score_v1";

const TV_CATEGORY_WEIGHTS = Object.freeze({
  display: 0.35,
  motionGaming: 0.15,
  audioAv: 0.15,
  connectivityInputs: 0.15,
  smartAi: 0.15,
  practical: 0.05,
});

const DISPLAY_FEATURE_RULES = Object.freeze([
  { key: "micro_led", score: 99, aliases: [/\bmicro\s*led\b/] },
  {
    key: "rgb_mini_led",
    score: 97,
    aliases: [/\brgb\s*mini[\s-]?led\b/, /\bmicro\s*rgb\b/],
  },
  { key: "qd_oled", score: 97, aliases: [/\bqd[\s-]?oled\b/] },
  { key: "oled", score: 94, aliases: [/\boled\b/] },
  { key: "mini_led", score: 90, aliases: [/\bmini[\s-]?led\b/] },
  { key: "neo_qled", score: 87, aliases: [/\bneo\s*qled\b/] },
  { key: "qned", score: 84, aliases: [/\bqned\b/] },
  { key: "qled", score: 82, aliases: [/\bqled\b/, /\bquantum\s*dot\b/] },
  {
    key: "full_array_local_dimming",
    score: 84,
    aliases: [/\bfull\s*array\b/, /\bfald\b/, /\blocal\s*dimming\b/],
  },
  { key: "direct_led", score: 64, aliases: [/\bdirect\s*led\b/] },
  { key: "led", score: 55, aliases: [/\bled\b/, /\blcd\b/] },
]);

const HDR_FEATURE_RULES = Object.freeze([
  {
    key: "dolby_vision_2",
    score: 98,
    aliases: [/\bdolby\s*vision\s*2\b/, /\bdv2\b/],
  },
  {
    key: "hdr10_plus_advanced",
    score: 96,
    aliases: [/\bhdr10\+\s*advanced\b/],
  },
  {
    key: "dolby_vision_iq",
    score: 94,
    aliases: [/\bdolby\s*vision\s*iq\b/],
  },
  { key: "dolby_vision", score: 92, aliases: [/\bdolby\s*vision\b/] },
  {
    key: "hdr10_plus_adaptive",
    score: 91,
    aliases: [/\bhdr10\+\s*adaptive\b/],
  },
  {
    key: "hdr10_plus_gaming",
    score: 90,
    aliases: [/\bhdr10\+\s*gaming\b/],
  },
  { key: "hdr10_plus", score: 88, aliases: [/\bhdr10\+\b/] },
  { key: "hlg_plus", score: 82, aliases: [/\bhlg\+\b/] },
  { key: "hdr10", score: 78, aliases: [/\bhdr10\b/] },
  { key: "hlg", score: 74, aliases: [/\bhlg\b/] },
  { key: "hdr", score: 66, aliases: [/\bhdr\b/] },
]);

const MOTION_GAMING_FEATURE_RULES = Object.freeze([
  { key: "vrr", score: 88, aliases: [/\bvrr\b/, /\bvariable\s*refresh\b/] },
  { key: "allm", score: 84, aliases: [/\ballm\b/, /\bauto\s*low\s*latency\b/] },
  { key: "freesync_premium", score: 86, aliases: [/\bfreesync\s*premium\b/] },
  { key: "freesync", score: 80, aliases: [/\bfreesync\b/] },
  { key: "g_sync", score: 82, aliases: [/\bg[\s-]?sync\b/] },
  { key: "game_mode", score: 72, aliases: [/\bgame\s*mode\b/, /\bgaming\s*mode\b/] },
  { key: "low_input_lag", score: 76, aliases: [/\blow\s*input\s*lag\b/] },
  { key: "qms", score: 76, aliases: [/\bqms\b/, /\bquick\s*media\s*switching\b/] },
  { key: "qft", score: 76, aliases: [/\bqft\b/, /\bquick\s*frame\s*transport\b/] },
  { key: "memc", score: 68, aliases: [/\bmemc\b/, /\bmotion\s*smoothing\b/] },
]);

const AUDIO_AV_FEATURE_RULES = Object.freeze([
  { key: "dolby_atmos", score: 92, aliases: [/\bdolby\s*atmos\b/] },
  { key: "dts_x", score: 88, aliases: [/\bdts[\s-]?x\b/, /\bdts:x\b/] },
  { key: "dts_virtual_x", score: 82, aliases: [/\bdts\s*virtual[\s-]?x\b/] },
  { key: "dolby_audio", score: 78, aliases: [/\bdolby\s*audio\b/] },
  { key: "e_arc", score: 84, aliases: [/\bearc\b/, /\benhanced\s*audio\s*return\b/] },
  { key: "arc", score: 66, aliases: [/\barc\b/, /\baudio\s*return\s*channel\b/] },
  { key: "av1_codec", score: 86, aliases: [/\bav1\b/] },
  { key: "hevc_codec", score: 76, aliases: [/\bhevc\b/, /\bh\.?265\b/] },
  { key: "vp9_codec", score: 70, aliases: [/\bvp9\b/] },
  { key: "filmmaker_mode", score: 78, aliases: [/\bfilmmaker\s*mode\b/] },
  { key: "imax_enhanced", score: 82, aliases: [/\bimax\s*enhanced\b/] },
]);

const CONNECTIVITY_INPUT_RULES = Object.freeze([
  { key: "wifi_7", score: 96, aliases: [/\bwi[\s-]?fi\s*7\b/, /\b802\.11be\b/] },
  { key: "wifi_6e", score: 90, aliases: [/\bwi[\s-]?fi\s*6e\b/] },
  { key: "wifi_6", score: 84, aliases: [/\bwi[\s-]?fi\s*6\b/, /\b802\.11ax\b/] },
  { key: "wifi_5", score: 68, aliases: [/\bwi[\s-]?fi\s*5\b/, /\b802\.11ac\b/] },
  { key: "bluetooth_5_4", score: 84, aliases: [/\bbluetooth\s*5\.4\b/, /\bbt\s*5\.4\b/] },
  { key: "bluetooth_5_3", score: 82, aliases: [/\bbluetooth\s*5\.3\b/, /\bbt\s*5\.3\b/] },
  { key: "bluetooth_5", score: 76, aliases: [/\bbluetooth\s*5(?:\.\d)?\b/, /\bbt\s*5(?:\.\d)?\b/] },
  { key: "hdmi_2_2", score: 98, aliases: [/\bhdmi\s*2\.2\b/] },
  { key: "ultra96", score: 99, aliases: [/\bultra\s*96\b/, /\bultra96\b/] },
  { key: "hdmi_2_1", score: 90, aliases: [/\bhdmi\s*2\.1\b/] },
  { key: "four_k_240", score: 94, aliases: [/\b4k\s*@?\s*240\b/, /\b4k240\b/] },
  { key: "four_k_120", score: 88, aliases: [/\b4k\s*@?\s*120\b/, /\b4k120\b/] },
  { key: "ethernet_lan", score: 66, aliases: [/\bethernet\b/, /\blan\b/] },
  { key: "usb_3", score: 70, aliases: [/\busb\s*3(?:\.\d)?\b/] },
  { key: "usb", score: 62, aliases: [/\busb\b/] },
  { key: "optical_audio", score: 62, aliases: [/\boptical\b/, /\bspdif\b/, /\bs\/pdif\b/] },
  {
    key: "av_input",
    score: 60,
    aliases: [/\bav\s*(?:in|input)\b/, /\bcomposite\s*(?:in|input)\b/, /\brca\b/],
  },
  { key: "airplay", score: 76, aliases: [/\bairplay\b/] },
  { key: "chromecast", score: 76, aliases: [/\bchromecast\b/, /\bgoogle\s*cast\b/] },
  { key: "miracast", score: 68, aliases: [/\bmiracast\b/, /\bscreen\s*mirroring\b/] },
]);

const SMART_AI_FEATURE_RULES = Object.freeze([
  { key: "gemini", score: 90, aliases: [/\bgemini\b/] },
  { key: "vision_ai", score: 88, aliases: [/\bvision\s*ai\b/] },
  {
    key: "ai_upscaling",
    score: 88,
    aliases: [/\bai\s*upscal(?:e|ing)\b/, /\b8k\s*upscal(?:e|ing)\b/, /\b4k\s*upscal(?:e|ing)\b/],
  },
  {
    key: "ai_picture",
    score: 86,
    aliases: [/\bai\s*picture\b/, /\bai\s*brightness\b/, /\bai\s*scene\b/],
  },
  {
    key: "ai_sound",
    score: 82,
    aliases: [/\bai\s*sound\b/, /\bdialogue\s*enhanc/, /\bvoice\s*enhanc/],
  },
  { key: "ai_processor", score: 78, aliases: [/\bai\s*processor\b/, /\bneural\s*processor\b/] },
  { key: "google_tv", score: 84, aliases: [/\bgoogle\s*tv\b/] },
  { key: "webos", score: 82, aliases: [/\bwebos\b/] },
  { key: "tizen", score: 82, aliases: [/\btizen\b/] },
  { key: "fire_tv", score: 80, aliases: [/\bfire\s*tv\b/] },
  { key: "android_tv", score: 78, aliases: [/\bandroid\s*tv\b/] },
  {
    key: "voice_assistant",
    score: 74,
    aliases: [/\bvoice\s*assistant\b/, /\balexa\b/, /\bbixby\b/, /\bgoogle\s*assistant\b/],
  },
  { key: "matter", score: 74, aliases: [/\bmatter\b/] },
  { key: "smart_things", score: 72, aliases: [/\bsmartthings\b/, /\bsmart\s*things\b/] },
]);

const PRACTICAL_FEATURE_RULES = Object.freeze([
  { key: "three_year_warranty", score: 78, aliases: [/\b3\s*year\s*warranty\b/, /\bthree\s*year\s*warranty\b/] },
  { key: "two_year_warranty", score: 68, aliases: [/\b2\s*year\s*warranty\b/, /\btwo\s*year\s*warranty\b/] },
  { key: "energy_star", score: 74, aliases: [/\benergy\s*star\b/] },
  { key: "high_energy_rating", score: 70, aliases: [/\b5\s*star\b/, /\bfive\s*star\b/] },
  { key: "wall_mount", score: 62, aliases: [/\bwall\s*mount\b/, /\bvesa\b/] },
]);

const ALL_RULES = Object.freeze([
  ...DISPLAY_FEATURE_RULES,
  ...HDR_FEATURE_RULES,
  ...MOTION_GAMING_FEATURE_RULES,
  ...AUDIO_AV_FEATURE_RULES,
  ...CONNECTIVITY_INPUT_RULES,
  ...SMART_AI_FEATURE_RULES,
  ...PRACTICAL_FEATURE_RULES,
]);

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

const toFiniteScore100 = (value) => {
  const parsed = toFiniteNumber(value);
  return parsed == null ? null : clamp(parsed, 0, 100);
};

const normalizeText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[’]/g, "'")
    .replace(/[_/]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const toObject = (value) => {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return {};

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
};

const mergeObjects = (...values) =>
  Object.assign({}, ...values.map((value) => toObject(value)));

const collectTextFragments = (value, bucket = []) => {
  if (value == null) return bucket;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    const text = String(value).trim();
    if (text) bucket.push(text);
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
    const matches = value.replace(/,/g, "").match(/-?\d+(?:\.\d+)?/g);
    if (matches) {
      matches.forEach((match) => {
        const parsed = Number(match);
        if (Number.isFinite(parsed)) bucket.push(parsed);
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
  return numbers.length ? Math.max(...numbers) : null;
};

const parseResolution = (value) => {
  const text = normalizeText(value);
  if (!text) return null;

  if (/\b16k\b/.test(text)) return { pixels: 15360 * 8640, label: "16K" };
  if (/\b12k\b/.test(text)) return { pixels: 11520 * 6480, label: "12K" };
  if (/\b10k\b/.test(text)) return { pixels: 10240 * 4320, label: "10K" };
  if (/\b8k\b/.test(text)) return { pixels: 7680 * 4320, label: "8K" };
  if (/\b4k\b|\buhd\b|\bultra\s*hd\b/.test(text)) {
    return { pixels: 3840 * 2160, label: "4K" };
  }
  if (/\bfull\s*hd\b|\bfhd\b/.test(text)) {
    return { pixels: 1920 * 1080, label: "Full HD" };
  }
  if (/\bhd\s*ready\b/.test(text)) return { pixels: 1366 * 768, label: "HD" };

  const match = text.match(/(\d{3,5})\s*[x*]\s*(\d{3,5})/i);
  if (!match) return null;

  const width = Number(match[1]);
  const height = Number(match[2]);
  if (!Number.isFinite(width) || !Number.isFinite(height)) return null;

  return { pixels: width * height, label: `${width}x${height}` };
};

const scoreResolution = (value) => {
  const parsed = parseResolution(value);
  if (!parsed) return null;

  if (parsed.pixels >= 15360 * 8640) return 100;
  if (parsed.pixels >= 11520 * 6480) return 99;
  if (parsed.pixels >= 10240 * 4320) return 98;
  if (parsed.pixels >= 7680 * 4320) return 95;
  if (parsed.pixels >= 3840 * 2160) return 86;
  if (parsed.pixels >= 1920 * 1080) return 58;
  if (parsed.pixels >= 1366 * 768) return 46;
  return 38;
};

const scoreRefreshRate = (refreshRate) => {
  if (refreshRate == null) return null;
  if (refreshRate >= 240) return 98;
  if (refreshRate >= 165) return 92;
  if (refreshRate >= 144) return 89;
  if (refreshRate >= 120) return 84;
  if (refreshRate >= 100) return 74;
  if (refreshRate >= 60) return 58;
  return 42;
};

const scoreBrightness = (brightnessNits) => {
  if (brightnessNits == null) return null;
  if (brightnessNits >= 4000) return 99;
  if (brightnessNits >= 2500) return 96;
  if (brightnessNits >= 1500) return 90;
  if (brightnessNits >= 1000) return 82;
  if (brightnessNits >= 700) return 72;
  if (brightnessNits >= 500) return 62;
  if (brightnessNits >= 350) return 52;
  return 42;
};

const scoreAudioPower = (watts) => {
  if (watts == null) return null;
  if (watts >= 100) return 96;
  if (watts >= 70) return 90;
  if (watts >= 50) return 84;
  if (watts >= 40) return 76;
  if (watts >= 30) return 68;
  if (watts >= 20) return 58;
  return 46;
};

const scoreHdmiPortCount = (count) => {
  if (count == null) return null;
  if (count >= 4) return 84;
  if (count >= 3) return 74;
  if (count >= 2) return 62;
  if (count >= 1) return 48;
  return 35;
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
  { diversityBoost = 2, maxScore = 100 } = {},
) => {
  const matches = matchFeatureRules(text, rules);
  if (!matches.length) return { score: null, keys: [] };

  const best = Math.max(...matches.map((rule) => rule.score));
  const score = clamp(best + Math.max(0, matches.length - 1) * diversityBoost, 0, maxScore);

  return {
    score: roundOne(score),
    keys: matches.map((rule) => rule.key),
  };
};

const finalizeWeightedCategory = (metrics, { neutral = 55 } = {}) => {
  const items = Array.isArray(metrics) ? metrics : [];
  const totalWeight = items.reduce((sum, item) => sum + (item?.weight || 0), 0);

  if (!totalWeight) return { score: neutral, coverage: 0 };

  let knownWeight = 0;
  let weightedKnown = 0;

  for (const item of items) {
    const score = toFiniteScore100(item?.score);
    const weight = Number(item?.weight) || 0;
    if (score == null || weight <= 0) continue;
    knownWeight += weight;
    weightedKnown += score * weight;
  }

  if (!knownWeight) return { score: neutral, coverage: 0 };

  const knownAverage = weightedKnown / knownWeight;
  const coverage = clamp(knownWeight / totalWeight, 0, 1);
  const confidenceFactor = 0.45 + coverage * 0.55;
  const score = neutral + (knownAverage - neutral) * confidenceFactor;

  return {
    score: roundOne(clamp(score, 0, 100)),
    coverage: roundTwo(coverage),
  };
};

const readHdmiPortCount = (ports, text) => {
  const direct = readLargestNumber(
    [
      ports?.hdmi,
      ports?.hdmi_ports,
      ports?.hdmi_count,
      ports?.hdmi_inputs,
      ports?.hdmiInput,
    ],
    { min: 1, max: 8 },
  );
  if (direct != null) return direct;

  const match =
    text.match(/(?:\bhdmi\b[^0-9]{0,12})([1-8])\b/) ||
    text.match(/\b([1-8])\s*(?:x\s*)?hdmi\b/);
  if (!match) return null;

  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
};

const readTvPrice = (source) => {
  const direct = [
    source?.price,
    source?.base_price,
    source?.starting_price,
    source?.min_store_price,
    source?.min_base_price,
  ];

  for (const candidate of direct) {
    const parsed = toFiniteNumber(candidate);
    if (parsed != null && parsed > 0) return parsed;
  }

  const variants = Array.isArray(source?.variants)
    ? source.variants
    : Array.isArray(source?.variants_json)
      ? source.variants_json
      : [];
  const prices = [];

  for (const variant of variants) {
    const basePrice = toFiniteNumber(
      variant?.base_price ?? variant?.price ?? variant?.amount,
    );
    if (basePrice != null && basePrice > 0) prices.push(basePrice);

    const stores = Array.isArray(variant?.store_prices)
      ? variant.store_prices
      : [];
    for (const store of stores) {
      const storePrice = toFiniteNumber(store?.price ?? store?.amount);
      if (storePrice != null && storePrice > 0) prices.push(storePrice);
    }
  }

  return prices.length ? Math.min(...prices) : null;
};

const getTvPriceBand = (price) => {
  const value = Number(price);
  if (!Number.isFinite(value) || value <= 0) return "unknown";
  if (value <= 30000) return "under_30000";
  if (value <= 50000) return "under_50000";
  if (value <= 80000) return "under_80000";
  if (value <= 130000) return "under_130000";
  if (value <= 200000) return "under_200000";
  return "above_200000";
};

const normalizeTvSections = (source) => {
  const item = toObject(source);
  const keySpecs = mergeObjects(item.key_specs_json, item.keySpecsJson, item.key_specs);
  const display = mergeObjects(item.display, item.display_json, item.displayJson);
  const video = mergeObjects(
    item.video,
    item.video_engine_json,
    item.videoEngineJson,
    item.picture,
    item.picture_json,
  );
  const audio = mergeObjects(item.audio, item.audio_json, item.audioJson);
  const smart = mergeObjects(
    item.smart,
    item.smart_tv_json,
    item.smartTvJson,
    item.smart_features,
    item.smartFeatures,
  );
  const gaming = mergeObjects(item.gaming, item.gaming_json, item.gamingJson);
  const ports = mergeObjects(item.ports, item.ports_json, item.portsJson);
  const connectivity = mergeObjects(
    item.connectivity,
    item.connectivity_json,
    item.connectivityJson,
    item.network,
  );
  const power = mergeObjects(item.power, item.power_json, item.powerJson);
  const physical = mergeObjects(item.physical, item.physical_json, item.physicalJson);
  const productDetails = mergeObjects(
    item.product_details,
    item.product_details_json,
    item.productDetailsJson,
  );
  const warranty = mergeObjects(item.warranty, item.warranty_json, item.warrantyJson);

  return {
    item,
    keySpecs,
    display,
    video,
    audio,
    smart,
    gaming,
    ports,
    connectivity,
    power,
    physical,
    productDetails,
    warranty,
  };
};

const collectMatchedFeatures = (...texts) => {
  const keys = new Set();
  texts.forEach((text) => {
    matchFeatureRules(text, ALL_RULES).forEach((rule) => keys.add(rule.key));
  });
  return Array.from(keys).sort();
};

const computeTvRawSpecScoreV2 = (source) => {
  const sections = normalizeTvSections(source);
  const {
    item,
    keySpecs,
    display,
    video,
    audio,
    smart,
    gaming,
    ports,
    connectivity,
    power,
    physical,
    productDetails,
    warranty,
  } = sections;

  const displayText = buildTextBlob(keySpecs, display, video, productDetails);
  const motionGamingText = buildTextBlob(keySpecs, display, video, gaming, ports);
  const audioAvText = buildTextBlob(audio, video, ports, smart);
  const connectivityInputsText = buildTextBlob(connectivity, ports, smart, keySpecs);
  const smartAiText = buildTextBlob(smart, keySpecs, video, audio, productDetails);
  const practicalText = buildTextBlob(power, physical, warranty, productDetails);
  const allText = buildTextBlob(item, sections);

  const displayFeatures = scoreFeatureSet(displayText, DISPLAY_FEATURE_RULES, {
    diversityBoost: 1.5,
  });
  const hdrFeatures = scoreFeatureSet(displayText, HDR_FEATURE_RULES, {
    diversityBoost: 1.5,
  });
  const motionGamingFeatures = scoreFeatureSet(
    motionGamingText,
    MOTION_GAMING_FEATURE_RULES,
    { diversityBoost: 2 },
  );
  const audioAvFeatures = scoreFeatureSet(audioAvText, AUDIO_AV_FEATURE_RULES, {
    diversityBoost: 1.8,
  });
  const connectivityInputFeatures = scoreFeatureSet(
    connectivityInputsText,
    CONNECTIVITY_INPUT_RULES,
    { diversityBoost: 1.2 },
  );
  const smartAiFeatures = scoreFeatureSet(smartAiText, SMART_AI_FEATURE_RULES, {
    diversityBoost: 1.8,
  });
  const practicalFeatures = scoreFeatureSet(practicalText, PRACTICAL_FEATURE_RULES, {
    diversityBoost: 1,
  });

  const resolutionScore = scoreResolution(
    display.resolution ??
      display.screen_resolution ??
      keySpecs.resolution ??
      video.resolution ??
      displayText,
  );
  const refreshRate = readLargestNumber(
    [
      display.refresh_rate,
      display.refreshRate,
      display.max_refresh_rate,
      keySpecs.refresh_rate,
      keySpecs.refreshRate,
      gaming.refresh_rate,
      gaming.refreshRate,
      motionGamingText,
    ],
    { min: 50, max: 500 },
  );
  const brightnessScore = scoreBrightness(
    readLargestNumber(
      [
        display.peak_brightness,
        display.peak_brightness_nits,
        display.brightness,
        display.brightness_nits,
        video.peak_brightness,
        displayText,
      ],
      { min: 250, max: 10000 },
    ),
  );
  const audioPowerScore = scoreAudioPower(
    readLargestNumber(
      [
        audio.output_power,
        audio.speaker_output,
        audio.speaker_wattage,
        audio.sound_output,
        audio.total_speaker_power,
        keySpecs.audio_output,
        audio,
      ],
      { min: 5, max: 300 },
    ),
  );
  const hdmiCountScore = scoreHdmiPortCount(
    readHdmiPortCount(ports, connectivityInputsText),
  );

  const displayCategory = finalizeWeightedCategory(
    [
      { score: resolutionScore, weight: 0.24 },
      { score: displayFeatures.score, weight: 0.26 },
      { score: hdrFeatures.score, weight: 0.2 },
      { score: brightnessScore, weight: 0.16 },
      { score: scoreRefreshRate(refreshRate), weight: 0.14 },
    ],
    { neutral: 58 },
  );

  const motionGamingCategory = finalizeWeightedCategory(
    [
      { score: scoreRefreshRate(refreshRate), weight: 0.32 },
      { score: motionGamingFeatures.score, weight: 0.42 },
      {
        score: scoreFeatureSet(connectivityInputsText, CONNECTIVITY_INPUT_RULES, {
          diversityBoost: 1,
        }).score,
        weight: 0.18,
      },
      {
        score: /\blow\s*input\s*lag\b|\bgame\s*mode\b/.test(motionGamingText)
          ? 76
          : null,
        weight: 0.08,
      },
    ],
    { neutral: 55 },
  );

  const audioAvCategory = finalizeWeightedCategory(
    [
      { score: audioPowerScore, weight: 0.28 },
      { score: audioAvFeatures.score, weight: 0.42 },
      { score: hdrFeatures.score, weight: 0.16 },
      {
        score: /\bsubwoofer\b|\bwoofer\b/.test(audioAvText) ? 76 : null,
        weight: 0.08,
      },
      {
        score: /\b2\.1\b|\b4\.1\b|\b5\.1\b|\b7\.1\b/.test(audioAvText)
          ? 72
          : null,
        weight: 0.06,
      },
    ],
    { neutral: 54 },
  );

  const connectivityInputsCategory = finalizeWeightedCategory(
    [
      { score: connectivityInputFeatures.score, weight: 0.46 },
      { score: hdmiCountScore, weight: 0.2 },
      {
        score:
          /\bearc\b|\bhdmi\s*2\.2\b|\bhdmi\s*2\.1\b/.test(connectivityInputsText)
            ? 84
            : null,
        weight: 0.16,
      },
      {
        score:
          /\bairplay\b|\bchromecast\b|\bgoogle\s*cast\b|\bmiracast\b/.test(
            connectivityInputsText,
          )
            ? 76
            : null,
        weight: 0.1,
      },
      {
        score:
          /\bav\s*(?:in|input)\b|\bcomposite\s*(?:in|input)\b|\brca\b/.test(
            connectivityInputsText,
          )
            ? 60
            : null,
        weight: 0.08,
      },
    ],
    { neutral: 54 },
  );

  const smartAiCategory = finalizeWeightedCategory(
    [
      { score: smartAiFeatures.score, weight: 0.52 },
      {
        score:
          /\bnetflix\b|\byoutube\b|\bprime\s*video\b|\bhotstar\b|\bdisney\b/.test(
            smartAiText,
          )
            ? 74
            : null,
        weight: 0.16,
      },
      {
        score:
          /\bvoice\s*assistant\b|\balexa\b|\bbixby\b|\bgoogle\s*assistant\b|\bgemini\b/.test(
            smartAiText,
          )
            ? 78
            : null,
        weight: 0.14,
      },
      {
        score:
          /\bai\b|\bgemini\b|\bvision\s*ai\b|\bneural\b/.test(smartAiText)
            ? 84
            : null,
        weight: 0.18,
      },
    ],
    { neutral: 56 },
  );

  const practicalCategory = finalizeWeightedCategory(
    [
      { score: practicalFeatures.score, weight: 0.44 },
      {
        score:
          /\bmetal\b|\baluminium\b|\baluminum\b|\bbezel[-\s]?less\b|\bslim\b/.test(
            practicalText,
          )
            ? 70
            : null,
        weight: 0.18,
      },
      {
        score:
          /\b5\s*star\b|\bfive\s*star\b|\benergy\s*star\b/.test(practicalText)
            ? 74
            : null,
        weight: 0.2,
      },
      {
        score:
          /\b3\s*year\b|\bthree\s*year\b/.test(practicalText)
            ? 78
            : /\b2\s*year\b|\btwo\s*year\b/.test(practicalText)
              ? 68
              : null,
        weight: 0.18,
      },
    ],
    { neutral: 55 },
  );

  const categoryScores = {
    display: displayCategory.score,
    motion_gaming: motionGamingCategory.score,
    audio_av: audioAvCategory.score,
    connectivity_inputs: connectivityInputsCategory.score,
    smart_ai: smartAiCategory.score,
    practical: practicalCategory.score,
  };
  const categoryCoverage = {
    display: displayCategory.coverage,
    motion_gaming: motionGamingCategory.coverage,
    audio_av: audioAvCategory.coverage,
    connectivity_inputs: connectivityInputsCategory.coverage,
    smart_ai: smartAiCategory.coverage,
    practical: practicalCategory.coverage,
  };

  const weightedScore =
    displayCategory.score * TV_CATEGORY_WEIGHTS.display +
    motionGamingCategory.score * TV_CATEGORY_WEIGHTS.motionGaming +
    audioAvCategory.score * TV_CATEGORY_WEIGHTS.audioAv +
    connectivityInputsCategory.score * TV_CATEGORY_WEIGHTS.connectivityInputs +
    smartAiCategory.score * TV_CATEGORY_WEIGHTS.smartAi +
    practicalCategory.score * TV_CATEGORY_WEIGHTS.practical;

  const coverageRatio =
    displayCategory.coverage * TV_CATEGORY_WEIGHTS.display +
    motionGamingCategory.coverage * TV_CATEGORY_WEIGHTS.motionGaming +
    audioAvCategory.coverage * TV_CATEGORY_WEIGHTS.audioAv +
    connectivityInputsCategory.coverage * TV_CATEGORY_WEIGHTS.connectivityInputs +
    smartAiCategory.coverage * TV_CATEGORY_WEIGHTS.smartAi +
    practicalCategory.coverage * TV_CATEGORY_WEIGHTS.practical;

  const price = readTvPrice(source);

  if (coverageRatio <= 0) {
    return {
      rawScore: null,
      source: `${TV_SPEC_SCORE_VERSION}_unavailable`,
      price,
      priceBand: getTvPriceBand(price),
      featureCoverage: 0,
      breakdown: categoryScores,
      categoryCoverage,
      matchedFeatures: [],
      version: TV_SPEC_SCORE_VERSION,
    };
  }

  const completenessMultiplier = 0.82 + clamp(coverageRatio, 0, 1) * 0.18;
  const adjusted = clamp(weightedScore * completenessMultiplier, 0, 100);

  return {
    rawScore: roundOne(adjusted),
    source: `${TV_SPEC_SCORE_VERSION}_feature_raw`,
    price,
    priceBand: getTvPriceBand(price),
    featureCoverage: roundOne(coverageRatio * 100),
    breakdown: categoryScores,
    categoryCoverage,
    matchedFeatures: collectMatchedFeatures(
      allText,
      displayText,
      motionGamingText,
      audioAvText,
      connectivityInputsText,
      smartAiText,
      practicalText,
    ),
    version: TV_SPEC_SCORE_VERSION,
  };
};

module.exports = {
  TV_SPEC_SCORE_VERSION,
  computeTvRawSpecScoreV2,
};
