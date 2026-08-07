"use strict";

const SCORE_VERSION = "hooks-compare-v2.0";

const CATEGORY_META = Object.freeze({
  performance: { label: "Performance", weight: 0.2 },
  camera: { label: "Camera", weight: 0.18 },
  display: { label: "Display", weight: 0.14 },
  battery: { label: "Battery", weight: 0.16 },
  software: { label: "Software longevity", weight: 0.12 },
  portability: { label: "Design & portability", weight: 0.08 },
  connectivity: { label: "Connectivity", weight: 0.05 },
  value: { label: "Value", weight: 0.07 },
});

const toObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

const toArray = (value) => (Array.isArray(value) ? value : []);

const clamp = (value, min = 0, max = 100) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return min;
  return Math.max(min, Math.min(max, numeric));
};

const round = (value, digits = 1) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  const scale = 10 ** digits;
  return Math.round(numeric * scale) / scale;
};

const normalizeKey = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();

const flattenObject = (value, prefix = "", output = []) => {
  if (value == null) return output;
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      flattenObject(item, `${prefix}.${index}`, output),
    );
    return output;
  }
  if (typeof value === "object") {
    Object.entries(value).forEach(([key, item]) => {
      const path = prefix ? `${prefix}.${key}` : key;
      flattenObject(item, path, output);
    });
    return output;
  }
  output.push({
    path: prefix,
    key: normalizeKey(prefix.split(".").pop()),
    normalizedPath: normalizeKey(prefix),
    value,
  });
  return output;
};

const firstSpecValue = (flatEntries, aliases = []) => {
  const normalizedAliases = aliases.map(normalizeKey).filter(Boolean);
  for (const alias of normalizedAliases) {
    const exact = flatEntries.find(
      (entry) => entry.key === alias || entry.normalizedPath === alias,
    );
    if (exact && exact.value !== "") return exact.value;
  }
  for (const alias of normalizedAliases) {
    const partial = flatEntries.find(
      (entry) =>
        entry.normalizedPath.endsWith(alias) ||
        (alias.length >= 5 && entry.normalizedPath.includes(alias)),
    );
    if (partial && partial.value !== "") return partial.value;
  }
  return null;
};

const toText = (value) => {
  if (value == null) return "";
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) return value.map(toText).filter(Boolean).join(", ");
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return String(value);
  }
};

const numberFrom = (value) => {
  if (Number.isFinite(Number(value))) return Number(value);
  const match = toText(value)
    .replace(/,/g, "")
    .match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
};

const allNumbersFrom = (value) =>
  Array.from(
    toText(value)
      .replace(/,/g, "")
      .matchAll(/-?\d+(?:\.\d+)?/g),
  )
    .map((match) => Number(match[0]))
    .filter(Number.isFinite);

const booleanFrom = (value) => {
  if (typeof value === "boolean") return value;
  const text = toText(value).toLowerCase();
  if (!text) return null;
  if (/\b(no|none|not supported|unsupported|absent|false)\b/.test(text))
    return false;
  if (/\b(yes|supported|available|present|true|enabled|with)\b/.test(text))
    return true;
  return null;
};

const scoreRange = (value, min, max, invert = false) => {
  if (!Number.isFinite(Number(value)) || max <= min) return null;
  const normalized = clamp((Number(value) - min) / (max - min), 0, 1);
  return round((invert ? 1 - normalized : normalized) * 100, 1);
};

const weightedAverage = (items = []) => {
  let weighted = 0;
  let weights = 0;
  items.forEach(({ value, weight = 1 }) => {
    if (!Number.isFinite(Number(value)) || !Number.isFinite(Number(weight)))
      return;
    weighted += Number(value) * Number(weight);
    weights += Number(weight);
  });
  return weights > 0 ? weighted / weights : null;
};

const panelScore = (value) => {
  const text = toText(value).toLowerCase();
  if (!text) return null;
  if (/micro.?led/.test(text)) return 100;
  if (/ltpo.*oled|oled.*ltpo/.test(text)) return 97;
  if (/amoled|oled/.test(text)) return 91;
  if (/mini.?led/.test(text)) return 88;
  if (/ips/.test(text)) return 70;
  if (/lcd/.test(text)) return 58;
  if (/tft/.test(text)) return 45;
  return 60;
};

const ipRatingScore = (value) => {
  const text = toText(value).toUpperCase();
  if (!text) return null;
  if (/IP69/.test(text)) return 100;
  if (/IP68/.test(text)) return 96;
  if (/IP67/.test(text)) return 88;
  if (/IP66/.test(text)) return 80;
  if (/IP65/.test(text)) return 72;
  if (/IP64/.test(text)) return 66;
  if (/IP54/.test(text)) return 54;
  if (/IP53/.test(text)) return 48;
  if (/SPLASH|WATER.?REPELLENT|DUST.?RESIST/.test(text)) return 35;
  return null;
};

const chipsetScore = (value) => {
  const text = toText(value).toLowerCase();
  if (!text) return null;

  const rules = [
    [/apple\s*a19|a19\s*pro/, 100],
    [/apple\s*a18\s*pro/, 99],
    [/apple\s*a18/, 97],
    [/apple\s*a17\s*pro/, 96],
    [/snapdragon\s*8\s*elite/, 99],
    [/snapdragon\s*8\s*gen\s*4/, 99],
    [/snapdragon\s*8\s*gen\s*3/, 96],
    [/snapdragon\s*8\s*gen\s*2/, 92],
    [/snapdragon\s*8s\s*gen\s*4/, 92],
    [/snapdragon\s*8s\s*gen\s*3/, 88],
    [/snapdragon\s*7\+\s*gen\s*3/, 85],
    [/snapdragon\s*7\s*gen\s*4/, 84],
    [/snapdragon\s*7\s*gen\s*3/, 80],
    [/snapdragon\s*7s\s*gen\s*3/, 75],
    [/snapdragon\s*6\s*gen\s*4/, 70],
    [/snapdragon\s*6\s*gen\s*3/, 66],
    [/snapdragon\s*4\s*gen\s*3/, 55],
    [/dimensity\s*9500/, 100],
    [/dimensity\s*9400/, 99],
    [/dimensity\s*9300/, 96],
    [/dimensity\s*9200/, 92],
    [/dimensity\s*9000/, 89],
    [/dimensity\s*8400/, 88],
    [/dimensity\s*8350/, 86],
    [/dimensity\s*8300/, 84],
    [/dimensity\s*8200/, 81],
    [/dimensity\s*7400/, 78],
    [/dimensity\s*7300/, 75],
    [/dimensity\s*7200/, 73],
    [/dimensity\s*7050/, 68],
    [/dimensity\s*6300/, 58],
    [/tensor\s*g5/, 94],
    [/tensor\s*g4/, 89],
    [/tensor\s*g3/, 84],
    [/exynos\s*2500/, 95],
    [/exynos\s*2400/, 91],
    [/exynos\s*1580/, 77],
    [/exynos\s*1480/, 72],
    [/kirin\s*9020/, 88],
    [/unisoc\s*t820/, 61],
  ];

  const match = rules.find(([pattern]) => pattern.test(text));
  if (match) return match[1];
  if (/snapdragon\s*8/.test(text)) return 88;
  if (/snapdragon\s*7/.test(text)) return 76;
  if (/snapdragon\s*6/.test(text)) return 64;
  if (/snapdragon\s*4/.test(text)) return 52;
  if (/dimensity\s*9/.test(text)) return 90;
  if (/dimensity\s*8/.test(text)) return 82;
  if (/dimensity\s*7/.test(text)) return 72;
  if (/dimensity\s*6/.test(text)) return 58;
  return 55;
};

const osVersionScore = (value) => {
  const text = toText(value).toLowerCase();
  if (!text) return null;
  const match = text.match(/(?:android|ios)\s*(\d+(?:\.\d+)?)/i);
  if (!match) return 60;
  const version = Number(match[1]);
  if (/ios/.test(text)) return clamp(50 + (version - 14) * 9, 45, 100);
  return clamp(48 + (version - 10) * 10, 42, 100);
};

const wifiScore = (value) => {
  const text = toText(value).toLowerCase();
  if (!text) return null;
  if (/wi.?fi\s*7|802\.11be/.test(text)) return 100;
  if (/wi.?fi\s*6e/.test(text)) return 92;
  if (/wi.?fi\s*6|802\.11ax/.test(text)) return 86;
  if (/wi.?fi\s*5|802\.11ac/.test(text)) return 68;
  return 55;
};

const resolveVariant = (device, selection = {}) => {
  const variants = toArray(device?.variants);
  const selectedId = Number(selection?.variant_id ?? selection?.variantId);
  if (Number.isInteger(selectedId) && selectedId > 0) {
    const matched = variants.find(
      (variant) => Number(variant?.id ?? variant?.variant_id) === selectedId,
    );
    if (matched) return matched;
  }
  const index = Number(
    selection?.variant_index ?? selection?.variantIndex ?? 0,
  );
  if (Number.isInteger(index) && index >= 0 && variants[index])
    return variants[index];
  return variants[0] || null;
};

const resolveVariantPrice = (variant, device) => {
  const stores = [
    ...toArray(variant?.store_prices),
    ...toArray(variant?.storePrices),
  ];
  const storePrices = stores
    .map((store) => Number(store?.price))
    .filter((price) => Number.isFinite(price) && price > 0);
  const base = Number(
    variant?.base_price ?? variant?.basePrice ?? variant?.price,
  );
  const devicePrice = Number(
    device?.min_price ??
      device?.price ??
      device?.starting_price ??
      device?.base_price,
  );
  const candidates = [
    ...storePrices,
    Number.isFinite(base) && base > 0 ? base : null,
    Number.isFinite(devicePrice) && devicePrice > 0 ? devicePrice : null,
  ].filter((price) => Number.isFinite(price) && price > 0);
  return candidates.length ? Math.min(...candidates) : null;
};

const getVariantMemory = (variant, aliases) => {
  const attributes = toObject(variant?.attributes);
  for (const alias of aliases) {
    const value = variant?.[alias] ?? attributes?.[alias];
    if (value != null && value !== "") return value;
  }
  return null;
};

const buildSnapshot = (device, selection = {}, baseline = null) => {
  const performance = toObject(device?.performance);
  const display = toObject(device?.display);
  const camera = toObject(device?.camera);
  const battery = toObject(device?.battery);
  const buildDesign = toObject(device?.build_design ?? device?.buildDesign);
  const connectivity = {
    ...toObject(device?.connectivity),
    ...toObject(device?.network),
    ...toObject(device?.ports),
  };
  const softwareSource = {
    ...performance,
    ...toObject(device?.software),
    ...toObject(device?.multimedia),
  };
  const sensors = toObject(device?.sensors);
  const flat = {
    performance: flattenObject(performance),
    display: flattenObject(display),
    camera: flattenObject(camera),
    battery: flattenObject(battery),
    design: flattenObject(buildDesign),
    connectivity: flattenObject(connectivity),
    software: flattenObject(softwareSource),
    sensors: flattenObject(sensors),
  };

  const variant = resolveVariant(device, selection);
  const price = resolveVariantPrice(variant, device);
  const ram = numberFrom(
    getVariantMemory(variant, ["ram", "RAM", "memory"]) ||
      firstSpecValue(flat.performance, ["ram", "memory"]),
  );
  const storage = numberFrom(
    getVariantMemory(variant, ["storage", "rom", "internal_storage"]) ||
      firstSpecValue(flat.performance, ["storage", "internalstorage", "rom"]),
  );

  const chipset = firstSpecValue(flat.performance, [
    "chipset",
    "processor",
    "soc",
    "processorname",
    "chipsetname",
  ]);
  const cpuClock =
    Math.max(
      ...allNumbersFrom(
        firstSpecValue(flat.performance, [
          "cpuclock",
          "clockspeed",
          "maxclock",
          "cpu",
        ]),
      ).filter((number) => number > 1 && number < 10),
      0,
    ) || null;

  const panel = firstSpecValue(flat.display, [
    "displaytype",
    "paneltype",
    "type",
    "technology",
  ]);
  const refreshRate = numberFrom(
    firstSpecValue(flat.display, ["refreshrate", "screenrefreshrate", "hz"]),
  );
  const brightness = numberFrom(
    firstSpecValue(flat.display, ["peakbrightness", "brightness", "nits"]),
  );
  const screenSize = numberFrom(
    firstSpecValue(flat.display, ["screensize", "displaysize", "size"]),
  );
  const resolution = firstSpecValue(flat.display, [
    "resolution",
    "screenresolution",
  ]);
  const resolutionNumbers = allNumbersFrom(resolution);
  const resolutionPixels =
    resolutionNumbers.length >= 2
      ? resolutionNumbers
          .slice(0, 2)
          .reduce((total, number) => total * number, 1)
      : null;

  const mainCamera = firstSpecValue(flat.camera, [
    "maincameraresolution",
    "maincamera",
    "primarycamera",
    "rearcamera",
    "widecameraresolution",
  ]);
  const frontCamera = firstSpecValue(flat.camera, [
    "frontcameraresolution",
    "frontcamera",
    "selfiecamera",
  ]);
  const cameraText = flat.camera
    .map((entry) => `${entry.path}:${toText(entry.value)}`)
    .join(" ");
  const mainCameraMp = numberFrom(mainCamera);
  const frontCameraMp = numberFrom(frontCamera);
  const cameraNumbers = allNumbersFrom(cameraText).filter(
    (number) => number >= 1 && number <= 250,
  );
  const sensorCount = Math.max(
    1,
    ["main", "ultra", "telephoto", "periscope", "macro", "depth"].filter(
      (token) => new RegExp(token, "i").test(cameraText),
    ).length,
  );
  const ois = /\bois\b|optical image stabilization/i.test(cameraText);
  const telephoto = /telephoto|periscope|optical zoom/i.test(cameraText);
  const ultrawide = /ultra.?wide/i.test(cameraText);
  const videoText = toText(
    firstSpecValue(flat.camera, [
      "videorecording",
      "rearvideo",
      "video",
      "recording",
    ]),
  );
  const videoK = numberFrom(videoText.match(/\d+\s*k/i)?.[0] || null);

  const capacity = numberFrom(
    firstSpecValue(flat.battery, ["batterycapacity", "capacity", "mah"]),
  );
  const chargingW = numberFrom(
    firstSpecValue(flat.battery, [
      "wiredcharging",
      "chargingspeed",
      "fastcharging",
      "chargingwatt",
      "charging",
    ]),
  );
  const wirelessW = numberFrom(
    firstSpecValue(flat.battery, ["wirelesscharging", "wirelesschargingspeed"]),
  );
  const reverseCharging = booleanFrom(
    firstSpecValue(flat.battery, [
      "reversecharging",
      "reversewirelesscharging",
    ]),
  );

  const weight = numberFrom(
    firstSpecValue(flat.design, ["weight", "weightg", "weightgrams"]),
  );
  const thickness = numberFrom(
    firstSpecValue(flat.design, ["thickness", "depth", "dimensions"]),
  );
  const ipRating = firstSpecValue(flat.design, [
    "iprating",
    "waterdustresistance",
    "waterresistance",
    "durability",
  ]);

  const os = firstSpecValue(flat.software, [
    "operatingsystem",
    "os",
    "androidversion",
    "software",
  ]);
  const updateYears = numberFrom(
    firstSpecValue(flat.software, [
      "osupdates",
      "majorosupdates",
      "softwareupdates",
      "updatepolicy",
      "androidupdates",
    ]),
  );
  const securityYears = numberFrom(
    firstSpecValue(flat.software, [
      "securityupdates",
      "securitypatches",
      "supportyears",
    ]),
  );

  const fiveG = booleanFrom(
    firstSpecValue(flat.connectivity, ["5g", "network5g", "fifthgeneration"]),
  );
  const wifi = firstSpecValue(flat.connectivity, [
    "wifi",
    "wlan",
    "wirelesslan",
  ]);
  const bluetooth = firstSpecValue(flat.connectivity, [
    "bluetooth",
    "btversion",
  ]);
  const bluetoothVersion = numberFrom(bluetooth);
  const nfc = booleanFrom(firstSpecValue(flat.connectivity, ["nfc"]));
  const esim = booleanFrom(
    firstSpecValue(flat.connectivity, ["esim", "embeddedsim"]),
  );
  const usb = firstSpecValue(flat.connectivity, ["usb", "usbtype", "port"]);

  const baselineOverall = Number(
    baseline?.overallScore ?? baseline?.overall_score ?? baseline?.totalScore,
  );

  const performanceScore = weightedAverage([
    { value: chipsetScore(chipset), weight: 0.58 },
    { value: scoreRange(ram, 4, 24), weight: 0.18 },
    { value: scoreRange(storage, 64, 1024), weight: 0.1 },
    { value: scoreRange(cpuClock, 2, 4.5), weight: 0.07 },
    {
      value: Number.isFinite(baselineOverall) ? clamp(baselineOverall) : null,
      weight: 0.07,
    },
  ]);

  const displayScore = weightedAverage([
    { value: panelScore(panel), weight: 0.27 },
    { value: scoreRange(refreshRate, 60, 165), weight: 0.24 },
    { value: scoreRange(brightness, 500, 5000), weight: 0.2 },
    { value: scoreRange(resolutionPixels, 1_000_000, 4_500_000), weight: 0.19 },
    { value: scoreRange(screenSize, 5.8, 7.2), weight: 0.1 },
  ]);

  const cameraScore = weightedAverage([
    { value: scoreRange(mainCameraMp, 12, 200), weight: 0.2 },
    { value: ois ? 100 : cameraText ? 30 : null, weight: 0.18 },
    { value: telephoto ? 100 : cameraText ? 25 : null, weight: 0.17 },
    { value: ultrawide ? 100 : cameraText ? 30 : null, weight: 0.1 },
    { value: scoreRange(frontCameraMp, 8, 50), weight: 0.08 },
    { value: scoreRange(videoK, 1, 8), weight: 0.12 },
    { value: scoreRange(sensorCount, 1, 5), weight: 0.1 },
    { value: scoreRange(Math.max(...cameraNumbers, 0), 12, 200), weight: 0.05 },
  ]);

  const batteryScore = weightedAverage([
    { value: scoreRange(capacity, 3500, 7500), weight: 0.46 },
    { value: scoreRange(chargingW, 15, 150), weight: 0.36 },
    { value: scoreRange(wirelessW, 5, 80), weight: 0.11 },
    {
      value: reverseCharging == null ? null : reverseCharging ? 100 : 20,
      weight: 0.07,
    },
  ]);

  const softwareScore = weightedAverage([
    { value: osVersionScore(os), weight: 0.3 },
    { value: scoreRange(updateYears, 1, 7), weight: 0.48 },
    { value: scoreRange(securityYears, 2, 8), weight: 0.22 },
  ]);

  const portabilityScore = weightedAverage([
    { value: scoreRange(weight, 155, 245, true), weight: 0.55 },
    { value: scoreRange(thickness, 6.5, 10.5, true), weight: 0.25 },
    { value: ipRatingScore(ipRating), weight: 0.2 },
  ]);

  const connectivityScore = weightedAverage([
    { value: fiveG == null ? null : fiveG ? 100 : 20, weight: 0.25 },
    { value: wifiScore(wifi), weight: 0.25 },
    { value: scoreRange(bluetoothVersion, 4.2, 6), weight: 0.15 },
    { value: nfc == null ? null : nfc ? 100 : 20, weight: 0.15 },
    { value: esim == null ? null : esim ? 100 : 25, weight: 0.1 },
    {
      value: /3\.?[12]|type.?c/i.test(toText(usb)) ? 85 : usb ? 60 : null,
      weight: 0.1,
    },
  ]);

  const categoryScores = {
    performance: round(performanceScore ?? baselineOverall ?? 55, 1),
    camera: round(cameraScore ?? baselineOverall ?? 55, 1),
    display: round(displayScore ?? baselineOverall ?? 55, 1),
    battery: round(batteryScore ?? baselineOverall ?? 55, 1),
    software: round(softwareScore ?? 55, 1),
    portability: round(portabilityScore ?? 55, 1),
    connectivity: round(connectivityScore ?? 55, 1),
  };

  const fieldPresence = [
    chipset,
    ram,
    storage,
    panel,
    refreshRate,
    brightness,
    mainCamera,
    ois || telephoto || ultrawide ? true : null,
    capacity,
    chargingW,
    weight,
    ipRating,
    os,
    updateYears,
    fiveG,
    wifi,
    price,
    device?.launch_date,
  ];
  const comparableFieldCount = fieldPresence.filter(
    (value) => value !== null && value !== undefined && value !== "",
  ).length;
  const completeness = clamp(comparableFieldCount / fieldPresence.length, 0, 1);

  return {
    productId: Number(device?.product_id ?? device?.id),
    productName: device?.name || device?.product_name || "Device",
    productType: device?.product_type || "smartphone",
    brand: device?.brand_name || device?.brand || null,
    launchDate: device?.launch_date || null,
    variant: variant
      ? {
          id: Number(variant?.id ?? variant?.variant_id) || null,
          key: variant?.variant_key || null,
          ram: getVariantMemory(variant, ["ram", "RAM", "memory"]) || null,
          storage:
            getVariantMemory(variant, ["storage", "rom", "internal_storage"]) ||
            null,
        }
      : null,
    price,
    values: {
      chipset: toText(chipset) || null,
      ram,
      storage,
      panel: toText(panel) || null,
      refreshRate,
      brightness,
      screenSize,
      resolution: toText(resolution) || null,
      resolutionPixels,
      mainCamera: toText(mainCamera) || null,
      mainCameraMp,
      frontCamera: toText(frontCamera) || null,
      frontCameraMp,
      ois,
      telephoto,
      ultrawide,
      video: videoText || null,
      videoK,
      sensorCount,
      capacity,
      chargingW,
      wirelessW,
      reverseCharging,
      weight,
      thickness,
      ipRating: toText(ipRating) || null,
      os: toText(os) || null,
      updateYears,
      securityYears,
      fiveG,
      wifi: toText(wifi) || null,
      bluetooth: toText(bluetooth) || null,
      bluetoothVersion,
      nfc,
      esim,
      usb: toText(usb) || null,
      price,
    },
    categoryScores,
    comparableFieldCount,
    completeness: round(completeness, 2),
  };
};

const normalizeAcrossSnapshots = (snapshots) => {
  const qualities = snapshots.map((snapshot) =>
    weightedAverage(
      Object.entries(CATEGORY_META)
        .filter(([key]) => key !== "value")
        .map(([key, meta]) => ({
          value: snapshot.categoryScores[key],
          weight: meta.weight,
        })),
    ),
  );
  const validPrices = snapshots
    .map((snapshot) => snapshot.price)
    .filter((price) => Number.isFinite(price) && price > 0);
  const minPrice = validPrices.length ? Math.min(...validPrices) : null;
  const maxPrice = validPrices.length ? Math.max(...validPrices) : null;

  snapshots.forEach((snapshot, index) => {
    const quality = qualities[index] ?? 55;
    let valueScore = quality;
    if (
      Number.isFinite(snapshot.price) &&
      snapshot.price > 0 &&
      minPrice &&
      maxPrice
    ) {
      const priceAdvantage =
        maxPrice === minPrice
          ? 50
          : clamp(((maxPrice - snapshot.price) / (maxPrice - minPrice)) * 100);
      valueScore = quality * 0.72 + priceAdvantage * 0.28;
    }
    snapshot.categoryScores.value = round(valueScore, 1);
    snapshot.overallScore = round(
      weightedAverage(
        Object.entries(CATEGORY_META).map(([key, meta]) => ({
          value: snapshot.categoryScores[key],
          weight: meta.weight,
        })),
      ),
      1,
    );
  });
};

const compareMetric = ({
  snapshots,
  key,
  label,
  category,
  higherIsBetter = true,
  formatter = null,
  importanceThresholds = [0.08, 0.25],
  differenceType = "specification",
  reason = null,
}) => {
  const values = snapshots.map((snapshot) => snapshot.values[key]);
  const numeric = values.map((value) =>
    Number.isFinite(Number(value)) ? Number(value) : null,
  );
  const available = numeric.filter((value) => value != null);
  if (available.length < 2) return null;
  const min = Math.min(...available);
  const max = Math.max(...available);
  if (max === min) return null;
  const winnerValue = higherIsBetter ? max : min;
  const winnerIndex = numeric.findIndex((value) => value === winnerValue);
  if (winnerIndex < 0) return null;
  const base = Math.max(Math.abs(min), 1);
  const deltaRatio = Math.abs(max - min) / base;
  const importance =
    deltaRatio >= importanceThresholds[1]
      ? "high"
      : deltaRatio >= importanceThresholds[0]
        ? "medium"
        : "low";
  const winner = snapshots[winnerIndex];
  const displayValues = Object.fromEntries(
    snapshots.map((snapshot, index) => [
      String(snapshot.productId),
      formatter ? formatter(values[index]) : values[index],
    ]),
  );
  return {
    id: `${category}-${key}`,
    category,
    property: label,
    values: displayValues,
    winner_product_id: winner.productId,
    winner_name: winner.productName,
    importance,
    difference_type: differenceType,
    delta: round(Math.abs(max - min), 1),
    explanation:
      typeof reason === "function"
        ? reason({ winner, min, max, values, snapshots })
        : reason ||
          `${winner.productName} has the stronger listed ${label.toLowerCase()}.`,
  };
};

const buildKeyDifferences = (snapshots) => {
  const currency = (value) =>
    Number.isFinite(Number(value))
      ? `₹${Math.round(Number(value)).toLocaleString("en-IN")}`
      : null;
  const unit = (suffix) => (value) =>
    Number.isFinite(Number(value)) ? `${round(value, 1)}${suffix}` : null;

  const metricDefinitions = [
    {
      key: "price",
      label: "Current selected-variant price",
      category: "value",
      higherIsBetter: false,
      formatter: currency,
      importanceThresholds: [0.08, 0.2],
      differenceType: "price",
      reason: ({ winner }) =>
        `${winner.productName} is the lower-priced selected variant.`,
    },
    {
      key: "refreshRate",
      label: "Display refresh rate",
      category: "display",
      formatter: unit("Hz"),
      importanceThresholds: [0.08, 0.3],
    },
    {
      key: "brightness",
      label: "Peak brightness",
      category: "display",
      formatter: unit(" nits"),
      importanceThresholds: [0.12, 0.35],
    },
    {
      key: "mainCameraMp",
      label: "Main-camera resolution",
      category: "camera",
      formatter: unit("MP"),
      importanceThresholds: [0.25, 0.8],
      reason: ({ winner }) =>
        `${winner.productName} has the higher listed main-camera resolution; image quality still depends on sensor and processing.`,
    },
    {
      key: "capacity",
      label: "Battery capacity",
      category: "battery",
      formatter: unit("mAh"),
      importanceThresholds: [0.08, 0.22],
    },
    {
      key: "chargingW",
      label: "Wired charging",
      category: "battery",
      formatter: unit("W"),
      importanceThresholds: [0.15, 0.5],
    },
    {
      key: "wirelessW",
      label: "Wireless charging",
      category: "battery",
      formatter: unit("W"),
      importanceThresholds: [0.2, 0.6],
    },
    {
      key: "ram",
      label: "Selected RAM",
      category: "performance",
      formatter: unit("GB"),
      importanceThresholds: [0.2, 0.6],
    },
    {
      key: "storage",
      label: "Selected storage",
      category: "performance",
      formatter: unit("GB"),
      importanceThresholds: [0.25, 0.8],
    },
    {
      key: "weight",
      label: "Weight",
      category: "portability",
      higherIsBetter: false,
      formatter: unit("g"),
      importanceThresholds: [0.04, 0.12],
      differenceType: "tradeoff",
      reason: ({ winner }) =>
        `${winner.productName} is lighter and should be easier to carry.`,
    },
    {
      key: "thickness",
      label: "Thickness",
      category: "portability",
      higherIsBetter: false,
      formatter: unit("mm"),
      importanceThresholds: [0.04, 0.12],
      differenceType: "tradeoff",
    },
    {
      key: "updateYears",
      label: "Major software-update promise",
      category: "software",
      formatter: unit(" years"),
      importanceThresholds: [0.2, 0.5],
      differenceType: "longevity",
    },
  ];

  const differences = metricDefinitions
    .map((definition) => compareMetric({ snapshots, ...definition }))
    .filter(Boolean);

  const addBooleanDifference = (key, label, category, type = "feature") => {
    const values = snapshots.map((snapshot) => snapshot.values[key]);
    const available = values.filter((value) => typeof value === "boolean");
    if (
      available.length < 2 ||
      available.every((value) => value === available[0])
    )
      return;
    const winner = snapshots.find((snapshot) => snapshot.values[key] === true);
    if (!winner) return;
    differences.push({
      id: `${category}-${key}`,
      category,
      property: label,
      values: Object.fromEntries(
        snapshots.map((snapshot) => [
          String(snapshot.productId),
          snapshot.values[key] == null
            ? "Unknown"
            : snapshot.values[key]
              ? "Supported"
              : "Not supported",
        ]),
      ),
      winner_product_id: winner.productId,
      winner_name: winner.productName,
      importance: key === "ois" || key === "telephoto" ? "high" : "medium",
      difference_type: type,
      delta: null,
      explanation: `${winner.productName} includes ${label.toLowerCase()} while at least one compared phone does not.`,
    });
  };

  addBooleanDifference("ois", "Optical image stabilisation", "camera");
  addBooleanDifference(
    "telephoto",
    "Telephoto or optical zoom camera",
    "camera",
  );
  addBooleanDifference("ultrawide", "Ultra-wide camera", "camera");
  addBooleanDifference("fiveG", "5G support", "connectivity");
  addBooleanDifference("nfc", "NFC", "connectivity");
  addBooleanDifference("esim", "eSIM", "connectivity");

  snapshots.forEach((snapshot) => {
    const categoryWinner = Object.entries(snapshot.categoryScores)
      .filter(([key]) => key !== "value")
      .sort((left, right) => right[1] - left[1])[0];
    if (!categoryWinner) return;
  });

  return differences
    .sort((left, right) => {
      const rank = { high: 3, medium: 2, low: 1 };
      return (rank[right.importance] || 0) - (rank[left.importance] || 0);
    })
    .slice(0, 18);
};

const buildCommonFeatures = (snapshots) => {
  const definitions = [
    ["panel", "Display panel", "display"],
    ["refreshRate", "Display refresh rate", "display"],
    ["capacity", "Battery capacity", "battery"],
    ["chargingW", "Wired charging", "battery"],
    ["ois", "Optical image stabilisation", "camera"],
    ["telephoto", "Telephoto camera", "camera"],
    ["ultrawide", "Ultra-wide camera", "camera"],
    ["fiveG", "5G support", "connectivity"],
    ["nfc", "NFC", "connectivity"],
    ["ram", "Selected RAM", "performance"],
    ["storage", "Selected storage", "performance"],
  ];

  return definitions
    .map(([key, label, category]) => {
      const values = snapshots.map((snapshot) => snapshot.values[key]);
      if (values.some((value) => value == null || value === "")) return null;
      const normalized = values.map((value) => toText(value).toLowerCase());
      if (!normalized.every((value) => value === normalized[0])) return null;
      return {
        id: `${category}-${key}`,
        category,
        property: label,
        value:
          typeof values[0] === "boolean"
            ? values[0]
              ? "Supported"
              : "Not supported"
            : values[0],
      };
    })
    .filter(Boolean)
    .slice(0, 12);
};

const buildCategoryVerdicts = (snapshots) => {
  const categoryVerdicts = [];
  const categoryWinners = {};

  Object.entries(CATEGORY_META).forEach(([key, meta]) => {
    const rows = snapshots
      .map((snapshot) => ({
        product_id: snapshot.productId,
        product_name: snapshot.productName,
        score: round(snapshot.categoryScores[key], 1),
        completeness: snapshot.completeness,
      }))
      .sort((left, right) => right.score - left.score);
    if (!rows.length) return;
    const top = rows[0];
    const second = rows[1] || top;
    const gap = round(top.score - second.score, 1) || 0;
    const isTie = gap < 1.5;
    const confidence = clamp(
      snapshots.reduce((sum, snapshot) => sum + snapshot.completeness, 0) /
        Math.max(1, snapshots.length) +
        Math.min(0.12, gap / 100),
      0.25,
      0.98,
    );

    const winner = isTie
      ? null
      : {
          product_id: top.product_id,
          product_name: top.product_name,
          score: top.score,
        };
    if (winner) {
      categoryWinners[key] = {
        product_id: winner.product_id,
        product_name: winner.product_name,
        score: winner.score,
        reason: `${winner.product_name} has the strongest ${meta.label.toLowerCase()} score in this comparison.`,
        confidence: round(confidence, 2),
      };
    }

    categoryVerdicts.push({
      category: key,
      label: meta.label,
      winner_product_id: winner?.product_id || null,
      winner_name: winner?.product_name || null,
      is_tie: isTie,
      gap,
      confidence: round(confidence, 2),
      scores: Object.fromEntries(
        rows.map((row) => [String(row.product_id), row.score]),
      ),
      reason: isTie
        ? `${meta.label} is closely matched across the selected phones.`
        : `${top.product_name} leads ${meta.label.toLowerCase()} by ${gap} points.`,
    });
  });

  return { categoryVerdicts, categoryWinners };
};

const buildTradeoffs = (snapshots, categoryVerdicts, keyDifferences) => {
  return snapshots.map((snapshot) => {
    const wins = categoryVerdicts
      .filter((verdict) => verdict.winner_product_id === snapshot.productId)
      .map((verdict) => verdict.label);
    const losses = categoryVerdicts
      .filter(
        (verdict) =>
          verdict.winner_product_id &&
          verdict.winner_product_id !== snapshot.productId,
      )
      .map((verdict) => verdict.label);
    const differenceWins = keyDifferences
      .filter(
        (difference) => difference.winner_product_id === snapshot.productId,
      )
      .map((difference) => difference.property);
    const differenceLosses = keyDifferences
      .filter(
        (difference) =>
          difference.winner_product_id &&
          difference.winner_product_id !== snapshot.productId,
      )
      .map((difference) => difference.property);

    return {
      product_id: snapshot.productId,
      product_name: snapshot.productName,
      choose_if: Array.from(new Set([...wins, ...differenceWins])).slice(0, 4),
      gain: Array.from(new Set([...differenceWins, ...wins])).slice(0, 5),
      give_up: Array.from(new Set([...differenceLosses, ...losses])).slice(
        0,
        5,
      ),
    };
  });
};

const buildUpgradeStory = (snapshots, keyDifferences, commonFeatures) => {
  const dated = snapshots
    .map((snapshot) => ({
      snapshot,
      time: snapshot.launchDate ? new Date(snapshot.launchDate).getTime() : NaN,
    }))
    .filter((entry) => Number.isFinite(entry.time))
    .sort((left, right) => left.time - right.time);
  if (dated.length < 2) {
    return {
      available: false,
      relationship: "switch",
      title: "What changes when you switch?",
      summary:
        "Launch dates are incomplete, so Hooks cannot reliably identify the newer phone.",
      major_gains: [],
      minor_gains: [],
      mostly_unchanged: commonFeatures.slice(0, 5).map((item) => item.property),
      tradeoffs: [],
    };
  }

  const older = dated[0].snapshot;
  const newer = dated[dated.length - 1].snapshot;
  const gapDays = Math.max(
    0,
    Math.round((dated[dated.length - 1].time - dated[0].time) / 86_400_000),
  );
  const gapMonths = round(gapDays / 30.44, 1);
  const sameBrand =
    older.brand &&
    newer.brand &&
    older.brand.toLowerCase() === newer.brand.toLowerCase();
  const gains = keyDifferences.filter(
    (difference) => difference.winner_product_id === newer.productId,
  );
  const regressions = keyDifferences.filter(
    (difference) => difference.winner_product_id === older.productId,
  );

  return {
    available: true,
    relationship: sameBrand ? "generation" : "switch",
    title: sameBrand
      ? `Is ${newer.productName} an upgrade?`
      : `What changes with ${newer.productName}?`,
    older_product_id: older.productId,
    older_product_name: older.productName,
    newer_product_id: newer.productId,
    newer_product_name: newer.productName,
    launch_gap_days: gapDays,
    launch_gap_months: gapMonths,
    summary: `${newer.productName} launched about ${gapMonths} months after ${older.productName}. Newer does not automatically mean better in every category.`,
    major_gains: gains
      .filter((difference) => difference.importance === "high")
      .map((difference) => difference.property)
      .slice(0, 5),
    minor_gains: gains
      .filter((difference) => difference.importance !== "high")
      .map((difference) => difference.property)
      .slice(0, 5),
    mostly_unchanged: commonFeatures.slice(0, 5).map((item) => item.property),
    tradeoffs: regressions.map((difference) => difference.property).slice(0, 5),
  };
};

const buildUseCasePicks = (snapshots, categoryVerdicts) => {
  const pickByCategory = (useCase, label, categoryKeys, reason) => {
    const combined = snapshots
      .map((snapshot) => ({
        snapshot,
        score: weightedAverage(
          categoryKeys.map((key) => ({
            value: snapshot.categoryScores[key],
            weight: 1,
          })),
        ),
      }))
      .sort((left, right) => right.score - left.score);
    if (!combined[0] || !Number.isFinite(combined[0].score)) return null;
    const second = combined[1] || combined[0];
    const gap = combined[0].score - second.score;
    return {
      use_case: useCase,
      label,
      winner_product_id: combined[0].snapshot.productId,
      winner_name: combined[0].snapshot.productName,
      score: round(combined[0].score, 1),
      confidence: round(clamp(0.55 + gap / 50, 0.45, 0.96), 2),
      reason:
        typeof reason === "function"
          ? reason(combined[0].snapshot)
          : `${combined[0].snapshot.productName} has the strongest relevant category score.`,
    };
  };

  return [
    pickByCategory(
      "gaming",
      "Best for gaming",
      ["performance", "display"],
      (snapshot) =>
        `${snapshot.productName} combines the strongest performance and display scores.`,
    ),
    pickByCategory(
      "photography",
      "Best for photography",
      ["camera"],
      (snapshot) =>
        `${snapshot.productName} has the stronger composite camera specification profile.`,
    ),
    pickByCategory(
      "battery",
      "Best for battery life",
      ["battery"],
      (snapshot) =>
        `${snapshot.productName} leads the battery and charging comparison.`,
    ),
    pickByCategory(
      "long_term",
      "Best for long-term use",
      ["software", "connectivity"],
      (snapshot) =>
        `${snapshot.productName} has the stronger software-longevity and connectivity profile.`,
    ),
    pickByCategory(
      "compact",
      "Best for portability",
      ["portability"],
      (snapshot) =>
        `${snapshot.productName} is the easier phone to carry based on available size and durability data.`,
    ),
    pickByCategory(
      "value",
      "Best value",
      ["value"],
      (snapshot) =>
        `${snapshot.productName} gives the strongest specification-to-price balance for the selected variant.`,
    ),
    pickByCategory(
      "premium",
      "Best premium experience",
      ["performance", "camera", "display"],
      (snapshot) =>
        `${snapshot.productName} has the best combined performance, camera and display package.`,
    ),
  ].filter(Boolean);
};

const buildPriceVerdict = (snapshots, categoryVerdicts) => {
  const priced = snapshots
    .filter((snapshot) => Number.isFinite(snapshot.price) && snapshot.price > 0)
    .sort((left, right) => left.price - right.price);
  if (priced.length < 2) {
    return {
      available: false,
      label: "Insufficient current-price data",
      summary:
        "Hooks needs selected-variant prices for at least two phones to judge the premium.",
      difference: null,
      percentage: null,
      premium_product_id: null,
      cheaper_product_id: null,
      extra_cost_provides: [],
      cheaper_phone_keeps: [],
    };
  }
  const cheaper = priced[0];
  const premium = priced[priced.length - 1];
  const difference = premium.price - cheaper.price;
  const percentage = (difference / cheaper.price) * 100;
  const premiumWins = categoryVerdicts
    .filter(
      (verdict) =>
        verdict.winner_product_id === premium.productId && verdict.gap >= 3,
    )
    .map((verdict) => verdict.label);
  const cheaperWins = categoryVerdicts
    .filter(
      (verdict) =>
        verdict.winner_product_id === cheaper.productId && verdict.gap >= 3,
    )
    .map((verdict) => verdict.label);

  let label = "Premium is partly justified";
  if (percentage <= 8 && premiumWins.length >= 2)
    label = "Worth the small premium";
  else if (premiumWins.length >= 3 && percentage <= 25)
    label = "Worth the premium for its strengths";
  else if (premiumWins.length <= 1 && cheaperWins.length >= 1)
    label = "Better value at the lower price";
  else if (percentage >= 35 && premiumWins.length < 3)
    label = "Price premium is difficult to justify";

  return {
    available: true,
    label,
    summary: `${premium.productName} costs ₹${Math.round(difference).toLocaleString("en-IN")} more than ${cheaper.productName}. ${
      premiumWins.length
        ? `The extra cost mainly buys ${premiumWins.slice(0, 3).join(", ").toLowerCase()}.`
        : "The available specification data does not show a clear major advantage for the premium."
    }`,
    difference: round(difference, 0),
    percentage: round(percentage, 1),
    premium_product_id: premium.productId,
    premium_product_name: premium.productName,
    cheaper_product_id: cheaper.productId,
    cheaper_product_name: cheaper.productName,
    extra_cost_provides: premiumWins.slice(0, 5),
    cheaper_phone_keeps: cheaperWins.slice(0, 5),
  };
};

const buildDecisionComparison = ({
  devices = [],
  selections = {},
  ranking = [],
} = {}) => {
  const rankingById = new Map(
    toArray(ranking).map((row) => [
      String(row?.productId ?? row?.product_id ?? row?.id),
      row,
    ]),
  );
  const snapshots = toArray(devices)
    .map((device) => {
      const productId = Number(device?.product_id ?? device?.id);
      if (!Number.isInteger(productId) || productId <= 0) return null;
      return buildSnapshot(
        device,
        selections?.[String(productId)] || selections?.[productId] || {},
        rankingById.get(String(productId)) || null,
      );
    })
    .filter(Boolean);

  normalizeAcrossSnapshots(snapshots);
  const { categoryVerdicts, categoryWinners } =
    buildCategoryVerdicts(snapshots);
  const keyDifferences = buildKeyDifferences(snapshots);
  const commonFeatures = buildCommonFeatures(snapshots);
  const tradeoffs = buildTradeoffs(snapshots, categoryVerdicts, keyDifferences);
  const upgradeStory = buildUpgradeStory(
    snapshots,
    keyDifferences,
    commonFeatures,
  );
  const useCasePicks = buildUseCasePicks(snapshots, categoryVerdicts);
  const priceVerdict = buildPriceVerdict(snapshots, categoryVerdicts);

  const ordered = [...snapshots].sort(
    (left, right) => right.overallScore - left.overallScore,
  );
  const top = ordered[0] || null;
  const second = ordered[1] || top;
  const overallGap =
    top && second ? round(top.overallScore - second.overallScore, 1) || 0 : 0;
  const isClose = overallGap < 2.5;
  const overallConfidenceBase = snapshots.length
    ? snapshots.reduce((sum, snapshot) => sum + snapshot.completeness, 0) /
      snapshots.length
    : 0;
  const priceCoverage = snapshots.length
    ? snapshots.filter((snapshot) => Number.isFinite(snapshot.price)).length /
      snapshots.length
    : 0;
  const confidence = clamp(
    overallConfidenceBase * 0.78 +
      priceCoverage * 0.12 +
      Math.min(0.1, overallGap / 100),
    0.2,
    0.98,
  );
  const confidenceLevel =
    confidence >= 0.78 ? "high" : confidence >= 0.55 ? "medium" : "low";
  const comparedFields = snapshots.reduce(
    (minimum, snapshot) => Math.min(minimum, snapshot.comparableFieldCount),
    snapshots[0]?.comparableFieldCount ?? 0,
  );

  const overallWinner = top
    ? {
        product_id: top.productId,
        product_name: top.productName,
        overall_score: top.overallScore,
        confidence: round(confidence, 2),
        reason: isClose
          ? `${top.productName} has a narrow overall lead, so the better choice depends on your priorities.`
          : `${top.productName} has the strongest balance across the weighted comparison categories.`,
      }
    : null;

  const warnings = [];
  if (priceCoverage < 1)
    warnings.push("Some selected-variant prices are unavailable.");
  if (confidenceLevel === "low")
    warnings.push("Several category-critical specifications are missing.");
  if (!upgradeStory.available)
    warnings.push("Launch-date comparison is incomplete.");
  if (!categoryWinners.camera)
    warnings.push(
      "Camera specifications are too close or incomplete for a confident standalone winner.",
    );

  const deviceResults = snapshots.map((snapshot, index) => {
    const rank =
      ordered.findIndex((item) => item.productId === snapshot.productId) + 1;
    const deviceTradeoff = tradeoffs.find(
      (item) => item.product_id === snapshot.productId,
    );
    const wonCategories = categoryVerdicts
      .filter((verdict) => verdict.winner_product_id === snapshot.productId)
      .map((verdict) => verdict.label);
    return {
      product_id: snapshot.productId,
      product_name: snapshot.productName,
      rank,
      overall_score: snapshot.overallScore,
      confidence: round(snapshot.completeness, 2),
      price: snapshot.price,
      selected_variant: snapshot.variant,
      category_scores: snapshot.categoryScores,
      breakdown: snapshot.categoryScores,
      reasons: wonCategories
        .slice(0, 4)
        .map((label) => `Leads in ${label.toLowerCase()}`),
      strengths: deviceTradeoff?.gain || [],
      tradeoffs: deviceTradeoff?.give_up || [],
      comparable_fields: snapshot.comparableFieldCount,
    };
  });

  return {
    scoreVersion: SCORE_VERSION,
    productType: snapshots[0]?.productType || "",
    generatedAt: new Date().toISOString(),
    devices: deviceResults,
    scores: deviceResults,
    overallWinner,
    overallVerdict: {
      winner_product_id: overallWinner?.product_id || null,
      winner_name: overallWinner?.product_name || null,
      confidence: round(confidence, 2),
      confidence_level: confidenceLevel,
      is_close_comparison: isClose,
      score_gap: overallGap,
      reason:
        overallWinner?.reason ||
        "Not enough data to produce an overall verdict.",
      compared_fields: comparedFields,
    },
    categoryWinners,
    categoryVerdicts,
    keyDifferences,
    commonFeatures,
    upgradeStory,
    useCasePicks,
    priceVerdict,
    tradeoffs,
    confidence: {
      score: round(confidence, 2),
      level: confidenceLevel,
      comparable_fields: comparedFields,
      price_coverage: round(priceCoverage, 2),
      explanation:
        confidenceLevel === "high"
          ? `Based on ${comparedFields} comparable fields and selected-variant price data.`
          : confidenceLevel === "medium"
            ? `Useful comparison, but some category details or prices are incomplete.`
            : `Treat the verdict as directional because important comparison data is missing.`,
    },
    warnings,
  };
};

module.exports = {
  SCORE_VERSION,
  CATEGORY_META,
  buildDecisionComparison,
};
