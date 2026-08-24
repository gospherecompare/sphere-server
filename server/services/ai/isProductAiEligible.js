const INVALID_TEXT_VALUES = new Set([
  "",
  "unknown",
  "n/a",
  "na",
  "not available",
  "not specified",
  "null",
  "undefined",
]);

const hasMeaningfulValue = (value) => {
  if (value === null || value === undefined || value === false) return false;
  if (typeof value === "string") {
    return !INVALID_TEXT_VALUES.has(value.trim().toLowerCase());
  }
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.some(hasMeaningfulValue);
  if (typeof value === "object") {
    return Object.values(value).some(hasMeaningfulValue);
  }
  return Boolean(value);
};

const getLaunchStatus = (product) => {
  const explicit = String(
    product?.launch_status_override || product?.launch_status || "",
  )
    .trim()
    .toLowerCase();

  if (explicit) {
    if (["released", "available", "launched"].includes(explicit)) {
      return "released";
    }
    if (["upcoming", "rumored", "announced", "pre-launch"].includes(explicit)) {
      return "upcoming";
    }
  }

  const launchDate = product?.launch_date ? new Date(product.launch_date) : null;
  if (launchDate && !Number.isNaN(launchDate.getTime())) {
    return launchDate <= new Date() ? "released" : "upcoming";
  }

  return "unknown";
};

const calculateSpecCompleteness = (product) => {
  const weights = [
    ["display", 15],
    ["performance", 20],
    ["camera", 20],
    ["battery", 15],
    ["build_design", 10],
    ["connectivity", 10],
    ["network", 5],
    ["audio", 5],
  ];
  const total = weights.reduce((sum, [, weight]) => sum + weight, 0);
  const score = weights.reduce(
    (sum, [field, weight]) =>
      sum + (hasMeaningfulValue(product?.[field]) ? weight : 0),
    0,
  );

  return Math.round((score / total) * 100);
};

const getProductAiEligibility = (product) => {
  if (product?.product_type !== "smartphone") {
    return { eligible: false, status: "disabled", reason: "Not a smartphone" };
  }
  if (product?.is_published !== true) {
    return { eligible: false, status: "waiting_for_data", reason: "Product is not published" };
  }

  const launchStatus = getLaunchStatus(product);
  if (launchStatus !== "released") {
    return {
      eligible: false,
      status: "waiting_for_data",
      reason: "Device is not released",
      launchStatus,
    };
  }

  const completeness = calculateSpecCompleteness(product);
  if (
    !hasMeaningfulValue(product?.display) ||
    !hasMeaningfulValue(product?.performance) ||
    completeness < 75
  ) {
    return {
      eligible: false,
      status: "waiting_for_data",
      reason: `Specifications are ${completeness}% complete`,
      completeness,
    };
  }

  return { eligible: true, status: "pending", completeness, launchStatus };
};

module.exports = {
  calculateSpecCompleteness,
  getProductAiEligibility,
  hasMeaningfulValue,
};
