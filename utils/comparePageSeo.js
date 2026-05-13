const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

const normalizeText = (value) =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const toFiniteNumber = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
};

const toSlug = (value = "") =>
  String(value || "")
    .toLowerCase()
    .trim()
    .replace(/-price-in-india$/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");

const joinNamesWithoutCommas = (names = []) => {
  const clean = (Array.isArray(names) ? names : [])
    .map((name) => normalizeText(name))
    .filter(Boolean);
  if (clean.length === 0) return "";
  if (clean.length === 1) return clean[0];
  return clean.join(" and ");
};

const resolveSmartphoneSegmentLabel = (price) => {
  const amount = toFiniteNumber(price);
  if (amount == null) return "";
  if (amount <= 10000) return "Entry";
  if (amount <= 20000) return "Budget";
  if (amount <= 30000) return "Lower Mid Range";
  if (amount <= 45000) return "Mid Range";
  if (amount <= 65000) return "Upper Mid Range";
  if (amount <= 90000) return "Premium";
  if (amount <= 130000) return "Flagship";
  return "Ultra Flagship";
};

const buildComparePageSlug = (names = []) => {
  const parts = (Array.isArray(names) ? names : [])
    .map((name) => toSlug(name))
    .filter(Boolean);
  if (parts.length < 2) return "";
  return `${parts.join("-and-")}-comparison`;
};

const buildComparePageTitle = ({
  names = [],
  segmentLabel = "",
  smartphoneTypeLabel = "",
} = {}) => {
  const joinedNames = joinNamesWithoutCommas(names);
  if (!joinedNames) return "Compare Smartphones Price Specifications and Features in India";

  const segment = normalizeText(segmentLabel);
  if (segment) {
    return `Compare ${joinedNames} in the ${segment} Segment Price Specifications and Features in India`;
  }

  const smartphoneType = normalizeText(smartphoneTypeLabel);
  if (smartphoneType) {
    return `Compare ${joinedNames} ${smartphoneType} Smartphones Price Specifications and Features in India`;
  }

  return `Compare ${joinedNames} Price Specifications and Features in India`;
};

const formatUpdatedDate = (value) => {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "long",
    year: "numeric",
  }).format(date);
};

const buildComparePageDescription = ({
  names = [],
  segmentLabel = "",
  smartphoneTypeLabel = "",
  updatedAt = null,
} = {}) => {
  const joinedNames = joinNamesWithoutCommas(names);
  if (!joinedNames) {
    return "Compare smartphones with latest price specifications camera battery performance and features in India.";
  }

  const segment = normalizeText(segmentLabel);
  if (segment) {
    return `Compare ${joinedNames} in the ${segment} Segment with latest price specifications camera battery performance and features in India Updated ${formatUpdatedDate(
      updatedAt,
    )}`;
  }

  const smartphoneType = normalizeText(smartphoneTypeLabel);
  if (smartphoneType) {
    return `Compare ${joinedNames} ${smartphoneType} smartphones with latest price specifications camera battery performance and features in India Updated ${formatUpdatedDate(
      updatedAt,
    )}`;
  }

  return `Compare ${joinedNames} with latest price specifications camera battery performance and features in India Updated ${formatUpdatedDate(
    updatedAt,
  )}`;
};

module.exports = {
  clamp,
  normalizeText,
  toFiniteNumber,
  toSlug,
  joinNamesWithoutCommas,
  resolveSmartphoneSegmentLabel,
  buildComparePageSlug,
  buildComparePageTitle,
  buildComparePageDescription,
  formatUpdatedDate,
};
