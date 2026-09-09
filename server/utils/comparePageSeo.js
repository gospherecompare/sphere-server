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

const joinNamesWithVs = (names = []) => {
  const clean = (Array.isArray(names) ? names : [])
    .map((name) => normalizeText(name))
    .filter(Boolean);
  if (clean.length === 0) return "";
  if (clean.length === 1) return clean[0];
  return clean.join(" vs ");
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

const resolveAutomaticPriceBandLabel = (price) => {
  const amount = toFiniteNumber(price);
  if (amount == null) return "";
  if (amount <= 20000) return "Under \u20b920,000";
  if (amount <= 30000) return "Under \u20b930,000";
  if (amount <= 50000) return "Under \u20b950,000";
  if (amount <= 70000) return "Under \u20b970,000";
  if (amount <= 100000) return "Under \u20b91,00,000";
  return "";
};

const buildAutomaticComparePageTitle = ({
  names = [],
  segmentLabel = "",
  smartphoneTypeLabel = "",
  price = null,
} = {}) => {
  const titleNames = joinNamesWithVs(names);
  if (!titleNames) {
    return "Smartphone Comparison Price Specifications and Features in India";
  }

  const count = (Array.isArray(names) ? names : []).filter(Boolean).length;
  const segment = normalizeText(segmentLabel);
  const type = normalizeText(smartphoneTypeLabel);
  const typeKey = type.toLowerCase();
  const priceBand = resolveAutomaticPriceBandLabel(price);
  const isFlagshipSegment = ["Premium", "Flagship", "Ultra Flagship"].includes(segment);

  if (count >= 3) {
    if (isFlagshipSegment) return `${titleNames} - Full Flagship Comparison`;
    if (typeKey === "gaming") return `${titleNames} - Gaming Comparison`;
    if (typeKey === "camera flagship" || typeKey === "camera") {
      return `${titleNames} - Camera Comparison`;
    }
    if (priceBand) return `Best Phones ${priceBand} - ${titleNames}`;
    return `${titleNames} - Which Phone is Better`;
  }

  if (typeKey === "gaming") {
    return priceBand
      ? `${titleNames} - Best Gaming Phone ${priceBand}?`
      : `${titleNames} - Gaming Comparison`;
  }

  if (typeKey === "camera flagship" || typeKey === "camera") {
    return `${titleNames} - Best Camera Phone Comparison`;
  }

  if (typeKey === "battery focused") {
    return `${titleNames} - Best Battery Phone`;
  }

  if (typeKey === "fast charging") {
    return `${titleNames} - Battery and Charging Comparison`;
  }

  if (typeKey === "value 5g") {
    return priceBand
      ? `${titleNames} - Best 5G Phone ${priceBand}?`
      : `${titleNames} - 5G Phone Comparison`;
  }

  if (typeKey === "selfie") return `${titleNames} - Selfie Camera Comparison`;
  if (typeKey === "compact") return `${titleNames} - Compact Phone Comparison`;
  if (typeKey === "clean android") return `${titleNames} - Clean Android Comparison`;
  if (typeKey === "slim design") return `${titleNames} - Design Comparison`;
  if (typeKey === "ai") return `${titleNames} - AI Features Comparison`;
  if (typeKey === "rugged") return `${titleNames} - Rugged Phone Comparison`;
  if (typeKey === "flip foldable" || typeKey === "book foldable") {
    return `${titleNames} - Full Foldable Comparison`;
  }
  if (typeKey === "new launch") {
    return `${titleNames} - Latest Price and Specs Comparison`;
  }

  if (isFlagshipSegment) return `${titleNames} - Ultimate Flagship Comparison`;
  if (priceBand) return `Best Phone ${priceBand} - ${titleNames}`;

  return `${titleNames} - Which Phone is Better`;
};

const buildAutomaticComparePageDescription = ({
  names = [],
  segmentLabel = "",
  smartphoneTypeLabel = "",
  price = null,
  updatedAt = null,
} = {}) => {
  const joinedNames = joinNamesWithoutCommas(names);
  if (!joinedNames) {
    return "Compare smartphones with latest price specifications camera battery processor performance and features in India.";
  }

  const segment = normalizeText(segmentLabel);
  const type = normalizeText(smartphoneTypeLabel).toLowerCase();
  const priceBand = resolveAutomaticPriceBandLabel(price);
  const updatedLabel = formatUpdatedDate(updatedAt);

  if (type === "gaming") {
    return `Compare ${joinedNames} for gaming performance processor battery display and latest price in India Updated ${updatedLabel}`;
  }

  if (type === "camera flagship" || type === "camera" || type === "selfie") {
    return `Compare ${joinedNames} for camera quality battery performance and latest price in India Updated ${updatedLabel}`;
  }

  if (type === "battery focused" || type === "fast charging") {
    return `Compare ${joinedNames} for battery backup charging speed performance and latest price in India Updated ${updatedLabel}`;
  }

  if (type === "value 5g") {
    return `Compare ${joinedNames} for 5G connectivity battery performance and latest price in India Updated ${updatedLabel}`;
  }

  if (segment) {
    return `Compare ${joinedNames} in the ${segment} segment with latest price specifications camera battery performance processor and features in India Updated ${updatedLabel}`;
  }

  if (priceBand) {
    return `Compare ${joinedNames} for the best smartphone value ${priceBand} with latest price specifications battery camera and performance in India Updated ${updatedLabel}`;
  }

  return `Compare ${joinedNames} with latest price specifications camera battery processor performance and features in India Updated ${updatedLabel}`;
};

module.exports = {
  clamp,
  normalizeText,
  toFiniteNumber,
  toSlug,
  joinNamesWithoutCommas,
  joinNamesWithVs,
  resolveSmartphoneSegmentLabel,
  buildComparePageSlug,
  buildComparePageTitle,
  buildComparePageDescription,
  resolveAutomaticPriceBandLabel,
  buildAutomaticComparePageTitle,
  buildAutomaticComparePageDescription,
  formatUpdatedDate,
};
