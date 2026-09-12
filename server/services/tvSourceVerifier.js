"use strict";

const OFFICIAL_DOMAINS = {
  samsung: ["samsung.com", "samsung.com/in"],
  lg: ["lg.com", "lg.com/in"],
  sony: ["sony.com", "sony.co.in"],
  xiaomi: ["mi.com", "mi.com/in"],
  oneplus: ["oneplus.com", "oneplus.in"],
  tcl: ["tcl.com", "tcl.com/in"],
  hisense: ["hisense.com", "hisense-india.com"],
  panasonic: ["panasonic.com", "panasonic.com/in"],
  vu: ["vutvs.com"],
};

const SECONDARY_DOMAINS = {
  "91mobiles": ["91mobiles.com"],
  beebom: ["beebom.com"],
  gadgets360: ["gadgets360.com"],
};

const normalizeHost = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/^www\./, "")
    .split("/")[0];
const hostMatches = (host, allowed) =>
  allowed.some((domain) => host === domain || host.endsWith(`.${domain}`));
const sourceTypeForHost = (host) =>
  Object.entries(SECONDARY_DOMAINS).find(([, domains]) =>
    hostMatches(host, domains),
  )?.[0] || null;

const verifyTvSourceEvidence = async ({
  evidence = [],
  identity = {},
  fetchImpl = globalThis.fetch,
  requireSecondary = true,
  minimumSecondarySources = 2,
}) => {
  const errors = [];
  const verified = [];
  const brandKey = String(identity.brandName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  const officialDomains = OFFICIAL_DOMAINS[brandKey];
  const seenTypes = new Set();

  if (!officialDomains)
    errors.push(
      `No official domain registry entry for brand: ${identity.brandName}`,
    );
  if (typeof fetchImpl !== "function")
    errors.push("fetch implementation is unavailable");

  for (const item of Array.isArray(evidence) ? evidence : []) {
    let url;
    try {
      url = new URL(item?.url);
    } catch {
      errors.push("source evidence contains an invalid URL");
      continue;
    }
    if (!["http:", "https:"].includes(url.protocol)) {
      errors.push(`unsupported source protocol: ${url.protocol}`);
      continue;
    }
    const host = normalizeHost(url.hostname);
    const sourceType = String(item?.source_type || "").toLowerCase();
    const expectedOfficial = sourceType === "official";
    const validDomain = expectedOfficial
      ? Boolean(officialDomains && hostMatches(host, officialDomains))
      : Boolean(
          SECONDARY_DOMAINS[sourceType] &&
          hostMatches(host, SECONDARY_DOMAINS[sourceType]),
        );
    if (!validDomain) {
      errors.push(
        `source domain is not allowed for ${sourceType || "unknown"}: ${host}`,
      );
      continue;
    }
    if (seenTypes.has(sourceType)) continue;

    try {
      const response = await fetchImpl(url, {
        redirect: "follow",
        signal: AbortSignal.timeout(8000),
      });
      if (!response.ok) {
        errors.push(`source returned HTTP ${response.status}: ${url.href}`);
        continue;
      }
      const finalHost = normalizeHost(
        new URL(response.url || url.href).hostname,
      );
      if (
        !validDomain ||
        !(
          (expectedOfficial
            ? officialDomains
            : SECONDARY_DOMAINS[sourceType]) || []
        ).some(
          (domain) => finalHost === domain || finalHost.endsWith(`.${domain}`),
        )
      ) {
        errors.push(`source redirected to an untrusted domain: ${finalHost}`);
        continue;
      }
      const text = (await response.text()).toLowerCase();
      const model = String(identity.model || "").toLowerCase();
      const brand = String(identity.brandName || "").toLowerCase();
      if (
        model &&
        !text.includes(model) &&
        !text.includes(model.replace(/[-\s]/g, ""))
      ) {
        errors.push(`source does not contain the requested model: ${url.href}`);
        continue;
      }
      if (brand && !text.includes(brand)) {
        errors.push(`source does not contain the requested brand: ${url.href}`);
        continue;
      }
      verified.push({
        ...item,
        url: url.href,
        domain: finalHost,
        verified: true,
        source_type: expectedOfficial ? "official" : sourceType,
      });
      seenTypes.add(sourceType);
    } catch (error) {
      errors.push(`failed to verify source ${url.href}: ${error.message}`);
    }
  }

  if (!seenTypes.has("official"))
    errors.push("verified official manufacturer source is required");
  if (requireSecondary) {
    const secondaryCount = Object.keys(SECONDARY_DOMAINS).filter((type) =>
      seenTypes.has(type),
    ).length;
    if (secondaryCount < minimumSecondarySources)
      errors.push(
        `at least ${minimumSecondarySources} verified secondary sources are required`,
      );
  }
  return {
    ok: errors.length === 0,
    errors,
    verified,
    source_types: [...seenTypes],
  };
};

module.exports = {
  OFFICIAL_DOMAINS,
  SECONDARY_DOMAINS,
  verifyTvSourceEvidence,
  sourceTypeForHost,
};
