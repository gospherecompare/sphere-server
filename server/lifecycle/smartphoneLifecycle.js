/**
 * PHASE 1: CANONICAL SMARTPHONE LIFECYCLE RESOLVERS
 *
 * This module is the AUTHORITATIVE source for all lifecycle state calculations.
 * It is extracted from index.js to enable:
 * 1. True unit testing in isolation (no server startup required)
 * 2. Reusability across different contexts
 * 3. Clear separation of concerns
 * 4. Deterministic, testable behavior
 *
 * CRITICAL: This module has ZERO dependencies on Express, database, or server state.
 * All inputs are passed as parameters. No side effects.
 */

/**
 * Normalize date input to YYYY-MM-DD format
 * Handles multiple input formats
 */
const normalizeDateOnlyInput = (value) => {
  if (!value) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  }
  if (value instanceof Date) {
    return value.toISOString().split("T")[0];
  }
  return null;
};

/**
 * Normalize launch status to valid values
 */
const normalizeLaunchStatusOverride = (value) => {
  if (!value) return null;
  const normalized = String(value).trim().toLowerCase();
  if (["rumored", "announced", "upcoming", "released"].includes(normalized)) {
    return normalized;
  }
  return null;
};

/**
 * Check if a date is in the future relative to today
 */
const hasFutureDate = (dateValue, todayIndia) => {
  const normalized = normalizeDateOnlyInput(dateValue);
  const today = normalizeDateOnlyInput(todayIndia);
  return Boolean(normalized && today && normalized > today);
};

/**
 * Check if a URL is a specific product URL (not a generic homepage)
 */
const isProductUrl = (url) => {
  if (!url || typeof url !== "string") return false;
  const trimmed = String(url).trim();
  if (!trimmed) return false;

  try {
    const parsed = new URL(trimmed);
    const pathname = parsed.pathname || "/";
    const search = parsed.search || "";

    // Root homepages like https://www.flipkart.com/ are not product pages.
    if (pathname === "/" && !search) return false;

    const directProductPatterns = [
      /\/p\//i,
      /\/product\//i,
      /\/dp\//i,
      /\/gp\/product\//i,
      /\/ss\//i,
      /\/m\//i,
      /\/item\//i,
      /\/b\//i,
      /\/i\//i,
      /\/(?:p|product|dp|gp\/product|ss|m|item|b|i)\b/i,
    ];

    if (directProductPatterns.some((pattern) => pattern.test(pathname))) {
      return true;
    }

    if (/[?&](asin|id|product_id|item_id|pid|sku|offerid)=/i.test(search)) {
      return true;
    }

    // Short retailer product links like amzn.to/abc123 are valid product URLs
    // as long as they are not the base domain root.
    const nonRootPath = pathname !== "/" && pathname !== "";
    if (nonRootPath) {
      const genericCatalogPaths =
        /^(?:\/)?(?:cart|wishlist|checkout|account|login|signup|offers|search|collections|categories|store|help|support|all|home)(?:\/|$)/i;
      return !genericCatalogPaths.test(pathname);
    }

    return false;
  } catch {
    // Fall back to the legacy pattern for plain strings without a valid URL.
    return Boolean(
      trimmed.match(/\/(p|product|dp)\//) ||
      trimmed.match(/[?&](asin|id|product_id|item_id)=/) ||
      (trimmed.match(/\/s\?k=/) && !trimmed.endsWith("/")) ||
      (trimmed.match(/^https?:\/\/[^/]+\/[^/]+$/) && !trimmed.endsWith("/")),
    );
  }
};

/**
 * Check if a store entry has a valid purchase signal
 * This is used to determine if a store is "live" (can actually purchase)
 */
const hasValidStorePriceSignal = (storeEntry) => {
  if (!storeEntry || typeof storeEntry !== "object") return false;

  const price =
    storeEntry.price ??
    storeEntry.current_price ??
    storeEntry.sale_price ??
    storeEntry.offer_price ??
    storeEntry.base_price;
  const url = String(
    storeEntry.url ?? storeEntry.link ?? storeEntry.affiliate_link ?? "",
  ).trim();

  // Must have BOTH price AND a specific product URL
  // Generic homepage URLs don't count
  const hasPrice = Boolean(Number(price) > 0);
  const hasProductUrl = isProductUrl(url);

  return hasPrice && hasProductUrl;
};

/**
 * Check if a store has availability info (may or may not be live yet)
 */
const hasStoreEntry = (storeEntry) => {
  if (!storeEntry || typeof storeEntry !== "object") return false;

  const storeName = String(
    storeEntry.store_name ?? storeEntry.storeName ?? "",
  ).trim();
  const logo = String(storeEntry.logo ?? storeEntry.store_logo ?? "").trim();

  return Boolean(storeName || logo || hasValidStorePriceSignal(storeEntry));
};

const extractSaleStartDate = (device) => {
  const direct = normalizeDateOnlyInput(
    device?.sale_start_date ??
      device?.saleStartDate ??
      device?.sale_date ??
      device?.saleDate ??
      null,
  );
  if (direct) return direct;

  const stores = [
    ...(Array.isArray(device?.store_prices) ? device.store_prices : []),
    ...(Array.isArray(device?.storePrices) ? device.storePrices : []),
    ...(Array.isArray(device?.variants)
      ? device.variants.flatMap((variant) => [
          ...(Array.isArray(variant?.store_prices) ? variant.store_prices : []),
          ...(Array.isArray(variant?.storePrices) ? variant.storePrices : []),
        ])
      : []),
  ];

  for (const store of stores) {
    const date = normalizeDateOnlyInput(
      store?.sale_start_date ??
        store?.saleStartDate ??
        store?.sale_date ??
        store?.saleDate ??
        null,
    );
    if (date) return date;
  }

  return null;
};

/**
 * RESOLVE LAUNCH STAGE — UNAMBIGUOUS PRECEDENCE
 *
 * Launch status is editorial. launch_date is metadata only and never changes
 * the stage automatically.
 */
const resolveCanonicalLaunchStage = (device, todayIndia = null) => {
  if (!device || typeof device !== "object") return "upcoming";

  const override = normalizeLaunchStatusOverride(
    device.launch_status_override || device.launchStatusOverride,
  );
  return override || "upcoming";
};

/**
 * RESOLVE SALE STAGE — INDEPENDENT FROM LAUNCH
 *
 * Sale state is driven by sale_start_date only.
 * Launch date affects whether it's "preorder" vs "on_sale",
 * but sale date is the primary authority for sale state.
 */
const resolveCanonicalSaleStage = (device, todayIndia = null) => {
  if (!device || typeof device !== "object") return "sale_tbd";

  const saleStartDate = extractSaleStartDate(device);

  const today = normalizeDateOnlyInput(todayIndia);

  // No sale date info
  if (!saleStartDate) {
    const storeRows = [
      ...(Array.isArray(device.store_prices) ? device.store_prices : []),
      ...(Array.isArray(device.storePrices) ? device.storePrices : []),
      ...(Array.isArray(device.variants)
        ? device.variants.flatMap((variant) => [
            ...(Array.isArray(variant?.store_prices)
              ? variant.store_prices
              : []),
            ...(Array.isArray(variant?.storePrices) ? variant.storePrices : []),
          ])
        : []),
    ];
    if (storeRows.some(hasValidStorePriceSignal)) {
      if (resolveCanonicalLaunchStage(device, today) === "released") {
        return "on_sale";
      }
    }
    return "sale_tbd";
  }

  // Sale date is in future
  if (today && saleStartDate > today) {
    return "sale_scheduled";
  }

  // Sale date has passed (or is today)
  // Distinguish preorder (shippable when product launches) vs on_sale (shippable now)
  const launchDate = normalizeDateOnlyInput(
    device.launch_date ??
      device.launchDate ??
      device.release_date ??
      device.releaseDate ??
      null,
  );

  const launchStage = resolveCanonicalLaunchStage(device, todayIndia);
  if (launchStage !== "released") {
    // Product not yet launched, but sale is open = preorder
    return "preorder";
  }

  // Product launched (or no launch date) and sale open = can ship
  return "on_sale";
};

/**
 * RESOLVE STORE STAGE — INDEPENDENT FROM LAUNCH AND SALE
 *
 * Store stage depends ONLY on store availability, not on launch or sale dates.
 * (Sale date affects whether store=prebooking, but that's determined by sale logic,
 * not by inferring it here)
 */
const resolveCanonicalStoreStage = (device, todayIndia = null) => {
  if (!device || typeof device !== "object") return "none";

  // Get device-level sale date for determining prebooking status
  const deviceSaleDate = normalizeDateOnlyInput(
    device.sale_start_date ??
      device.saleStartDate ??
      device.sale_date ??
      device.saleDate ??
      null,
  );

  // Collect all store entries
  let storeRows = [
    ...(Array.isArray(device.store_prices) ? device.store_prices : []),
    ...(Array.isArray(device.storePrices) ? device.storePrices : []),
  ];

  // Extract store entries from variants
  const variants = Array.isArray(device.variants) ? device.variants : [];
  for (const variant of variants) {
    if (variant && Array.isArray(variant.store_prices)) {
      storeRows.push(...variant.store_prices);
    }
    if (variant && Array.isArray(variant.storePrices)) {
      storeRows.push(...variant.storePrices);
    }
  }

  // Filter to only include stores with actual purchasing signal
  // A store must have a valid price signal or an explicit positive price.
  storeRows = storeRows.filter((store) => {
    if (!store || typeof store !== "object") return false;

    const hasSignal = hasValidStorePriceSignal(store);
    const hasExplicitPrice = Boolean(
      Number(store.price ?? store.current_price ?? store.sale_price ?? 0) > 0,
    );

    return hasSignal || hasExplicitPrice;
  });

  // No stores listed
  if (storeRows.length === 0) {
    return "none";
  }

  // Check if any store is live (has price + valid URL + not future sale date)
  const today = normalizeDateOnlyInput(todayIndia);
  const hasLiveStore = storeRows.some((store) => {
    if (!hasValidStorePriceSignal(store)) return false;

    // Check if store or device has future availability date
    const storeSaleDate = normalizeDateOnlyInput(
      store.sale_start_date ??
        store.saleStartDate ??
        store.sale_date ??
        store.saleDate ??
        null,
    );
    const effectiveSaleDate = storeSaleDate || deviceSaleDate;

    if (effectiveSaleDate && today && effectiveSaleDate > today) {
      // Store sale date is future = not yet live
      return false;
    }

    return true;
  });

  if (hasLiveStore) {
    return "live";
  }

  // Stores exist but not live yet
  // Check if any have future availability dates (prebooking period)
  const hasPrebookingStore = storeRows.some((store) => {
    const storeSaleDate = normalizeDateOnlyInput(
      store.sale_start_date ??
        store.saleStartDate ??
        store.sale_date ??
        store.saleDate ??
        null,
    );
    const effectiveSaleDate = storeSaleDate || deviceSaleDate;

    return Boolean(effectiveSaleDate && today && effectiveSaleDate > today);
  });

  if (hasPrebookingStore) {
    return "prebooking";
  }

  // Stores exist, not live, no specific dates
  return "listed";
};

/**
 * RESOLVE RENDER TYPE — BASED ON LAUNCH ONLY
 *
 * Determines how to display the product in UI.
 * Depends ONLY on launch stage, not on sale or store.
 */
const resolveCanonicalRenderType = (launchStage) => {
  const isUpcomingLaunch = ["rumored", "announced", "upcoming"].includes(
    launchStage,
  );

  return {
    type: isUpcomingLaunch ? "upcoming" : "released",
    display_status: isUpcomingLaunch ? "Upcoming" : "Released",
  };
};

/**
 * RESOLVE PERMISSIONS — BASED ON LAUNCH AND SPEC ELIGIBILITY
 *
 * Controls what features are available in the UI.
 * compare/competitors: Launch must be "released"
 * spec_score: Depends on launch AND spec completeness
 */
const resolveCanonicalPermissions = (launchStage, specScoreEligible = true) => {
  // Compare and competitors only available for released products
  const canCompare = launchStage === "released";
  const canShowCompetitors = launchStage === "released";

  // Spec score depends on both launch AND spec eligibility
  // Upcoming products can show score if specs are complete enough
  const canShowSpecScore = launchStage === "released" && specScoreEligible;

  return {
    allow_compare: canCompare,
    allow_competitors: canShowCompetitors,
    allow_spec_score: canShowSpecScore,
  };
};

/**
 * CREATE CANONICAL RESPONSE
 *
 * This is the unified lifecycle response contract.
 * All API endpoints must return this structure (plus additional fields).
 *
 * @param {Object} device - The device document
 * @param {string} todayIndia - ISO date string "YYYY-MM-DD" for today
 * @param {boolean} specScoreEligible - Whether this product has eligible specs for scoring
 * @returns {Object} Canonical lifecycle response
 */
const createCanonicalSmartphoneResponse = (
  device,
  todayIndia = null,
  specScoreEligible = true,
) => {
  if (!device || typeof device !== "object") {
    return {
      launch: {
        stage: "upcoming",
        date: null,
        date_type: null,
        mode: "auto",
      },
      sale: {
        stage: "sale_tbd",
        start_date: null,
      },
      store: {
        stage: "none",
      },
      render: {
        type: "upcoming",
        display_status: "Upcoming",
      },
      allow_compare: false,
      allow_competitors: false,
      allow_spec_score: false,
    };
  }

  // Resolve each dimension independently
  const launchStage = resolveCanonicalLaunchStage(device, todayIndia);
  const saleStage = resolveCanonicalSaleStage(device, todayIndia);
  const storeStage = resolveCanonicalStoreStage(device, todayIndia);
  const renderType = resolveCanonicalRenderType(launchStage);
  const permissions = resolveCanonicalPermissions(
    launchStage,
    specScoreEligible,
  );

  // Normalize dates
  const launchDate = normalizeDateOnlyInput(
    device.launch_date ??
      device.launchDate ??
      device.release_date ??
      device.releaseDate ??
      null,
  );
  const saleStartDate = extractSaleStartDate(device);
  const mode = String(
    device.launch_status_mode || device.launchStatusMode || "auto",
  ).toLowerCase();

  const marketStatus = ["rumored", "announced", "upcoming"].includes(
    launchStage,
  )
    ? "Upcoming"
    : "Released";

  return {
    // Launch: when product is released to market
    launch: {
      stage: launchStage,
      date: launchDate,
      date_type: device.launch_date_type || device.launchDateType || null,
      mode: mode,
    },

    // Sale: when orders can be placed
    sale: {
      stage: saleStage,
      start_date: saleStartDate,
    },

    // Store: where product is listed/available
    store: {
      stage: storeStage,
    },

    // Market status is a presentation/compatibility field derived from launch stage only.
    market_status: marketStatus,
    marketStatus: marketStatus,

    // Render: how UI displays this product
    render: renderType,

    // Permissions: what features are available
    allow_compare: permissions.allow_compare,
    allow_competitors: permissions.allow_competitors,
    allow_spec_score: permissions.allow_spec_score,
  };
};

// Export all functions for use in tests and index.js
module.exports = {
  normalizeDateOnlyInput,
  normalizeLaunchStatusOverride,
  hasFutureDate,
  hasValidStorePriceSignal,
  hasStoreEntry,
  resolveCanonicalLaunchStage,
  resolveCanonicalSaleStage,
  resolveCanonicalStoreStage,
  resolveCanonicalRenderType,
  resolveCanonicalPermissions,
  createCanonicalSmartphoneResponse,
};
