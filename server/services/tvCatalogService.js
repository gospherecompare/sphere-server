"use strict";

const TV_SECTIONS = [
  "key_specs_json",
  "basic_info_json",
  "display_json",
  "video_engine_json",
  "audio_json",
  "smart_tv_json",
  "gaming_json",
  "ports_json",
  "connectivity_json",
  "power_json",
  "physical_json",
  "product_details_json",
  "in_the_box_json",
  "warranty_json",
  "storage_json",
];

const asObject = (value) => {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed
      : {};
  } catch {
    return {};
  }
};

const hasOwn = (object, key) =>
  Object.prototype.hasOwnProperty.call(object || {}, key);

const normalizeText = (value) =>
  String(value ?? "")
    .trim()
    .replace(/\s+/g, " ");

const toNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const cleaned = String(value).replace(/[^0-9.-]/g, "");
  if (!cleaned || cleaned === "-" || cleaned === ".") return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseFirstNumeric = (value) => {
  const match = String(value ?? "").match(/(\d+(?:\.\d+)?)/);
  return match ? Number(match[1]) : null;
};

const normalizeDomain = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/^www\./, "");

const validateTvImageUrl = (value, domains = []) => {
  let parsed;
  try {
    parsed = new URL(String(value || "").trim());
  } catch {
    throw new Error("Image URL must be a valid HTTPS URL");
  }
  if (parsed.protocol !== "https:") throw new Error("Image URL must use HTTPS");
  const host = normalizeDomain(parsed.hostname);
  const approved = (Array.isArray(domains) ? domains : [])
    .map(normalizeDomain)
    .filter(Boolean);
  if (!approved.length) {
    throw new Error(
      "No approved official image domains configured for this brand",
    );
  }
  if (
    !approved.some((domain) => host === domain || host.endsWith(`.${domain}`))
  ) {
    throw new Error("Image URL must use an approved official brand domain");
  }
  return parsed.href;
};

const asImageUrl = (value) => {
  if (typeof value === "string") return value.trim();
  if (value && typeof value === "object") {
    return String(value.url || value.image_url || value.src || "").trim();
  }
  return "";
};

const normalizeImages = (images, domains = []) => {
  const source = Array.isArray(images) ? images : [];
  const out = [];
  const seen = new Set();
  for (const item of source) {
    const raw = asImageUrl(item);
    if (!raw) continue;
    const normalized = validateTvImageUrl(raw, domains);
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
};

const normalizeStoreRows = (rows) => {
  const byStore = new Map();
  for (const input of Array.isArray(rows) ? rows : []) {
    const store = asObject(input);
    const storeName = normalizeText(
      store.store_name ||
        store.store ||
        store.storeName ||
        store.display_store_name,
    );
    const url = normalizeText(
      store.url ||
        store.store_url ||
        store.link ||
        store.affiliate_url ||
        store.affiliateUrl,
    );
    if (!storeName || !url) continue;

    const candidate = {
      store_name: storeName,
      price: toNumberOrNull(
        store.price ?? store.current_price ?? store.sale_price,
      ),
      url,
      offer_text:
        normalizeText(store.offer_text || store.offerText || store.offer) ||
        null,
      delivery_info:
        normalizeText(
          store.delivery_info || store.deliveryInfo || store.delivery,
        ) || null,
    };

    const key = storeName.toLowerCase();
    const previous = byStore.get(key);
    if (!previous) {
      byStore.set(key, candidate);
      continue;
    }

    const candidatePrice = candidate.price;
    const previousPrice = previous.price;
    const useCandidate =
      (candidatePrice !== null && previousPrice === null) ||
      (candidatePrice !== null &&
        previousPrice !== null &&
        candidatePrice < previousPrice);

    if (useCandidate) {
      byStore.set(key, { ...previous, ...candidate });
    }
  }

  return [...byStore.values()].sort((a, b) => {
    if (a.price !== null && b.price !== null && a.price !== b.price)
      return a.price - b.price;
    if (a.price !== null && b.price === null) return -1;
    if (a.price === null && b.price !== null) return 1;
    return a.store_name.localeCompare(b.store_name);
  });
};

const normalizeVariant = (input, index, imageDomains) => {
  const variant = asObject(input);
  const inferredSize = normalizeText(
    variant.screen_size ||
      variant.size ||
      variant.display_size ||
      variant.variant_key,
  );
  const variantKey =
    normalizeText(variant.variant_key || variant.variant || inferredSize) ||
    `tv_variant_${index + 1}`;
  const screenSize = inferredSize || variantKey;

  const directAttributes = { ...variant.attributes };
  const excluded = new Set([
    "variant_key",
    "variant",
    "screen_size",
    "size",
    "display_size",
    "screen_size_value",
    "base_price",
    "price",
    "amount",
    "store_prices",
    "stores",
    "storeRows",
    "images",
    "images_json",
    "variant_images",
    "variant_id",
  ]);

  for (const [key, value] of Object.entries(variant)) {
    if (excluded.has(key)) continue;
    if (key === "attributes") continue;
    directAttributes[key] = value;
  }

  if (!hasOwn(directAttributes, "screen_size") && screenSize)
    directAttributes.screen_size = screenSize;
  if (!hasOwn(directAttributes, "screen_size_value")) {
    const numericSize =
      toNumberOrNull(variant.screen_size_value) ??
      parseFirstNumeric(screenSize);
    if (numericSize !== null) directAttributes.screen_size_value = numericSize;
  }

  const rawImages =
    variant.images ?? variant.images_json ?? variant.variant_images ?? [];
  const stores =
    variant.store_prices ?? variant.stores ?? variant.storeRows ?? [];

  return {
    variant_key: variantKey,
    screen_size: screenSize || null,
    screen_size_value:
      toNumberOrNull(variant.screen_size_value) ??
      parseFirstNumeric(screenSize),
    base_price: toNumberOrNull(
      variant.base_price ?? variant.price ?? variant.amount,
    ),
    attributes: directAttributes,
    images: normalizeImages(rawImages, imageDomains),
    store_prices: normalizeStoreRows(stores),
  };
};

const normalizeVariants = (variants, imageDomains = []) => {
  const groups = new Map();
  for (const [index, input] of (Array.isArray(variants)
    ? variants
    : []
  ).entries()) {
    const normalized = normalizeVariant(input, index, imageDomains);
    const key = normalized.variant_key.toLowerCase();
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, normalized);
      continue;
    }

    const basePrice =
      existing.base_price === null
        ? normalized.base_price
        : normalized.base_price === null
          ? existing.base_price
          : Math.min(existing.base_price, normalized.base_price);

    const images = [
      ...new Set([...(existing.images || []), ...(normalized.images || [])]),
    ];
    const mergedStores = normalizeStoreRows([
      ...(existing.store_prices || []),
      ...(normalized.store_prices || []),
    ]);

    groups.set(key, {
      ...existing,
      base_price: basePrice,
      attributes: { ...existing.attributes, ...normalized.attributes },
      images,
      store_prices: mergedStores,
    });
  }
  return [...groups.values()];
};

const legacyToSections = (input, merged) => {
  const legacy = asObject(merged.home_appliance);
  if (!Object.keys(legacy).length) return merged;

  const normalized = { ...merged };

  if (!hasOwn(normalized, "category") && legacy.appliance_type)
    normalized.category = legacy.appliance_type;
  if (!hasOwn(normalized, "model") && legacy.model_number)
    normalized.model = legacy.model_number;

  if (!hasOwn(normalized, "basic_info_json")) {
    normalized.basic_info_json = {
      model_number: legacy.model_number || null,
      launch_year: legacy.release_year || null,
    };
  }
  if (
    !hasOwn(normalized, "product_details_json") &&
    (legacy.country_of_origin || legacy.release_year)
  ) {
    normalized.product_details_json = {
      country_of_origin: legacy.country_of_origin || null,
      launch_year: legacy.release_year || null,
    };
  }
  if (
    !hasOwn(normalized, "display_json") &&
    legacy.specifications &&
    typeof legacy.specifications === "object"
  ) {
    normalized.display_json = legacy.specifications;
  }
  if (
    !hasOwn(normalized, "video_engine_json") &&
    legacy.performance &&
    typeof legacy.performance === "object"
  ) {
    normalized.video_engine_json = legacy.performance;
  }
  if (
    !hasOwn(normalized, "physical_json") &&
    legacy.physical_details &&
    typeof legacy.physical_details === "object"
  ) {
    normalized.physical_json = legacy.physical_details;
  }
  if (
    !hasOwn(normalized, "warranty_json") &&
    legacy.warranty &&
    typeof legacy.warranty === "object"
  ) {
    normalized.warranty_json = legacy.warranty;
  }
  if (!hasOwn(normalized, "smart_tv_json") && Array.isArray(legacy.features)) {
    normalized.smart_tv_json = { smart_features: legacy.features };
  }

  return normalized;
};

const normalizeTvRequestInput = (input = {}) => {
  const body = asObject(input);
  const nestedTv = asObject(body.tv);
  const merged = legacyToSections(
    body,
    Object.keys(nestedTv).length ? { ...body, ...nestedTv } : body,
  );
  const product = asObject(merged.product);
  const basic = asObject(merged.basic_info_json);

  const normalized = { ...merged };
  if (!hasOwn(normalized, "product_name") && product.name)
    normalized.product_name = product.name;
  if (!hasOwn(normalized, "brand_id") && product.brand_id)
    normalized.brand_id = product.brand_id;
  if (!hasOwn(normalized, "brand_name") && product.brand_name)
    normalized.brand_name = product.brand_name;
  if (!hasOwn(normalized, "publish") && hasOwn(normalized, "published"))
    normalized.publish = normalized.published;
  if (!hasOwn(normalized, "images_json") && Array.isArray(normalized.images))
    normalized.images_json = normalized.images;
  if (
    !hasOwn(normalized, "variants_json") &&
    Array.isArray(normalized.variants)
  )
    normalized.variants_json = normalized.variants;

  if (!hasOwn(normalized, "model") && (basic.model_number || basic.model)) {
    normalized.model = basic.model_number || basic.model;
  }

  return normalized;
};

const toCanonicalTvPayload = (input = {}, { imageDomains = [] } = {}) => {
  const normalized = normalizeTvRequestInput(input);
  const product = asObject(normalized.product);
  const basic = asObject(normalized.basic_info_json);

  const rawBrandName =
    normalized.brand_name ||
    normalized.brand ||
    product.brand_name ||
    product.brand ||
    basic.brand_name ||
    basic.brand ||
    null;

  const productName = normalizeText(
    normalized.product_name ||
      normalized.name ||
      product.name ||
      basic.title ||
      normalized.model ||
      "",
  );

  const model = normalizeText(
    normalized.model || basic.model_number || basic.model || "",
  );

  const manufacturerModel =
    normalizeText(
      normalized.manufacturer_model ||
        model ||
        basic.model_number ||
        basic.model ||
        "",
    ) || null;

  const imagesInput = normalized.images_json ?? normalized.images ?? [];

  const variantsInput = normalized.variants_json ?? normalized.variants ?? [];

  const sections = Object.fromEntries(
    TV_SECTIONS.map((key) => [key, asObject(normalized[key])]),
  );

  return {
    product_name: productName,
    brand_id: normalized.brand_id ?? product.brand_id ?? null,
    brand_name: rawBrandName ? normalizeText(rawBrandName) : null,
    model,
    manufacturer_model: manufacturerModel,
    category: normalizeText(normalized.category || "") || null,
    launch_date: normalized.launch_date ?? null,
    publish: Boolean(normalized.publish),
    images_json: normalizeImages(imagesInput, imageDomains),
    variants: normalizeVariants(variantsInput, imageDomains),
    sections,
  };
};

const buildVariantsJson = (variants) =>
  variants.map((variant) => ({
    ...variant,
    variant_key: variant.variant_key,
    screen_size: variant.screen_size,
    screen_size_value: variant.screen_size_value,
    base_price: variant.base_price,
    store_prices: variant.store_prices,
    images: variant.images,
    attributes: variant.attributes,
  }));

async function createTvCatalogRecord(
  client,
  input,
  { resolveBrandId, parseDate, imageDomains = [] } = {},
) {
  const payload = toCanonicalTvPayload(input, { imageDomains });
  if (!payload.product_name) throw new Error("product_name is required");
  if (!payload.model) throw new Error("model is required");
  const brandId = await resolveBrandId(
    client,
    payload.brand_id,
    payload.brand_name,
  );
  if (!brandId)
    throw new Error("brand is required and must reference an existing brand");

  const productResult = await client.query(
    "INSERT INTO products (name, brand_id, product_type) VALUES ($1,$2,'tv') RETURNING id",
    [payload.product_name, brandId],
  );
  const productId = productResult.rows[0].id;
  const json = (value) => JSON.stringify(value ?? null);
  const variantsJson = buildVariantsJson(payload.variants);

  await client.query(
    `INSERT INTO tvs (product_id, category, model, manufacturer_model, launch_date,
      key_specs_json, basic_info_json, display_json, video_engine_json, audio_json,
      smart_tv_json, gaming_json, ports_json, connectivity_json, power_json, physical_json,
      product_details_json, in_the_box_json, warranty_json, storage_json, images_json, variants_json)
     VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,
      $12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,
      $20::jsonb,$21::jsonb,$22::jsonb)`,
    [
      productId,
      payload.category,
      payload.model,
      payload.manufacturer_model,
      parseDate ? parseDate(payload.launch_date) : payload.launch_date,
      ...TV_SECTIONS.map((key) => json(payload.sections[key])),
      json(payload.images_json),
      json(variantsJson),
    ],
  );

  for (let index = 0; index < payload.images_json.length; index += 1) {
    await client.query(
      "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
      [productId, payload.images_json[index], index + 1],
    );
  }

  for (const variant of payload.variants) {
    const variantResult = await client.query(
      `INSERT INTO product_variants (product_id, variant_key, attributes, base_price)
       VALUES ($1,$2,$3::jsonb,$4) RETURNING id`,
      [
        productId,
        variant.variant_key,
        json({
          ...variant.attributes,
          screen_size: variant.screen_size,
          screen_size_value: variant.screen_size_value,
        }),
        variant.base_price,
      ],
    );
    const variantId = variantResult.rows[0].id;

    for (const store of variant.store_prices) {
      await client.query(
        `INSERT INTO variant_store_prices
         (variant_id, store_name, price, url, offer_text, delivery_info)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (variant_id, store_name)
         DO UPDATE SET price=EXCLUDED.price, url=EXCLUDED.url,
           offer_text=EXCLUDED.offer_text, delivery_info=EXCLUDED.delivery_info`,
        [
          variantId,
          store.store_name,
          store.price,
          store.url,
          store.offer_text,
          store.delivery_info,
        ],
      );
    }

    for (let index = 0; index < variant.images.length; index += 1) {
      await client.query(
        `INSERT INTO product_variant_images (variant_id, image_url, position)
         VALUES ($1,$2,$3)
         ON CONFLICT (variant_id, image_url)
         DO UPDATE SET position=EXCLUDED.position`,
        [variantId, variant.images[index], index + 1],
      );
    }
  }

  await client.query(
    `INSERT INTO product_publish (product_id, is_published)
     VALUES ($1,$2)`,
    [productId, payload.publish],
  );

  return { productId, payload };
}

async function persistTvUpdate(
  client,
  productId,
  input,
  { resolveBrandId, parseDate, publish, imageDomains = [] } = {},
) {
  const payload = toCanonicalTvPayload(input, { imageDomains });
  if (!payload.product_name) throw new Error("product_name is required");
  if (!payload.model) throw new Error("model is required");

  const brandId = await resolveBrandId(
    client,
    payload.brand_id,
    payload.brand_name,
  );
  if (!brandId)
    throw new Error("brand is required and must reference an existing brand");

  const json = (value) => JSON.stringify(value ?? null);
  const variantsJson = buildVariantsJson(payload.variants);

  await client.query(
    "UPDATE products SET name = $1, brand_id = $2 WHERE id = $3",
    [payload.product_name, brandId, productId],
  );

  await client.query(
    `UPDATE tvs SET category=$1, model=$2, manufacturer_model=$3, launch_date=$4,
      key_specs_json=$5::jsonb, basic_info_json=$6::jsonb, display_json=$7::jsonb,
      video_engine_json=$8::jsonb, audio_json=$9::jsonb, smart_tv_json=$10::jsonb,
      gaming_json=$11::jsonb, ports_json=$12::jsonb, connectivity_json=$13::jsonb,
      power_json=$14::jsonb, physical_json=$15::jsonb, product_details_json=$16::jsonb,
      in_the_box_json=$17::jsonb, warranty_json=$18::jsonb, storage_json=$19::jsonb,
      images_json=$20::jsonb, variants_json=$21::jsonb WHERE product_id=$22`,
    [
      payload.category,
      payload.model,
      payload.manufacturer_model,
      parseDate ? parseDate(payload.launch_date) : payload.launch_date,
      ...TV_SECTIONS.map((key) => json(payload.sections[key])),
      json(payload.images_json),
      json(variantsJson),
      productId,
    ],
  );

  await client.query("DELETE FROM product_images WHERE product_id = $1", [
    productId,
  ]);

  const oldVariants = await client.query(
    "SELECT id FROM product_variants WHERE product_id = $1",
    [productId],
  );
  for (const variant of oldVariants.rows) {
    await client.query(
      "DELETE FROM product_variant_images WHERE variant_id = $1",
      [variant.id],
    );
    await client.query(
      "DELETE FROM variant_store_prices WHERE variant_id = $1",
      [variant.id],
    );
  }
  await client.query("DELETE FROM product_variants WHERE product_id = $1", [
    productId,
  ]);

  for (let index = 0; index < payload.images_json.length; index += 1) {
    await client.query(
      "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
      [productId, payload.images_json[index], index + 1],
    );
  }

  for (const variant of payload.variants) {
    const variantResult = await client.query(
      `INSERT INTO product_variants (product_id, variant_key, attributes, base_price)
       VALUES ($1,$2,$3::jsonb,$4) RETURNING id`,
      [
        productId,
        variant.variant_key,
        json({
          ...variant.attributes,
          screen_size: variant.screen_size,
          screen_size_value: variant.screen_size_value,
        }),
        variant.base_price,
      ],
    );
    const variantId = variantResult.rows[0].id;

    for (const store of variant.store_prices) {
      await client.query(
        `INSERT INTO variant_store_prices
         (variant_id, store_name, price, url, offer_text, delivery_info)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (variant_id, store_name)
         DO UPDATE SET price=EXCLUDED.price, url=EXCLUDED.url,
           offer_text=EXCLUDED.offer_text, delivery_info=EXCLUDED.delivery_info`,
        [
          variantId,
          store.store_name,
          store.price,
          store.url,
          store.offer_text,
          store.delivery_info,
        ],
      );
    }

    for (let index = 0; index < variant.images.length; index += 1) {
      await client.query(
        `INSERT INTO product_variant_images (variant_id, image_url, position)
         VALUES ($1,$2,$3)
         ON CONFLICT (variant_id, image_url)
         DO UPDATE SET position=EXCLUDED.position`,
        [variantId, variant.images[index], index + 1],
      );
    }
  }

  if (publish !== undefined) {
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,$2)
       ON CONFLICT (product_id)
       DO UPDATE SET is_published=EXCLUDED.is_published`,
      [productId, publish],
    );
  }

  return { productId, payload };
}

module.exports = {
  TV_SECTIONS,
  validateTvImageUrl,
  normalizeTvRequestInput,
  toCanonicalTvPayload,
  createTvCatalogRecord,
  persistTvUpdate,
};
