const isPlainObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value);

const normalizeStringArray = (value) => {
  if (value === undefined || value === null || value === "") return [];
  if (Array.isArray(value))
    return value.map((entry) => String(entry)).filter(Boolean);
  if (typeof value === "string") {
    return value
      .split(/[|,;]+/)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }
  return [];
};

const normalizeNullableText = (value) => {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") return value.trim() || null;
  return String(value).trim() || null;
};

const normalizeSection = (value) => {
  if (value === undefined || value === null) return {};
  if (Array.isArray(value)) return {};
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return isPlainObject(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }
  return isPlainObject(value) ? value : {};
};

const normalizeStoreEntry = (store) => {
  if (!isPlainObject(store)) return null;

  const normalized = {
    store_name: normalizeNullableText(
      store.store_name ??
        store.storeName ??
        store.store ??
        store.display_store_name ??
        store.displayStoreName,
    ),
    region: normalizeNullableText(
      store.region ?? store.region_name ?? store.location,
    ),
    price:
      store.price ??
      store.current_price ??
      store.sale_price ??
      store.offer_price ??
      null,
    currency: normalizeNullableText(
      store.currency ?? store.currency_code ?? store.currencyCode,
    ),
    status: normalizeNullableText(
      store.status ?? store.availability ?? store.store_status,
    ),
    url: normalizeNullableText(
      store.url ??
        store.link ??
        store.affiliate_link ??
        store.affiliateUrl ??
        store.href,
    ),
    offer_text: normalizeNullableText(
      store.offer_text ?? store.offerText ?? store.offer ?? store.offer_label,
    ),
    notes: normalizeNullableText(store.notes ?? store.note ?? store.comment),
  };

  const hasUsefulData =
    normalized.store_name ||
    normalized.region ||
    normalized.price !== null ||
    normalized.url ||
    normalized.offer_text ||
    normalized.notes;

  return hasUsefulData ? normalized : null;
};

const normalizeStores = (value) => {
  if (!value) return [];
  const source = Array.isArray(value)
    ? value
    : Array.isArray(value.stores)
      ? value.stores
      : Array.isArray(value.store_prices)
        ? value.store_prices
        : [];

  return source.map((store) => normalizeStoreEntry(store)).filter(Boolean);
};

const normalizeVariant = (variant) => {
  if (!isPlainObject(variant)) return null;

  const normalized = {
    ...variant,
    ram: normalizeNullableText(
      variant.ram ?? variant.memory ?? variant.ram_size,
    ),
    storage: normalizeNullableText(
      variant.storage ?? variant.storage_size ?? variant.storageSize,
    ),
    base_price:
      variant.base_price ?? variant.price ?? variant.starting_price ?? null,
    attributes: isPlainObject(variant.attributes) ? variant.attributes : {},
    stores: normalizeStores(variant),
  };

  if (normalized.attributes.ram === undefined && normalized.ram)
    normalized.attributes.ram = normalized.ram;
  if (normalized.attributes.storage === undefined && normalized.storage)
    normalized.attributes.storage = normalized.storage;

  const variantKey = normalizeNullableText(
    variant.variant_key ?? variant.variantKey ?? variant.key,
  );
  if (variantKey) normalized.variant_key = variantKey;

  if (!normalized.base_price && normalized.stores.length) {
    const cheapest = normalized.stores
      .map((store) => Number(store.price))
      .filter((price) => Number.isFinite(price))
      .sort((a, b) => a - b)[0];
    if (Number.isFinite(cheapest)) normalized.base_price = cheapest;
  }

  delete normalized.store_prices;
  delete normalized.stores_raw;
  delete normalized.store;
  return normalized;
};

const normalizeVariants = (value) => {
  if (!Array.isArray(value)) {
    const parsed = Array.isArray(value?.variants)
      ? value.variants
      : Array.isArray(value?.variants_json)
        ? value.variants_json
        : [];
    return parsed.map((variant) => normalizeVariant(variant)).filter(Boolean);
  }

  return value.map((variant) => normalizeVariant(variant)).filter(Boolean);
};

const flattenVariantStoreRows = (value = []) => {
  const variants = Array.isArray(value)
    ? value
    : Array.isArray(value?.variants)
      ? value.variants
      : Array.isArray(value?.variants_json)
        ? value.variants_json
        : [];

  return variants.flatMap((variant, index) => {
    const variantObj = isPlainObject(variant) ? variant : {};
    const stores = Array.isArray(variantObj.stores)
      ? variantObj.stores
      : Array.isArray(variantObj.store_prices)
        ? variantObj.store_prices
        : [];

    return stores.map((store) => {
      const normalizedStore = normalizeStoreEntry(store);
      return normalizedStore
        ? {
            ...normalizedStore,
            variant_index: index,
            variant_id: variantObj.id ?? null,
            variant_key: variantObj.variant_key ?? variantObj.variantKey ?? null,
            sale_start_date: normalizeNullableText(
              store?.sale_start_date ??
                store?.saleStartDate ??
                store?.sale_date ??
                store?.saleDate ??
                null,
            ),
          }
        : null;
    });
  }).filter(Boolean);
};

const normalizeSmartphonePayload = (rawBody = {}) => {
  const body = isPlainObject(rawBody) ? { ...rawBody } : {};
  const product = isPlainObject(body.product) ? body.product : {};
  const nestedSmartphone = isPlainObject(body.smartphone)
    ? body.smartphone
    : {};
  const merged = { ...body, ...product, ...nestedSmartphone };

  const variantsInput = Array.isArray(merged.variants)
    ? merged.variants
    : Array.isArray(merged.variants_json)
      ? merged.variants_json
      : [];

  const variants = normalizeVariants(variantsInput);
  const variantStorePrices = Array.isArray(merged.variant_store_prices)
    ? merged.variant_store_prices
    : Array.isArray(merged.store_prices)
      ? merged.store_prices
      : flattenVariantStoreRows(variantsInput);

  const normalized = {
    product_name: normalizeNullableText(
      merged.product_name ??
        merged.name ??
        product.name ??
        product.product_name ??
        merged.model_name,
    ),
    brand_name: normalizeNullableText(
      merged.brand_name ??
        merged.brand ??
        product.brand_name ??
        product.brand ??
        merged.brandName,
    ),
    model: normalizeNullableText(
      merged.model ?? merged.model_name ?? merged.modelName,
    ),
    category: normalizeNullableText(merged.category ?? merged.segment),
    launch_date: normalizeNullableText(merged.launch_date ?? merged.launchDate),
    launch_status_override: normalizeNullableText(
      merged.launch_status_override ??
        merged.launchStatusOverride ??
        merged.launch_status ??
        merged.launchStatus,
    ),
    images: Array.isArray(merged.images)
      ? merged.images
      : Array.isArray(merged.images_json)
        ? merged.images_json
        : [],
    colors: normalizeStringArray(merged.colors ?? merged.colors_json),
    build_design: normalizeSection(
      merged.build_design ?? merged.build_design_json,
    ),
    display: normalizeSection(merged.display ?? merged.display_json),
    performance: normalizeSection(
      merged.performance ?? merged.performance_json,
    ),
    camera: normalizeSection(merged.camera ?? merged.camera_json),
    battery: normalizeSection(merged.battery ?? merged.battery_json),
    connectivity: normalizeSection(
      merged.connectivity ??
        merged.connectivity_json ??
        merged.network_connectivity_json,
    ),
    network: normalizeSection(
      merged.network ?? merged.network_json ?? merged.navigation_json,
    ),
    ports: normalizeSection(
      merged.ports ?? merged.ports_json ?? merged.port_json,
    ),
    audio: normalizeSection(merged.audio ?? merged.audio_json),
    multimedia: normalizeSection(merged.multimedia ?? merged.multimedia_json),
    sensors: normalizeStringArray(merged.sensors ?? merged.sensors_json),
    variants,
    variant_store_prices: variantStorePrices,
    published:
      merged.published ?? merged.publish ?? merged.is_published ?? false,
    expected_price: merged.expected_price ?? merged.expectedPrice ?? null,
  };

  if (!normalized.product_name && normalized.model)
    normalized.product_name = normalized.model;
  if (!normalized.brand_name && product.brand_id)
    normalized.brand_name = product.brand_name || product.brand || null;

  normalized.brand = normalized.brand_name;
  normalized.product = {
    name: normalized.product_name,
    brand_name: normalized.brand_name,
    brand: normalized.brand_name,
    ...product,
  };
  normalized.smartphone = {
    ...normalized,
    brand: normalized.brand_name,
    model: normalized.model,
  };

  return normalized;
};

module.exports = {
  flattenVariantStoreRows,
  normalizeSmartphonePayload,
  normalizeStoreEntry,
  normalizeVariant,
  normalizeVariants,
  normalizeStores,
};
