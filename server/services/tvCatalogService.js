"use strict";

const TV_SECTIONS = [
  "key_specs_json", "basic_info_json", "display_json", "video_engine_json",
  "audio_json", "smart_tv_json", "gaming_json", "ports_json", "connectivity_json",
  "power_json", "physical_json", "product_details_json", "in_the_box_json", "warranty_json",
];

const asObject = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
const asImageUrl = (value) => typeof value === "string" ? value : value?.url || value?.image_url || null;
const validateTvImageUrl = (value, domains = []) => {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("Image URL must be a valid HTTPS URL");
  }
  if (parsed.protocol !== "https:") throw new Error("Image URL must use HTTPS");
  const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
  if (domains.length && !domains.some((domain) => host === domain || host.endsWith(`.${domain}`))) {
    throw new Error("Image URL must use an approved official brand domain");
  }
  return parsed.href;
};
const normalizeImages = (images, domains = []) => (Array.isArray(images) ? images : [])
  .map(asImageUrl)
  .filter(Boolean)
  .map((image) => validateTvImageUrl(image, domains));
const normalizeVariants = (variants, imageDomains = []) => {
  const groups = new Map();
  for (const input of Array.isArray(variants) ? variants : []) {
    const variant = asObject(input);
    const variantKey = String(variant.variant_key || variant.variant || variant.screen_size || "").trim();
    if (!variantKey) continue;
    const key = variantKey.toLowerCase();
    const existing = groups.get(key) || {
      variant_key: variantKey,
      screen_size: variant.screen_size || null,
      screen_size_value: variant.screen_size_value ?? null,
      base_price: variant.base_price ?? null,
      attributes: asObject(variant.attributes),
      images: normalizeImages(variant.images, imageDomains),
      store_prices: [],
    };
    const stores = variant.store_prices || variant.stores || [];
    for (const storeInput of Array.isArray(stores) ? stores : []) {
      const store = asObject(storeInput);
      const storeName = String(store.store_name || store.store || store.storeName || "").trim();
      const url = store.url || store.store_url || store.link || store.affiliate_url || null;
      if (!storeName || !url) continue;
      existing.store_prices.push({
        store_name: storeName,
        price: store.price ?? store.current_price ?? store.sale_price ?? null,
        url,
        offer_text: store.offer_text || store.offerText || null,
        delivery_info: store.delivery_info || store.deliveryInfo || null,
      });
    }
    groups.set(key, existing);
  }
  return [...groups.values()];
};

const toCanonicalTvPayload = (input = {}, { imageDomains = [] } = {}) => {
  const product = asObject(input.product);
  const basic = asObject(input.basic_info_json);
  const variants = normalizeVariants(input.variants || input.variants_json, imageDomains);
  const images = normalizeImages(input.images_json || input.images, imageDomains);
  return {
    product_name: String(input.product_name || input.name || product.name || basic.title || input.model || "").trim(),
    brand_id: input.brand_id ?? product.brand_id ?? null,
    brand_name: input.brand_name || input.brand || product.brand_name || product.brand || basic.brand_name || basic.brand || null,
    model: String(input.model || basic.model_number || basic.model || "").trim(),
    manufacturer_model: input.manufacturer_model || input.model || basic.model_number || basic.model || null,
    category: input.category || null,
    launch_date: input.launch_date || null,
    publish: Boolean(input.publish),
    images_json: images,
    variants,
    sections: Object.fromEntries(TV_SECTIONS.map((key) => [key, asObject(input[key])])),
  };
};

async function createTvCatalogRecord(client, input, { resolveBrandId, parseDate, imageDomains = [] } = {}) {
  const payload = toCanonicalTvPayload(input, { imageDomains });
  if (!payload.product_name) throw new Error("product_name is required");
  if (!payload.model) throw new Error("model is required");
  const brandId = await resolveBrandId(client, payload.brand_id, payload.brand_name);
  if (!brandId) throw new Error("brand is required and must reference an existing brand");

  const productResult = await client.query(
    "INSERT INTO products (name, brand_id, product_type) VALUES ($1,$2,'tv') RETURNING id",
    [payload.product_name, brandId],
  );
  const productId = productResult.rows[0].id;
  const json = (value) => JSON.stringify(value ?? null);
  const variantsJson = payload.variants.map((variant) => ({
    variant_key: variant.variant_key,
    screen_size: variant.screen_size,
    screen_size_value: variant.screen_size_value,
    base_price: variant.base_price,
    store_prices: variant.store_prices,
    images: variant.images,
    ...variant.attributes,
  }));
  await client.query(
    `INSERT INTO tvs (product_id, category, model, manufacturer_model, launch_date,
      key_specs_json, basic_info_json, display_json, video_engine_json, audio_json,
      smart_tv_json, gaming_json, ports_json, connectivity_json, power_json, physical_json,
      product_details_json, in_the_box_json, warranty_json, images_json, variants_json)
     VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,
      $12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,
      $20::jsonb,$21::jsonb)`,
    [productId, payload.category, payload.model, payload.manufacturer_model,
      parseDate ? parseDate(payload.launch_date) : payload.launch_date,
      ...TV_SECTIONS.map((key) => json(payload.sections[key])), json(payload.images_json), json(variantsJson)],
  );

  for (let index = 0; index < payload.images_json.length; index += 1) {
    await client.query(
      "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
      [productId, payload.images_json[index], index + 1],
    );
  }
  for (const variant of payload.variants) {
    const variantResult = await client.query(
      "INSERT INTO product_variants (product_id, variant_key, attributes, base_price) VALUES ($1,$2,$3::jsonb,$4) RETURNING id",
      [productId, variant.variant_key, json({ ...variant.attributes, screen_size: variant.screen_size, screen_size_value: variant.screen_size_value }), variant.base_price],
    );
    const variantId = variantResult.rows[0].id;
    for (const store of variant.store_prices) {
      await client.query(
        `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (variant_id, store_name) DO UPDATE SET price=EXCLUDED.price, url=EXCLUDED.url,
           offer_text=EXCLUDED.offer_text, delivery_info=EXCLUDED.delivery_info`,
        [variantId, store.store_name, store.price, store.url, store.offer_text, store.delivery_info],
      );
    }
    for (let index = 0; index < variant.images.length; index += 1) {
      await client.query(
        `INSERT INTO product_variant_images (variant_id, image_url, position)
         VALUES ($1,$2,$3) ON CONFLICT (variant_id, image_url) DO UPDATE SET position=EXCLUDED.position`,
        [variantId, variant.images[index], index + 1],
      );
    }
  }
  await client.query("INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)", [productId, payload.publish]);
  return { productId, payload };
}

async function persistTvUpdate(client, productId, input, { resolveBrandId, parseDate, publish, imageDomains = [] } = {}) {
  const payload = toCanonicalTvPayload(input, { imageDomains });
  if (!payload.product_name) throw new Error("product_name is required");
  if (!payload.model) throw new Error("model is required");
  const brandId = await resolveBrandId(client, payload.brand_id, payload.brand_name);
  if (!brandId) throw new Error("brand is required and must reference an existing brand");

  const json = (value) => JSON.stringify(value ?? null);
  const variantsJson = payload.variants.map((variant) => ({
    variant_key: variant.variant_key,
    screen_size: variant.screen_size,
    screen_size_value: variant.screen_size_value,
    base_price: variant.base_price,
    store_prices: variant.store_prices,
    images: variant.images,
    ...variant.attributes,
  }));

  await client.query("UPDATE products SET name = $1, brand_id = $2 WHERE id = $3", [payload.product_name, brandId, productId]);
  await client.query(
    `UPDATE tvs SET category=$1, model=$2, manufacturer_model=$3, launch_date=$4,
      key_specs_json=$5::jsonb, basic_info_json=$6::jsonb, display_json=$7::jsonb,
      video_engine_json=$8::jsonb, audio_json=$9::jsonb, smart_tv_json=$10::jsonb,
      gaming_json=$11::jsonb, ports_json=$12::jsonb, connectivity_json=$13::jsonb,
      power_json=$14::jsonb, physical_json=$15::jsonb, product_details_json=$16::jsonb,
      in_the_box_json=$17::jsonb, warranty_json=$18::jsonb, images_json=$19::jsonb,
      variants_json=$20::jsonb WHERE product_id=$21`,
    [payload.category, payload.model, payload.manufacturer_model,
      parseDate ? parseDate(payload.launch_date) : payload.launch_date,
      ...TV_SECTIONS.map((key) => json(payload.sections[key])), json(payload.images_json), json(variantsJson), productId],
  );

  await client.query("DELETE FROM product_images WHERE product_id = $1", [productId]);
  const oldVariants = await client.query("SELECT id FROM product_variants WHERE product_id = $1", [productId]);
  for (const variant of oldVariants.rows) {
    await client.query("DELETE FROM product_variant_images WHERE variant_id = $1", [variant.id]);
    await client.query("DELETE FROM variant_store_prices WHERE variant_id = $1", [variant.id]);
  }
  await client.query("DELETE FROM product_variants WHERE product_id = $1", [productId]);

  for (let index = 0; index < payload.images_json.length; index += 1) {
    await client.query("INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)", [productId, payload.images_json[index], index + 1]);
  }
  for (const variant of payload.variants) {
    const variantResult = await client.query(
      "INSERT INTO product_variants (product_id, variant_key, attributes, base_price) VALUES ($1,$2,$3::jsonb,$4) RETURNING id",
      [productId, variant.variant_key, json({ ...variant.attributes, screen_size: variant.screen_size, screen_size_value: variant.screen_size_value }), variant.base_price],
    );
    const variantId = variantResult.rows[0].id;
    for (const store of variant.store_prices) {
      await client.query(
        `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info)
         VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (variant_id, store_name)
         DO UPDATE SET price=EXCLUDED.price, url=EXCLUDED.url, offer_text=EXCLUDED.offer_text, delivery_info=EXCLUDED.delivery_info`,
        [variantId, store.store_name, store.price, store.url, store.offer_text, store.delivery_info],
      );
    }
    for (let index = 0; index < variant.images.length; index += 1) {
      await client.query(
        `INSERT INTO product_variant_images (variant_id, image_url, position) VALUES ($1,$2,$3)
         ON CONFLICT (variant_id, image_url) DO UPDATE SET position=EXCLUDED.position`,
        [variantId, variant.images[index], index + 1],
      );
    }
  }
  if (publish !== undefined) {
    await client.query(
      `INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)
       ON CONFLICT (product_id) DO UPDATE SET is_published=EXCLUDED.is_published`,
      [productId, publish],
    );
  }
  return { productId, payload };
}

module.exports = { TV_SECTIONS, validateTvImageUrl, toCanonicalTvPayload, createTvCatalogRecord, persistTvUpdate };
