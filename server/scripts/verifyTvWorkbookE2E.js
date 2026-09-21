
const fs = require("fs");
const { readWorkbook, loadImportWorkbook } = require("../routes/importTvs");
const { toCanonicalTvPayload } = require("../services/tvCatalogService");

const norm = (value) => String(value || "").trim().toLowerCase();

(async () => {
  const file = process.argv[2];
  if (!file) throw new Error("Workbook path required");
  const workbook = await loadImportWorkbook(fs.readFileSync(file));
  const data = readWorkbook(workbook);

  const tvByKey = new Map();
  for (const tv of data.tvs) {
    const key = [tv.brandName, tv.manufacturerModel, tv.productName].map(norm).join("|");
    tvByKey.set(key, tv);
  }

  const uniqueVariantKeys = new Set(
    data.variants.map((v) => [v.brandName, v.model, v.productName, v.variantKey].map(norm).join("|"))
  );

  const officialImageUrls = [];
  for (const tv of data.tvs) {
    for (const img of (tv.fields.images_json || [])) {
      const url = typeof img === "string" ? img : img?.url || img?.image_url;
      if (url) officialImageUrls.push(url);
    }
  }
  for (const v of data.variants) {
    for (const img of (v.images || [])) if (img) officialImageUrls.push(img);
  }

  const imageHosts = [...new Set(officialImageUrls.map((url) => new URL(url).hostname))];

  let canonicalFirst = null;
  const first = data.tvs[0];
  if (first) {
    const key = [first.brandName, first.manufacturerModel, first.productName].map(norm).join("|");
    const variants = data.variants
      .filter((v) =>
        (!v.brandName || norm(v.brandName) === norm(first.brandName)) &&
        (!v.model || norm(v.model) === norm(first.manufacturerModel)) &&
        (!v.productName || norm(v.productName) === norm(first.productName))
      )
      .map((v) => ({
        variant_key: v.variantKey,
        base_price: v.basePrice,
        screen_size: v.screenSize,
        screen_size_value: v.screenSizeValue,
        attributes: v.attributes,
        images: v.images,
        store_prices: data.storePrices
          .filter((p) =>
            norm(p.brandName || first.brandName) === norm(first.brandName) &&
            norm(p.model || first.manufacturerModel) === norm(first.manufacturerModel) &&
            norm(p.productName || first.productName) === norm(first.productName) &&
            norm(p.variantKey) === norm(v.variantKey)
          )
          .map((p) => ({
            store_name: p.storeName,
            price: p.price,
            url: p.url,
            offer_text: p.offerText,
            delivery_info: p.deliveryInfo,
          })),
      }));

    canonicalFirst = toCanonicalTvPayload({
      product_name: first.productName,
      brand_name: first.brandName,
      model: first.model,
      manufacturer_model: first.manufacturerModel,
      category: first.category,
      launch_date: first.launchDate,
      publish: first.publish,
      ...first.fields,
      variants,
    }, { imageDomains: imageHosts });
  }

  const checks = {
    tv_rows: data.tvs.length,
    unique_tv_keys: tvByKey.size,
    variant_rows: data.variants.length,
    unique_variant_keys: uniqueVariantKeys.size,
    store_price_rows: data.storePrices.length,
    tv_json_rows_complete: data.tvs.filter((tv) => tv.fields && tv.fields.key_specs_json !== null).length,
    image_url_count_including_variant_images: officialImageUrls.length,
    all_image_urls_https: officialImageUrls.every((url) => new URL(url).protocol === "https:"),
    official_image_hosts: imageHosts,
    first_tv_canonical_ok: Boolean(canonicalFirst?.product_name && canonicalFirst?.model),
    first_tv_variant_count: canonicalFirst?.variants.length ?? 0,
    first_tv_direct_image_count: canonicalFirst?.images_json.length ?? 0,
    first_tv_direct_variant_image_count: canonicalFirst?.variants.reduce((sum, v) => sum + v.images.length, 0) ?? 0,
  };

  console.log(JSON.stringify(checks, null, 2));
})();
