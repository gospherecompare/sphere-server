const express = require("express");
const ExcelJS = require("exceljs");
const SheetJS = require("xlsx");
const multer = require("multer");
const { db } = require("../db");
const {
  createTvCatalogRecord,
  validateTvImageUrl,
} = require("../services/tvCatalogService");

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });
const JSON_COLUMNS = [
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
  "images_json",
  "variants_json",
];

const normalize = (value) =>
  String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
const normalizeHeader = (value) => normalize(value).replace(/[\s_-]+/g, "");
const HEADER_ALIASES = {
  key_specs_json: [
    "key_specs_json",
    "key_specs",
    "key specifications",
    "specifications_json",
  ],
  basic_info_json: [
    "basic_info_json",
    "basic_info",
    "basic information_json",
    "basic information",
  ],
  display_json: [
    "display_json",
    "display",
    "display_specs_json",
    "display specifications",
  ],
  video_engine_json: [
    "video_engine_json",
    "video_engine",
    "video processor_json",
    "video processor",
    "performance_json",
    "performance",
  ],
  audio_json: ["audio_json", "audio", "audio_specs_json"],
  smart_tv_json: [
    "smart_tv_json",
    "smart_tv",
    "smart tv",
    "smart_features_json",
  ],
  gaming_json: ["gaming_json", "gaming", "gaming_features_json"],
  ports_json: ["ports_json", "ports", "port_specs_json"],
  connectivity_json: [
    "connectivity_json",
    "connectivity",
    "connectivity_specs_json",
  ],
  power_json: ["power_json", "power", "power_specs_json"],
  physical_json: [
    "physical_json",
    "physical",
    "dimensions_json",
    "physical_specs_json",
  ],
  product_details_json: [
    "product_details_json",
    "product_details",
    "details_json",
    "details",
  ],
  in_the_box_json: [
    "in_the_box_json",
    "in_the_box",
    "box_contents_json",
    "box contents",
  ],
  warranty_json: ["warranty_json", "warranty", "warranty_details_json"],
  images_json: ["images_json", "images", "image_urls", "image_urls_json"],
  variants_json: ["variants_json", "variants"],
};
const valueOf = (row, headers, name) => {
  const aliases = HEADER_ALIASES[name] || [name];
  const column = aliases
    .map((alias) => headers[normalizeHeader(alias)])
    .find(Boolean);
  if (!column) return null;
  const value = row.getCell(column).value;
  if (value === null || value === undefined) return null;
  if (typeof value === "object") {
    if (typeof value.text === "string") return value.text;
    if (Array.isArray(value.richText))
      return value.richText.map((part) => part.text || "").join("");
    if (value.result !== undefined) return value.result;
    if (value.hyperlink && value.text) return value.text;
  }
  return value;
};
const headersOf = (sheet) => {
  const headers = {};
  sheet.getRow(1).eachCell((cell, column) => {
    headers[normalizeHeader(cell.value)] = column;
  });
  return headers;
};
const firstValue = (row, headers, names) =>
  names
    .map((name) => valueOf(row, headers, name))
    .find(
      (value) =>
        value !== null && value !== undefined && String(value).trim() !== "",
    );
const parseDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
};
const parseJson = (value, column) => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "object") return value;
  const raw = String(value).trim();
  try {
    return JSON.parse(raw);
  } catch {
    try {
      return JSON.parse(
        raw
          .replace(/[\u2018\u2019]/g, "'")
          .replace(/[\u201C\u201D]/g, '"')
          .replace(/'/g, '"'),
      );
    } catch {
      throw new Error(`Invalid JSON in column: ${column}`);
    }
  }
};
const parseJsonColumn = (value, column) => {
  if (column !== "images_json") return parseJson(value, column);
  if (value === null || value === undefined || value === "") return null;
  try {
    const parsed = parseJson(value, column);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch (error) {
    const urls = String(value)
      .split(/[\r\n,|]+/)
      .map((url) => url.trim())
      .filter(Boolean);
    if (urls.every((url) => /^https?:\/\//i.test(url))) return urls;
    throw error;
  }
};
const parseNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(String(value).replace(/[^\d.-]/g, ""));
  return Number.isFinite(number) ? number : null;
};
const normalizeHost = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/^www\./, "");
const imageUrlIdentity = (value) => normalize(value).replace(/[^a-z0-9]/g, "");
const isOfficialImageUrl = (url, domains) => {
  const parsed = new URL(url);
  if (parsed.protocol !== "https:") return false;
  const host = normalizeHost(parsed.hostname);
  return domains.some(
    (domain) => host === domain || host.endsWith(`.${domain}`),
  );
};
const validateImageUrl = async (url, { domains } = {}) =>
  validateTvImageUrl(url, domains);
const imageInputsFor = (tv) =>
  Array.isArray(tv.fields.images_json)
    ? tv.fields.images_json
        .map((image) =>
          typeof image === "string" ? image : image?.image_url || image?.url,
        )
        .filter(Boolean)
    : [];
const previewIdentity = (tv) => ({
  row: tv.rowNumber,
  brand_name: tv.brandName,
  product_name: tv.productName,
  model: tv.model,
  manufacturer_model: tv.manufacturerModel,
  category: tv.category,
  launch_date: tv.launchDate,
});

async function loadImportWorkbook(buffer) {
  const workbook = new ExcelJS.Workbook();
  try {
    await workbook.xlsx.load(buffer);
    if (workbook.worksheets.length) return workbook;
  } catch {
    // Some valid Excel workbooks are not compatible with ExcelJS's XML parser.
  }

  const parsed = SheetJS.read(buffer, { cellDates: true });
  const sheets = parsed.SheetNames.map((name) => {
    const values = SheetJS.utils.sheet_to_json(parsed.Sheets[name], {
      header: 1,
      defval: null,
      raw: true,
    });
    return {
      name,
      rowCount: values.length,
      getRow(rowNumber) {
        const row = values[rowNumber - 1] || [];
        return {
          getCell(columnNumber) {
            return { value: row[columnNumber - 1] ?? null };
          },
          eachCell(callback) {
            row.forEach((value, index) => callback({ value }, index + 1));
          },
        };
      },
    };
  });
  if (!sheets.length) throw new Error("Workbook contains no worksheets");
  return {
    worksheets: sheets,
    getWorksheet(name) {
      return sheets.find((sheet) => sheet.name === name);
    },
  };
}
const sheetRows = (sheet, mapRow) => {
  if (!sheet) return [];
  const headers = headersOf(sheet);
  const rows = [];
  for (let rowNumber = 2; rowNumber <= sheet.rowCount; rowNumber += 1)
    rows.push(mapRow(sheet.getRow(rowNumber), headers, rowNumber));
  return rows.filter(Boolean);
};

async function findModelMatch(client, brandName, identityModel) {
  const result = await client.query(
    `SELECT t.product_id, p.name, t.model, t.manufacturer_model, t.launch_date
       FROM tvs t JOIN products p ON p.id = t.product_id JOIN brands b ON b.id = p.brand_id
      WHERE p.product_type = 'tv' AND LOWER(TRIM(b.name)) = $1
        AND LOWER(REGEXP_REPLACE(TRIM(COALESCE(t.manufacturer_model, t.model)), '\\s+', ' ', 'g')) = $2
      LIMIT 1`,
    [normalize(brandName), normalize(identityModel)],
  );
  return result.rows[0] || null;
}

router.get("/tvs/template", async (req, res) => {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "MobilesX";

  const tvSheet = workbook.addWorksheet("TVs");
  tvSheet.columns = [
    "brand_name",
    "product_name",
    "model",
    "manufacturer_model",
    "category",
    "launch_date",
    "source_page_url",
    "images_json",
    "publish",
    ...JSON_COLUMNS.filter((column) => column !== "images_json"),
  ].map((header) => ({
    header,
    key: header,
    width: Math.max(header.length + 2, 18),
  }));
  tvSheet.addRow({
    brand_name: "Samsung",
    product_name: "Samsung QN90F 65-inch",
    model: "QN90F 65-inch",
    manufacturer_model: "QN90F-65IN-2025",
    category: "QLED TV",
    launch_date: "2025-01-01",
    source_page_url: "https://www.samsung.com/in/tvs/qled-4k/qn90f/",
    images_json: '["https://official-cdn.example.com/qn90f-65.jpg"]',
    publish: "false",
  });
  tvSheet.getRow(1).font = { bold: true, color: { argb: "FFFFFFFF" } };
  tvSheet.getRow(1).fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF334155" },
  };
  tvSheet.freezePanes = { row: 2 };

  const combinedSheet = workbook.addWorksheet("Variants_StorePrices");
  combinedSheet.columns = [
    ["brand_name", 18],
    ["manufacturer_model", 24],
    ["product_name", 28],
    ["variant_key", 18],
    ["base_price", 14],
    ["screen_size", 14],
    ["screen_size_value", 18],
    ["attributes_json", 24],
    ["store_name", 18],
    ["price", 14],
    ["store_url", 40],
    ["offer_text", 24],
    ["delivery_info", 24],
    ["source_url", 40],
  ].map(([header, width]) => ({ header, key: header, width }));
  combinedSheet.addRows([
    {
      brand_name: "Samsung",
      manufacturer_model: "QN90F-65IN-2025",
      product_name: "Samsung QN90F 65-inch",
      variant_key: "65-inch",
      base_price: 149999,
      screen_size: "65-inch",
      screen_size_value: 65,
      attributes_json: '{"panel":"QLED"}',
      store_name: "Amazon",
      price: 149999,
      store_url: "https://example.com/amazon-product",
      source_url: "https://example.com/source",
    },
    {
      brand_name: "Samsung",
      manufacturer_model: "QN90F-65IN-2025",
      product_name: "Samsung QN90F 65-inch",
      variant_key: "65-inch",
      base_price: 149999,
      screen_size: "65-inch",
      screen_size_value: 65,
      attributes_json: '{"panel":"QLED"}',
      store_name: "Flipkart",
      price: null,
      store_url: "https://example.com/flipkart-product",
      source_url: "https://example.com/source",
    },
  ]);
  combinedSheet.getRow(1).font = { bold: true };

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    'attachment; filename="MobilesX_TV_Import_Template.xlsx"',
  );
  await workbook.xlsx.write(res);
  res.end();
});

function classifyModelMatch(match, launchDate) {
  if (!match) return null;
  const existingDate =
    match.launch_date && new Date(match.launch_date).toISOString().slice(0, 10);
  if (existingDate && launchDate && existingDate !== launchDate)
    return "MODEL_MATCH_DATE_CONFLICT";
  return "DUPLICATE";
}

function readWorkbook(workbook) {
  const tvSheet = workbook.getWorksheet("TVs") || workbook.worksheets[0];
  if (!tvSheet) throw new Error("Worksheet TVs not found");
  const variantsSheet = workbook.getWorksheet("Variants");
  const pricesSheet = workbook.getWorksheet("StorePrices");
  const combinedSheet = workbook.getWorksheet("Variants_StorePrices");
  const tvs = sheetRows(tvSheet, (row, headers, rowNumber) => {
    const brandName = String(
      firstValue(row, headers, ["brand_name", "brand"]) || "",
    ).trim();
    const productName = String(
      firstValue(row, headers, ["product_name", "name"]) || "",
    ).trim();
    const displayModel = String(
      firstValue(row, headers, ["model"]) || "",
    ).trim();
    const manufacturerModel = String(
      firstValue(row, headers, ["manufacturer_model"]) || displayModel,
    ).trim();
    const model = displayModel || manufacturerModel;
    const rawLaunchDate = firstValue(row, headers, ["launch_date"]);
    const launchDate = parseDate(rawLaunchDate);
    const sourcePageUrl = String(
      firstValue(row, headers, [
        "source_page_url",
        "source_page",
        "official_product_url",
        "product_url",
      ]) || "",
    ).trim();
    const fields = {};
    for (const column of JSON_COLUMNS)
      fields[column] = parseJsonColumn(valueOf(row, headers, column), column);
    return {
      rowNumber,
      brandName,
      productName,
      model,
      manufacturerModel,
      sourcePageUrl,
      category: String(firstValue(row, headers, ["category"]) || "").trim(),
      launchDate,
      invalidDate: Boolean(rawLaunchDate && !launchDate),
      publish:
        String(firstValue(row, headers, ["publish"]) || "").toLowerCase() ===
        "true",
      fields,
    };
  });
  const variantRows = sheetRows(variantsSheet, (row, headers, rowNumber) => ({
    rowNumber,
    brandName: String(
      firstValue(row, headers, ["brand_name", "brand"]) || "",
    ).trim(),
    model: String(
      firstValue(row, headers, ["manufacturer_model", "model"]) || "",
    ).trim(),
    productName: String(
      firstValue(row, headers, ["product_name", "tv_name"]) || "",
    ).trim(),
    variantKey: String(
      firstValue(row, headers, ["variant_key", "variant", "screen_size"]) || "",
    ).trim(),
    basePrice: parseNumber(firstValue(row, headers, ["base_price", "price"])),
    screenSize: firstValue(row, headers, ["screen_size"]),
    screenSizeValue: firstValue(row, headers, ["screen_size_value"]),
    attributes: {
      ...(parseJson(
        firstValue(row, headers, ["attributes_json", "attributes"]),
        "attributes_json",
      ) || {}),
      ...(firstValue(row, headers, ["screen_size"]) !== null
        ? { screen_size: firstValue(row, headers, ["screen_size"]) }
        : {}),
      ...(firstValue(row, headers, ["screen_size_value"]) !== null
        ? { screen_size_value: firstValue(row, headers, ["screen_size_value"]) }
        : {}),
    },
  }));
  const priceRows = sheetRows(pricesSheet, (row, headers, rowNumber) => ({
    rowNumber,
    brandName: String(
      firstValue(row, headers, ["brand_name", "brand"]) || "",
    ).trim(),
    model: String(
      firstValue(row, headers, ["manufacturer_model", "model"]) || "",
    ).trim(),
    productName: String(
      firstValue(row, headers, ["product_name", "tv_name"]) || "",
    ).trim(),
    variantKey: String(
      firstValue(row, headers, ["variant_key", "variant", "screen_size"]) || "",
    ).trim(),
    storeName: String(
      firstValue(row, headers, ["store_name", "store"]) || "",
    ).trim(),
    price: parseNumber(firstValue(row, headers, ["price", "current_price"])),
    priceStatus:
      firstValue(row, headers, ["price", "current_price"]) === null ||
      firstValue(row, headers, ["price", "current_price"]) === undefined ||
      firstValue(row, headers, ["price", "current_price"]) === ""
        ? "UNKNOWN"
        : "KNOWN",
    url: String(
      firstValue(row, headers, ["url", "store_url", "link"]) || "",
    ).trim(),
    offerText: firstValue(row, headers, ["offer_text", "offer"]),
    deliveryInfo: firstValue(row, headers, ["delivery_info", "delivery"]),
  }));
  if (combinedSheet) {
    const combinedRows = sheetRows(
      combinedSheet,
      (row, headers, rowNumber) => ({
        rowNumber,
        brandName: String(
          firstValue(row, headers, ["brand_name", "brand"]) || "",
        ).trim(),
        model: String(
          firstValue(row, headers, ["manufacturer_model", "model"]) || "",
        ).trim(),
        productName: String(
          firstValue(row, headers, ["product_name", "product", "tv_name"]) ||
            "",
        ).trim(),
        variantKey: String(
          firstValue(row, headers, ["variant_key", "variant", "screen_size"]) ||
            "",
        ).trim(),
        basePrice: parseNumber(firstValue(row, headers, ["base_price"])),
        screenSize: firstValue(row, headers, ["screen_size"]),
        screenSizeValue: firstValue(row, headers, ["screen_size_value"]),
        attributes: {
          ...(parseJson(
            firstValue(row, headers, ["attributes_json", "attributes"]),
            "attributes_json",
          ) || {}),
          ...(firstValue(row, headers, ["screen_size"]) !== null
            ? { screen_size: firstValue(row, headers, ["screen_size"]) }
            : {}),
          ...(firstValue(row, headers, ["screen_size_value"]) !== null
            ? {
                screen_size_value: firstValue(row, headers, [
                  "screen_size_value",
                ]),
              }
            : {}),
        },
        images: (() => {
          const value = firstValue(row, headers, [
            "variant_images_json",
            "variant_images",
            "images_json",
            "images",
            "image_url",
          ]);
          if (!value) return [];
          try {
            return parseJsonColumn(value, "images_json") || [];
          } catch {
            return [String(value).trim()];
          }
        })(),
        storeName: String(
          firstValue(row, headers, ["store_name", "store"]) || "",
        ).trim(),
        price: parseNumber(
          firstValue(row, headers, ["price", "current_price"]),
        ),
        priceStatus:
          firstValue(row, headers, ["price", "current_price"]) === null ||
          firstValue(row, headers, ["price", "current_price"]) === undefined ||
          firstValue(row, headers, ["price", "current_price"]) === ""
            ? "UNKNOWN"
            : "KNOWN",
        url: String(
          firstValue(row, headers, ["url", "store_url", "link"]) || "",
        ).trim(),
        offerText: firstValue(row, headers, ["offer_text", "offer"]),
        deliveryInfo: firstValue(row, headers, ["delivery_info", "delivery"]),
      }),
    );
    const uniqueVariants = new Map();
    for (const row of combinedRows) {
      const key = [row.brandName, row.model, row.productName, row.variantKey]
        .map(normalize)
        .join("|");
      const existing = uniqueVariants.get(key);
      if (!existing) uniqueVariants.set(key, row);
      else
        existing.images = [
          ...new Set([...(existing.images || []), ...(row.images || [])]),
        ];
    }
    return {
      tvs,
      variants: [...uniqueVariants.values()],
      storePrices: combinedRows,
    };
  }
  return { tvs, variants: variantRows, storePrices: priceRows };
}

router.post("/tvs", upload.single("file"), async (req, res) => {
  if (!req.file?.buffer)
    return res.status(400).json({ message: "No file uploaded" });
  let workbook;
  try {
    workbook = await loadImportWorkbook(req.file.buffer);
  } catch (error) {
    return res.status(400).json({ message: `Invalid XLSX: ${error.message}` });
  }
  let workbookData;
  try {
    workbookData = readWorkbook(workbook);
  } catch (error) {
    return res.status(400).json({ message: error.message });
  }

  const preview = normalize(req.query.preview) === "true";
  const client = await db.connect();
  const results = [];
  let workbookTransactionOpen = false;
  try {
    if (!preview) {
      await client.query("BEGIN");
      workbookTransactionOpen = true;
    }
    for (const tv of workbookData.tvs) {
      const savepoint = `tv_import_${tv.rowNumber}`;
      try {
        if (!preview) await client.query(`SAVEPOINT ${savepoint}`);
        const invalid = [];
        if (!tv.brandName) invalid.push("brand_name");
        if (!tv.productName) invalid.push("product_name");
        if (!tv.model) invalid.push("manufacturer_model/model");
        if (!tv.category) invalid.push("category");
        if (tv.invalidDate) invalid.push("launch_date");
        if (invalid.length) {
          results.push({
            ...previewIdentity(tv),
            status: "INVALID",
            reason: `Required or invalid fields: ${invalid.join(", ")}`,
          });
          continue;
        }

        const brandResult = await client.query(
          "SELECT id FROM brands WHERE LOWER(TRIM(name)) = $1 LIMIT 1",
          [normalize(tv.brandName)],
        );
        if (!brandResult.rows[0]) {
          results.push({
            ...previewIdentity(tv),
            status: "INVALID",
            reason: `Brand not found: ${tv.brandName}`,
          });
          continue;
        }
        const brandId = brandResult.rows[0].id;
        const domainResult = await client.query(
          "SELECT domain FROM official_brand_domains WHERE brand_id = $1 AND is_active = true",
          [brandId],
        );
        const officialDomains = domainResult.rows.map((row) =>
          normalizeHost(row.domain),
        );
        const imageInputs = imageInputsFor(tv);
        if (imageInputs.length && !officialDomains.length) {
          results.push({
            ...previewIdentity(tv),
            status: "INVALID",
            reason:
              "No approved official image domains configured for this brand",
          });
          continue;
        }
        const modelMatch = await findModelMatch(
          client,
          tv.brandName,
          tv.manufacturerModel,
        );
        const modelStatus = classifyModelMatch(modelMatch, tv.launchDate);
        if (modelStatus) {
          results.push({
            ...previewIdentity(tv),
            status: modelStatus,
            product_id: modelMatch.product_id,
            reason:
              modelStatus === "DUPLICATE"
                ? "Brand and manufacturer model already exist"
                : "Manufacturer model exists with a different launch date",
          });
          continue;
        }

        const nameResult = await client.query(
          "SELECT id, brand_id, product_type FROM products WHERE LOWER(TRIM(name)) = $1 LIMIT 1",
          [normalize(tv.productName)],
        );
        if (nameResult.rows[0]?.product_type === "tv") {
          results.push({
            ...previewIdentity(tv),
            status: "POSSIBLE_DUPLICATE",
            product_id: nameResult.rows[0].id,
            reason: "TV product name already exists with a different model",
          });
          continue;
        }
        if (nameResult.rows[0]) {
          results.push({
            ...previewIdentity(tv),
            status: "NOT_TV_DUPLICATE",
            product_id: nameResult.rows[0].id,
            reason: `Name belongs to ${nameResult.rows[0].product_type}; products.name is globally unique`,
          });
          continue;
        }

        const key = normalize(tv.productName);
        const variants = workbookData.variants.filter(
          (variant) =>
            (!variant.brandName ||
              normalize(variant.brandName) === normalize(tv.brandName)) &&
            (!variant.model ||
              normalize(variant.model) === normalize(tv.manufacturerModel)) &&
            (!variant.productName || normalize(variant.productName) === key),
        );
        const prices = workbookData.storePrices.filter(
          (price) =>
            (!price.brandName ||
              normalize(price.brandName) === normalize(tv.brandName)) &&
            (!price.model ||
              normalize(price.model) === normalize(tv.manufacturerModel)) &&
            (!price.productName || normalize(price.productName) === key),
        );
        const variantKeys = new Set(
          variants.map((variant) => normalize(variant.variantKey)),
        );
        const orphanPrices = prices.filter(
          (price) =>
            !variantKeys.has(normalize(price.variantKey)) ||
            !price.storeName ||
            !price.url,
        );
        if (orphanPrices.length) {
          results.push({
            ...previewIdentity(tv),
            status: "INVALID",
            reason: `Invalid or orphan StorePrices rows: ${orphanPrices.map((price) => price.rowNumber).join(", ")}`,
          });
          continue;
        }
        const duplicateVariant = variants.find((variant, index) =>
          variants
            .slice(0, index)
            .some(
              (prior) =>
                normalize(prior.variantKey) === normalize(variant.variantKey),
            ),
        );
        if (
          duplicateVariant ||
          variants.some((variant) => !variant.variantKey)
        ) {
          results.push({
            ...previewIdentity(tv),
            status: "INVALID",
            reason: "Duplicate or missing variant_key",
          });
          continue;
        }

        let images = [];
        for (const imageUrl of imageInputs) {
          images.push(
            await validateImageUrl(String(imageUrl).trim(), {
              domains: officialDomains,
              model: tv.model,
              productName: tv.productName,
              sourcePageUrl: tv.sourcePageUrl || undefined,
            }),
          );
        }
        const unknownStorePrices = prices.filter(
          (price) => price.price === null,
        ).length;
        const base = {
          ...previewIdentity(tv),
          variants: variants.length,
          store_prices: prices.length,
          unknown_store_prices: unknownStorePrices,
          store_price_statuses: prices.map((price) => ({
            variant_key: price.variantKey,
            store_name: price.storeName,
            price_status: price.priceStatus,
          })),
          images: images.length,
          image_status: images.length ? "validated_direct_url" : "none",
        };
        if (preview) {
          results.push({ ...base, status: "NEW" });
          continue;
        }

        const catalogVariants = variants.map((variant) => ({
          variant_key: variant.variantKey,
          screen_size: variant.screenSize,
          screen_size_value: variant.screenSizeValue,
          base_price: variant.basePrice,
          attributes: variant.attributes,
          images: variant.images || [],
          store_prices: prices
            .filter(
              (price) =>
                normalize(price.variantKey) === normalize(variant.variantKey),
            )
            .map((price) => ({
              store_name: price.storeName,
              price: price.price,
              url: price.url,
              offer_text: price.offerText,
              delivery_info: price.deliveryInfo,
            })),
        }));
        const { productId } = await createTvCatalogRecord(
          client,
          {
            product_name: tv.productName,
            brand_id: brandId,
            brand_name: tv.brandName,
            model: tv.model,
            manufacturer_model: tv.manufacturerModel,
            category: tv.category,
            launch_date: tv.launchDate,
            publish: tv.publish,
            images_json: images,
            variants: catalogVariants,
            ...tv.fields,
          },
          {
            resolveBrandId: async (_client, id) => id,
            parseDate,
            imageDomains: officialDomains,
          },
        );
        if (!preview) await client.query(`RELEASE SAVEPOINT ${savepoint}`);
        results.push({ ...base, status: "READY", product_id: productId });
      } catch (error) {
        if (!preview) await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
        results.push({
          row: tv.rowNumber,
          brand_name: tv.brandName,
          product_name: tv.productName,
          manufacturer_model: tv.manufacturerModel,
          status: "INVALID",
          reason: error.message,
        });
      }
    }
    if (!preview) {
      await client.query("COMMIT");
      workbookTransactionOpen = false;
    }
    return res.json({
      preview,
      rows: results,
      sheets: {
        variants: workbookData.variants,
        store_prices: workbookData.storePrices,
      },
      summary: {
        ...results.reduce(
          (summary, row) => {
            const key = row.status.toLowerCase();
            summary[key] = (summary[key] || 0) + 1;
            return summary;
          },
          { total_rows: results.length },
        ),
        tv_rows: workbookData.tvs.length,
        variant_rows: workbookData.variants.length,
        store_price_rows: workbookData.storePrices.length,
        known_prices: workbookData.storePrices.filter(
          (price) => price.price !== null,
        ).length,
        unknown_prices: workbookData.storePrices.filter(
          (price) => price.price === null,
        ).length,
      },
    });
  } catch (error) {
    if (workbookTransactionOpen) await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
});

router.readWorkbook = readWorkbook;
router.loadImportWorkbook = loadImportWorkbook;
module.exports = router;
