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
];

const normalizeIdentityValue = (value) =>
  String(value || "")
    .trim()
    .replace(/\s+/g, " ");
const normalizeSearchValue = (value) =>
  normalizeIdentityValue(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const emptyTvSections = () =>
  Object.fromEntries(TV_SECTIONS.map((key) => [key, {}]));

const sourceEvidenceSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    url: { type: "string" },
    domain: { type: "string" },
    source_type: { type: "string" },
    claims_checked: { type: "array", items: { type: "string" } },
  },
  required: ["url", "domain", "source_type", "claims_checked"],
};

const conflictSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    field: { type: "string" },
    sources: { type: "array", items: { type: "string" } },
    values: { type: "array", items: { type: "string" } },
    resolution: { type: "string" },
  },
  required: ["field", "sources", "values", "resolution"],
};

const imageCandidateSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    image_url: { type: "string" },
    source_page_url: { type: "string" },
    title: { type: "string" },
    model: { type: "string" },
  },
  required: ["image_url", "source_page_url", "title", "model"],
};

const TV_GENERATION_RESPONSE_FORMAT = {
  type: "object",
};

const SUPPORTED_TV_BRANDS = [
  "Samsung",
  "LG",
  "Sony",
  "Xiaomi",
  "OnePlus",
  "TCL",
  "Hisense",
  "Panasonic",
  "Vu",
];

const buildTvGenerationPrompt = ({
  productName,
  brandName,
  model,
  screenSizes = [],
}) => `
You are the MobilesX TV data ingestion engine.

Create a factual TV draft for this exact identity:
product_name: ${productName}
brand_name: ${brandName}
model: ${model}
requested_screen_sizes: ${JSON.stringify(screenSizes)}

Return only valid JSON using exactly these top-level keys:
product_name, brand_name, category, model, publish, source_evidence, conflicts, image_candidates, key_specs_json, basic_info_json, display_json, video_engine_json, audio_json, smart_tv_json, gaming_json, ports_json, connectivity_json, power_json, physical_json, product_details_json, in_the_box_json, warranty_json, variants_json.

Source rules:
1. Use the official manufacturer source first.
2. Cross-check 91mobiles, Beebom Gadgets, and Gadgets 360 when available.
3. Never guess, infer, or copy a value from another model.
4. Use null, {}, or [] when a value is not reliably published.
5. Preserve exact manufacturer wording where precision matters.
6. Keep size-dependent values under the correct screen-size key.
7. Treat different screen sizes of the same model as variants.
8. Do not generate images or image URLs; images_json is handled separately.
9. Set publish to false.
10. Return no commentary or markdown fences.
11. Return source_evidence with one entry per source actually consulted. Each entry must include url, domain, source_type, and claims_checked.
12. Return conflicts for disagreements between consulted sources; return [] only when none were found.
13. Return image_candidates from official manufacturer domains only; do not invent image URLs. Each candidate must include image_url, source_page_url, title, and model for the exact requested TV.

The JSON sections must match the existing MobilesX tvs table fields. Use variants_json as an array of objects containing variant_key, screen_size, screen_size_value, base_price, images, and store_prices where known.
`;

const buildAutomaticTvGenerationPrompt = () => `
You are the MobilesX TV data ingestion engine.

Find one currently relevant TV model sold or announced in India that is not already in the MobilesX catalog.
Choose exactly one manufacturer from this allowed list: ${SUPPORTED_TV_BRANDS.join(", ")}.
In the same response, research that exact TV and create its complete factual specification draft.

${buildTvGenerationPrompt({
  productName: "the discovered TV",
  brandName: "the discovered brand",
  model: "the discovered exact model number",
  screenSizes: [],
})}
`;

const parseJsonOutput = (text) => {
  const raw = String(text || "")
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "");
  try {
    return JSON.parse(raw);
  } catch {
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start < 0 || end <= start)
      throw new Error("Gemini returned invalid TV JSON");
    try {
      return JSON.parse(raw.slice(start, end + 1));
    } catch {
      throw new Error("Gemini returned invalid TV JSON");
    }
  }
};

const normalizeGeneratedTv = (value = {}, identity = {}) => {
  const source =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const normalized = {
    product_name: normalizeIdentityValue(
      source.product_name || identity.productName,
    ),
    brand_name: normalizeIdentityValue(source.brand_name || identity.brandName),
    category:
      normalizeIdentityValue(source.category || "television") || "television",
    model: normalizeIdentityValue(source.model || identity.model),
    publish: false,
    source_evidence: Array.isArray(source.source_evidence)
      ? source.source_evidence
      : [],
    conflicts: Array.isArray(source.conflicts) ? source.conflicts : [],
    image_candidates: Array.isArray(source.image_candidates)
      ? source.image_candidates
      : [],
    ...emptyTvSections(),
    variants_json: Array.isArray(source.variants_json)
      ? source.variants_json
      : [],
  };

  for (const key of TV_SECTIONS) {
    if (
      source[key] &&
      typeof source[key] === "object" &&
      !Array.isArray(source[key])
    ) {
      normalized[key] = source[key];
    }
  }
  normalized.images_json = [];
  return normalized;
};

const validateGeneratedTv = (draft) => {
  const errors = [];
  if (!draft.product_name) errors.push("product_name is required");
  if (!draft.brand_name) errors.push("brand_name is required");
  if (!draft.model) errors.push("model is required");
  if (draft.category && normalizeSearchValue(draft.category) !== "television") {
    errors.push("category must be television");
  }
  if (
    !Array.isArray(draft.source_evidence) ||
    draft.source_evidence.length === 0
  ) {
    errors.push("source_evidence must contain at least one consulted source");
  }
  for (const [index, evidence] of (draft.source_evidence || []).entries()) {
    if (
      !normalizeIdentityValue(evidence?.url) ||
      !normalizeIdentityValue(evidence?.domain)
    ) {
      errors.push(`source_evidence[${index}] requires url and domain`);
    }
    if (!Array.isArray(evidence?.claims_checked)) {
      errors.push(`source_evidence[${index}].claims_checked must be an array`);
    }
  }

  const keys = new Set();
  for (const [index, variant] of draft.variants_json.entries()) {
    const variantKey = normalizeIdentityValue(variant?.variant_key);
    const screenSize = normalizeIdentityValue(variant?.screen_size);
    if (!variantKey || !screenSize)
      errors.push(
        `variants_json[${index}] requires variant_key and screen_size`,
      );
    if (keys.has(variantKey))
      errors.push(`duplicate variant_key: ${variantKey}`);
    if (variantKey) keys.add(variantKey);
  }
  return errors;
};

const findExistingTv = async (db, identity) => {
  const brand = normalizeSearchValue(identity.brandName);
  const model = normalizeSearchValue(identity.model);
  const productName = normalizeSearchValue(identity.productName);
  const result = await db.query(
    `SELECT p.id AS product_id, p.name, b.name AS brand_name, t.model
     FROM products p
     INNER JOIN tvs t ON t.product_id = p.id
     LEFT JOIN brands b ON b.id = p.brand_id
     WHERE p.product_type = 'tv'
       AND (
         (LOWER(regexp_replace(COALESCE(b.name, ''), '[^a-z0-9]+', ' ', 'g')) = $1 AND LOWER(regexp_replace(COALESCE(t.model, ''), '[^a-z0-9]+', ' ', 'g')) = $2)
         OR LOWER(regexp_replace(p.name, '[^a-z0-9]+', ' ', 'g')) = $3
       )
     LIMIT 1`,
    [brand, model, productName],
  );
  return result.rows[0] || null;
};

const generateTvDraft = async ({
  db,
  generateContent,
  verifyEvidence,
  reserveCall,
  identity,
}) => {
  const duplicate = identity ? await findExistingTv(db, identity) : null;
  if (duplicate) {
    return { duplicate, draft: null, usage: { gemini_called: false } };
  }
  if (reserveCall) await reserveCall();

  const response = await generateContent({
    systemInstruction:
      "Return only a factual JSON object. Never invent unavailable TV specifications. Use Google Search grounding and cite the sources consulted in source_evidence.",
    prompt: identity
      ? buildTvGenerationPrompt(identity)
      : buildAutomaticTvGenerationPrompt(),
    requestId: `tv-generation-${Date.now()}`,
    tools: [
      { type: "google_search", search_types: ["web_search", "image_search"] },
    ],
    responseFormat: TV_GENERATION_RESPONSE_FORMAT,
  });
  const draft = normalizeGeneratedTv(
    parseJsonOutput(response.summary),
    identity || {},
  );
  const resolvedIdentity = {
    productName: draft.product_name,
    brandName: draft.brand_name,
    model: draft.model,
    screenSizes: draft.variants_json
      .map((variant) => normalizeIdentityValue(variant?.screen_size))
      .filter(Boolean),
  };
  const generatedDuplicate = identity
    ? null
    : await findExistingTv(db, resolvedIdentity);
  if (generatedDuplicate) {
    return {
      duplicate: generatedDuplicate,
      draft: null,
      identity: resolvedIdentity,
      usage: { gemini_called: true },
    };
  }
  const validationErrors = validateGeneratedTv(draft);
  const sourceVerification = verifyEvidence
    ? await verifyEvidence({
        evidence: draft.source_evidence,
        identity: identity || resolvedIdentity,
      })
    : null;
  if (sourceVerification && !sourceVerification.ok) {
    validationErrors.push(...sourceVerification.errors);
  }
  return {
    duplicate: null,
    draft,
    identity: resolvedIdentity,
    validation_errors: validationErrors,
    source_verification: sourceVerification,
    usage: {
      gemini_called: true,
      model: response.model,
      input_tokens: response.inputTokens,
      output_tokens: response.outputTokens,
      grounded: Boolean(response.grounding),
    },
  };
};

module.exports = {
  TV_SECTIONS,
  TV_GENERATION_RESPONSE_FORMAT,
  SUPPORTED_TV_BRANDS,
  buildTvGenerationPrompt,
  buildAutomaticTvGenerationPrompt,
  findExistingTv,
  normalizeGeneratedTv,
  parseJsonOutput,
  validateGeneratedTv,
  generateTvDraft,
};
