const test = require("node:test");
const assert = require("node:assert/strict");

const {
  resolveProductIdByEntityOrProductId,
} = require("../utils/productDeleteLookup");
const { toCanonicalTvPayload } = require("../services/tvCatalogService");

test("resolveProductIdByEntityOrProductId accepts either product_id or table id", async () => {
  const client = {
    async query(sql, params) {
      if (sql.includes("WHERE product_id = $1")) {
        if (params[0] === 99) {
          return { rows: [] };
        }
        return { rows: [{ product_id: 42 }] };
      }
      if (sql.includes("WHERE id = $1")) {
        if (params[0] === 99) {
          return { rows: [{ product_id: 42 }] };
        }
        return { rows: [] };
      }
      return { rows: [] };
    },
  };

  const productId = await resolveProductIdByEntityOrProductId(client, {
    tableName: "tvs",
    targetId: 99,
  });

  assert.equal(productId, 42);
});

test("resolveProductIdByEntityOrProductId returns null when no matching record exists", async () => {
  const client = {
    async query() {
      return { rows: [] };
    },
  };

  const productId = await resolveProductIdByEntityOrProductId(client, {
    tableName: "tvs",
    targetId: 404,
  });

  assert.equal(productId, null);
});

test("toCanonicalTvPayload strips empty strings and blank nested values", () => {
  const payload = toCanonicalTvPayload({
    product_name: "Demo TV",
    brand_name: "BrandX",
    model: "DM-01",
    category: "Smart TV",
    publish: true,
    key_specs_json: {
      screen_size: "55 inch",
      panel_type: "",
      refresh_rate: "   ",
      ports: ["", "4", null, "HDMI"],
      nested: {
        blank: "",
        keep: "yes",
        deep: {
          empty: "",
          keep2: "still here",
        },
      },
    },
  });

  assert.equal(payload.product_name, "Demo TV");
  assert.deepEqual(payload.sections.key_specs_json, {
    screen_size: "55 inch",
    ports: ["4", "HDMI"],
    nested: {
      keep: "yes",
      deep: {
        keep2: "still here",
      },
    },
  });
});

test("toCanonicalTvPayload preserves custom nested JSON in storage_json", () => {
  const payload = toCanonicalTvPayload({
    product_name: "Demo TV",
    brand_name: "BrandX",
    model: "DM-01",
    category: "Smart TV",
    publish: true,
    tuner_broadcasting_json: {
      digital_and_analog_tuner: "DVB-T2/T/C/S2/S",
      rf_tuner: 2,
      nicam_a2_stereo: true,
    },
    import_details_json: {
      source: "supplier-feed",
      supplier_id: "ABC-123",
    },
  });

  assert.deepEqual(payload.source_payload_json.tuner_broadcasting_json, {
    digital_and_analog_tuner: "DVB-T2/T/C/S2/S",
    rf_tuner: 2,
    nicam_a2_stereo: true,
  });
  assert.deepEqual(payload.sections.storage_json.tuner_broadcasting_json, {
    digital_and_analog_tuner: "DVB-T2/T/C/S2/S",
    rf_tuner: 2,
    nicam_a2_stereo: true,
  });
  assert.deepEqual(payload.sections.storage_json.import_details_json, {
    source: "supplier-feed",
    supplier_id: "ABC-123",
  });
});

test("toCanonicalTvPayload reads section data from nested source_payload_json.sections", () => {
  const payload = toCanonicalTvPayload({
    product: { name: "Hisense E7Q Pro", brand_name: "Hisense" },
    tv: {
      category: "Smart TV",
      model: "55E7Q Pro",
      source_payload_json: {
        sections: {
          key_specs_json: {
            resolution: "3840 x 2160",
            screen_size: "55 inch",
            hdr: ["Dolby Vision", "HDR10+"],
          },
          display_json: {
            panel_type: "VA",
            native_refresh_rate: "144Hz",
          },
          audio_json: {
            speaker_configuration: "2.0 Channel",
            audio_output: "24W",
          },
          gaming_json: {
            vrr: "48-144Hz",
            allm: true,
          },
        },
      },
    },
  });

  assert.deepEqual(payload.sections.key_specs_json, {
    resolution: "3840 x 2160",
    screen_size: "55 inch",
    hdr: ["Dolby Vision", "HDR10+"],
  });
  assert.deepEqual(payload.sections.display_json, {
    panel_type: "VA",
    native_refresh_rate: "144Hz",
  });
  assert.deepEqual(payload.sections.audio_json, {
    speaker_configuration: "2.0 Channel",
    audio_output: "24W",
  });
  assert.deepEqual(payload.sections.gaming_json, {
    vrr: "48-144Hz",
    allm: true,
  });
});
