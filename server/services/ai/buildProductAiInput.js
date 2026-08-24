const safe = (value) => (value === undefined || value === null ? null : value);

const cleanObject = (value) => {
  if (Array.isArray(value)) {
    return value
      .map(cleanObject)
      .filter((item) => item !== null && item !== undefined && item !== "");
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .map(([key, item]) => [key, cleanObject(item)])
        .filter(
          ([, item]) => item !== null && item !== undefined && item !== "",
        ),
    );
  }

  return safe(value);
};

const selectFields = (source, fields) => {
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    return source;
  }

  return cleanObject(
    Object.fromEntries(
      fields
        .filter((field) => Object.prototype.hasOwnProperty.call(source, field))
        .map((field) => [field, source[field]]),
    ),
  );
};

const selectNestedFields = (source, fields) => {
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    return source;
  }

  return selectFields(source, fields);
};

const selectProductSpecifications = (specs) => ({
  build: selectFields(specs.build_design || specs.design, [
    "material",
    "frame",
    "back",
    "dimensions",
    "height",
    "width",
    "thickness",
    "weight",
    "colors",
    "water_dust_resistance",
    "ip_rating",
  ]),
  display: selectFields(specs.display, [
    "size",
    "resolution",
    "panel",
    "type",
    "refresh_rate",
    "touch_sampling_rate",
    "brightness",
    "pixel_density",
    "color_gamut",
    "protection",
  ]),
  performance: selectFields(specs.performance, [
    "processor",
    "chipset",
    "cpu",
    "gpu",
    "ram",
    "ram_options",
    "storage",
    "storage_options",
    "operating_system",
    "os",
    "os_version",
  ]),
  camera: cleanObject({
    rear_camera: selectNestedFields(
      specs.camera?.rear_camera || specs.camera?.rear || specs.camera?.main,
      [
        "main_camera",
        "main_camera_megapixels",
        "resolution",
        "resolution_mp",
        "megapixels",
        "mp",
        "wide",
        "ultra_wide",
        "ultrawide",
        "ultrawide_camera_megapixels",
        "telephoto",
        "telephoto_camera_megapixels",
        "periscope",
        "ois",
        "aperture",
        "video",
      ],
    ),
    front_camera: selectNestedFields(
      specs.camera?.front_camera || specs.camera?.front,
      ["resolution", "resolution_mp", "megapixels", "mp", "aperture", "video"],
    ),
    main_camera: specs.camera?.main_camera,
    main_camera_megapixels: specs.camera?.main_camera_megapixels,
    telephoto_camera_megapixels: specs.camera?.telephoto_camera_megapixels,
    ultrawide_camera_megapixels: specs.camera?.ultrawide_camera_megapixels,
    shooting_modes: specs.camera?.shooting_modes,
  }),
  battery: selectFields(specs.battery, [
    "capacity",
    "battery_capacity",
    "battery_capacity_mah",
    "rated_capacity",
    "type",
    "charging",
    "fast_charging",
    "wireless_charging",
  ]),
  connectivity: selectFields(specs.connectivity, [
    "wifi",
    "bluetooth",
    "usb",
    "audio",
    "nfc",
    "sim_type",
    "sim_slots",
    "esim_support",
    "dual_standby",
  ]),
  network: selectFields(specs.network, [
    "sim",
    "5g_bands",
    "network_types",
    "5g_support",
    "network_bands",
  ]),
  ports: selectFields(specs.ports, [
    "usb_type",
    "headphone_jack",
    "charging_port",
  ]),
  audio: selectFields(specs.audio, [
    "speakers",
    "audio_jack",
    "microphone",
    "speaker_type",
    "speaker_count",
    "max_volume",
    "microphone_count",
    "microphone_features",
  ]),
  sensors: Array.isArray(specs.sensors)
    ? specs.sensors
    : selectFields(specs.sensors, ["sensors"]),
});

const buildProductAiInput = ({
  product,
  smartphone,
  variants = [],
  prices = [],
}) => {
  const specs = smartphone || {};

  return cleanObject({
    product: {
      id: product?.id,
      name: product?.name,
      brand: product?.brand_name || specs?.brand,
      model: specs?.model,
      category: product?.product_type || "smartphone",
      launch_date: specs?.launch_date,
      launch_status: specs?.launch_status_override,
    },

    pricing: {
      expected_price: specs?.expected_price,
      lowest_price: prices?.length
        ? Math.min(
            ...prices.map((item) => Number(item.price)).filter(Number.isFinite),
          )
        : null,
    },

    specifications: selectProductSpecifications(specs),

    variants: variants.map((variant) => ({
      id: variant.id,
      variant_key: variant.variant_key,
      storage: variant.storage,
      ram: variant.ram,
      color: variant.color,
      price: variant.base_price,
    })),
  });
};

module.exports = {
  buildProductAiInput,
};
