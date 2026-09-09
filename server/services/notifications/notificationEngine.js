const normalizeDateOnlyInput = (value) => {
  if (!value) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
    const asDate = new Date(trimmed);
    if (!Number.isNaN(asDate.getTime())) {
      return asDate.toISOString().slice(0, 10);
    }
    return null;
  }

  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }

  return null;
};

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const dedupeNotificationEvents = (events = []) => {
  const seen = new Set();
  return (Array.isArray(events) ? events : []).filter((event) => {
    const key = event?.occurrence_key || event?.type;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const buildSmartphoneNotificationEvents = ({
  product,
  today,
  previousPrice,
  currentPrice,
} = {}) => {
  const productRecord = product || {};
  const normalizedToday = normalizeDateOnlyInput(today || new Date());
  const launchDate = normalizeDateOnlyInput(
    productRecord.launch_date || productRecord.launchDate,
  );
  const saleDate = normalizeDateOnlyInput(
    productRecord.sale_start_date || productRecord.saleStartDate,
  );
  const actualPreviousPrice = toNumber(
    previousPrice ?? productRecord.previous_price ?? productRecord.previousPrice,
  );
  const actualCurrentPrice = toNumber(
    currentPrice ?? productRecord.current_price ?? productRecord.currentPrice,
  );

  const events = [];

  if (launchDate && normalizedToday && launchDate <= normalizedToday) {
    events.push({
      type: "launch_alert",
      product_id: productRecord.id || null,
      product_name: productRecord.name || null,
      occurrence_key: `smartphone:${productRecord.id || "unknown"}:launch_alert:${launchDate}`,
      title: `${productRecord.name || "Smartphone"} has launched in India`,
      message: `${productRecord.name || "Smartphone"} has launched in India`,
      meta: {
        launch_date: launchDate,
      },
    });
  }

  if (saleDate && normalizedToday && saleDate <= normalizedToday) {
    events.push({
      type: "sale_started",
      product_id: productRecord.id || null,
      product_name: productRecord.name || null,
      occurrence_key: `smartphone:${productRecord.id || "unknown"}:sale_started:${saleDate}`,
      title: `${productRecord.name || "Smartphone"} is now available to buy`,
      message: `${productRecord.name || "Smartphone"} is now available to buy`,
      meta: {
        sale_start_date: saleDate,
      },
    });
  }

  if (
    actualCurrentPrice !== null &&
    actualPreviousPrice !== null &&
    actualCurrentPrice < actualPreviousPrice
  ) {
    const delta = Math.round(actualPreviousPrice - actualCurrentPrice);
    events.push({
      type: "price_dropped",
      product_id: productRecord.id || null,
      product_name: productRecord.name || null,
      occurrence_key: `smartphone:${productRecord.id || "unknown"}:price_dropped:${Math.round(actualCurrentPrice)}`,
      title: `${productRecord.name || "Smartphone"} price dropped by ₹${delta.toLocaleString("en-IN")}`,
      message: `${productRecord.name || "Smartphone"} price dropped by ₹${delta.toLocaleString("en-IN")}`,
      meta: {
        previous_price: actualPreviousPrice,
        current_price: actualCurrentPrice,
        price_delta: delta,
      },
    });
  }

  return dedupeNotificationEvents(events);
};

const getEventPreferenceKey = (eventType) => {
  const prefMap = {
    launch_alert: "launch_alerts",
    launch_alerts: "launch_alerts",
    sale_started: "sale_alerts",
    sale_alert: "sale_alerts",
    sale_alerts: "sale_alerts",
    price_dropped: "price_drop_alerts",
    price_drop: "price_drop_alerts",
    price_drop_alerts: "price_drop_alerts",
    deal_alert: "deal_alerts",
    deal_alerts: "deal_alerts",
  };

  if (!eventType) return null;
  const normalized = String(eventType).trim();
  return prefMap[normalized] || prefMap[normalized.toLowerCase()] || null;
};

const resolveFollowPreferencesForEvent = ({ eventType, users = [] }) => {
  const preferenceKey = getEventPreferenceKey(eventType);

  if (!preferenceKey) {
    return (Array.isArray(users) ? users : []).filter((user) => user?.preferences);
  }

  return (Array.isArray(users) ? users : []).filter((user) => {
    const prefs = user?.preferences || {};
    return Boolean(prefs[preferenceKey]);
  });
};

const recordSmartphoneLifecycleEvents = async ({ db, product, today }) => {
  if (!db || !product || !product.id) return [];

  const events = buildSmartphoneNotificationEvents({
    product,
    today,
  });

  if (!events.length) return [];

  const recorded = [];

  for (const event of events) {
    const result = await db.query(
      `
        INSERT INTO notification_events (
          product_id,
          event_type,
          occurrence_key,
          title,
          message,
          payload,
          event_date
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (occurrence_key) DO NOTHING
        RETURNING id, product_id, event_type, occurrence_key, title, message, payload, created_at
      `,
      [
        event.product_id,
        event.type,
        event.occurrence_key,
        event.title,
        event.message,
        JSON.stringify(event.meta || {}),
        normalizeDateOnlyInput(today || new Date()),
      ],
    );

    if (!result.rowCount) continue;
    recorded.push({ ...result.rows[0], ...event });

    const userRows = await db.query(
      `
        SELECT pf.user_id, np.*
        FROM product_follows pf
        INNER JOIN notification_preferences np ON np.user_id = pf.user_id
        WHERE pf.product_id = $1
          AND pf.is_active = true
          AND np.user_id IS NOT NULL
      `,
      [event.product_id],
    );

    for (const row of userRows.rows) {
      const prefKey = getEventPreferenceKey(event.type);
      if (!prefKey || !row[prefKey]) continue;

      await db.query(
        `
          INSERT INTO notification_deliveries (
            event_id,
            user_id,
            channel,
            status,
            payload
          )
          VALUES ($1, $2, 'push', 'queued', $3)
          ON CONFLICT (event_id, user_id, channel)
          DO NOTHING
        `,
        [result.rows[0].id, row.user_id, JSON.stringify({ title: event.title, message: event.message })],
      );
    }
  }

  return recorded;
};

module.exports = {
  normalizeDateOnlyInput,
  buildSmartphoneNotificationEvents,
  dedupeNotificationEvents,
  resolveFollowPreferencesForEvent,
  getEventPreferenceKey,
  recordSmartphoneLifecycleEvents,
};
