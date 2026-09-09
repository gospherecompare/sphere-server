const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildSmartphoneNotificationEvents,
  dedupeNotificationEvents,
  resolveFollowPreferencesForEvent,
} = require("../services/notifications/notificationEngine");

test("launch and sale alerts are generated on the exact lifecycle dates once", () => {
  const product = {
    id: 12,
    name: "Redmi 17 5G",
    launch_date: "2026-09-03",
    sale_start_date: "2026-09-10",
    current_price: 39999,
  };

  const events = buildSmartphoneNotificationEvents({
    product,
    today: "2026-09-10",
  });

  assert.deepEqual(
    events.map((event) => event.type),
    ["launch_alert", "sale_started"],
  );

  const deduped = dedupeNotificationEvents(events);
  assert.equal(deduped.length, 2);
  assert.deepEqual(
    deduped.map((event) => event.occurrence_key),
    [
      "smartphone:12:launch_alert:2026-09-03",
      "smartphone:12:sale_started:2026-09-10",
    ],
  );
});

test("price drops create a single price alert only when the price actually falls", () => {
  const events = buildSmartphoneNotificationEvents({
    product: {
      id: 99,
      name: "vivo T5",
      current_price: 36999,
      previous_price: 38999,
    },
    today: "2026-09-09",
  });

  assert.equal(events.length, 1);
  assert.equal(events[0].type, "price_dropped");
  assert.equal(events[0].meta.price_delta, 2000);
});

test("user preferences filter a notification event to the relevant subscription set", () => {
  const users = [
    { id: 1, preferences: { launch_alerts: true, sale_alerts: true, price_drop_alerts: true, deal_alerts: false } },
    { id: 2, preferences: { launch_alerts: true, sale_alerts: false, price_drop_alerts: false, deal_alerts: true } },
    { id: 3, preferences: { launch_alerts: false, sale_alerts: true, price_drop_alerts: true, deal_alerts: true } },
  ];

  const matching = resolveFollowPreferencesForEvent({
    eventType: "sale_started",
    users,
  });

  assert.deepEqual(matching.map((user) => user.id), [1, 3]);
});
