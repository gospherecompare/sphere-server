const test = require("node:test");
const assert = require("node:assert/strict");

const {
  applyPhoneFilters,
  buildFinderPreferences,
  calculateMatchScore,
  rankPhones,
  buildRecommendationReasons,
} = require("../services/phoneFinder");

test("applyPhoneFilters removes phones failing hard requirements", () => {
  const phones = [
    {
      id: 1,
      name: "Phone A",
      price: 25000,
      numericPrice: 25000,
      batteryMah: 7000,
      ramOptions: [8, 12],
      network5G: true,
    },
    {
      id: 2,
      name: "Phone B",
      price: 35000,
      numericPrice: 35000,
      batteryMah: 6000,
      ramOptions: [8],
      network5G: true,
    },
    {
      id: 3,
      name: "Phone C",
      price: 28000,
      numericPrice: 28000,
      batteryMah: 5000,
      ramOptions: [8],
      network5G: true,
    },
  ];

  const filtered = applyPhoneFilters(phones, {
    maxPrice: 30000,
    minRam: 8,
    minBattery: 6000,
    fiveG: true,
  });

  assert.deepEqual(
    filtered.map((phone) => phone.id),
    [1],
  );
});

test("applyPhoneFilters respects a minimum budget", () => {
  const phones = [
    { id: "under", numericPrice: 14999 },
    { id: "in-range", numericPrice: 20000 },
  ];

  const filtered = applyPhoneFilters(phones, {
    minPrice: 15000,
    maxPrice: 25000,
  });

  assert.deepEqual(
    filtered.map((phone) => phone.id),
    ["in-range"],
  );
});

test("buildFinderPreferences translates primary use and priorities into weights", () => {
  const preferences = buildFinderPreferences({
    primaryUse: "gaming",
    priorities: ["performance", "battery", "display"],
  });

  assert.ok(preferences.performance > preferences.camera);
  assert.ok(preferences.battery > preferences.camera);
  assert.ok(
    Math.abs(
      Object.values(preferences).reduce((sum, value) => sum + value, 0) - 100,
    ) < 0.1,
  );
});

test("calculateMatchScore weights user priorities deterministically", () => {
  const phone = {
    performanceScore: 94,
    cameraScore: 82,
    batteryScore: 90,
    displayScore: 88,
    softwareScore: 85,
    valueScore: 0,
  };

  const score = calculateMatchScore(phone, {
    performance: 40,
    camera: 10,
    battery: 30,
    display: 10,
    software: 10,
    value: 0,
  });

  assert.ok(Math.abs(score - 90.1) < 0.05);
});

test("rankPhones sorts by match score descending", () => {
  const phones = [
    { id: "low", matchScore: 80 },
    { id: "high", matchScore: 95 },
    { id: "mid", matchScore: 88 },
  ];

  const ranked = rankPhones(phones);
  assert.deepEqual(
    ranked.map((phone) => phone.id),
    ["high", "mid", "low"],
  );
});

test("buildRecommendationReasons creates user-facing reasoning", () => {
  const reasons = buildRecommendationReasons({
    price: 25000,
    numericPrice: 25000,
    batteryMah: 7000,
    ramOptions: [8],
    network5G: true,
    performanceScore: 94,
  });

  assert.ok(reasons.includes("Within your budget"));
  assert.ok(reasons.includes("8GB+ RAM"));
  assert.ok(reasons.includes("6000mAh+ battery"));
  assert.ok(reasons.includes("5G available"));
});
