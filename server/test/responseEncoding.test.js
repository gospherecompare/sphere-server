const test = require("node:test");
const assert = require("node:assert/strict");

const { BASE64_VERSION, encodeBase64Json } = require("../responseEncoding");

test("encodes JSON payloads as a versioned UTF-8 Base64 envelope", () => {
  const payload = { name: "Redmi 17 5G", offer: "₹23,999" };
  const encoded = encodeBase64Json(payload);

  assert.equal(encoded.v, BASE64_VERSION);
  assert.deepEqual(JSON.parse(Buffer.from(encoded.d, "base64").toString("utf8")), payload);
});
