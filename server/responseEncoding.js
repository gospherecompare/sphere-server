const BASE64_VERSION = 1;

const encodeBase64Json = (payload) => ({
  v: BASE64_VERSION,
  d: Buffer.from(JSON.stringify(payload), "utf8").toString("base64"),
});

module.exports = {
  BASE64_VERSION,
  encodeBase64Json,
};
