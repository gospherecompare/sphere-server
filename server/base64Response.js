const { encodeBase64Json } = require("./responseEncoding");

const createBase64JsonMiddleware = ({
  enabled = false,
  shouldEncode = () => false,
} = {}) =>
  (req, res, next) => {
    if (!enabled || !shouldEncode(req)) return next();

    const originalJson = res.json.bind(res);
    res.json = function base64Json(payload) {
      return originalJson(encodeBase64Json(payload));
    };
    next();
  };

module.exports = {
  createBase64JsonMiddleware,
};
