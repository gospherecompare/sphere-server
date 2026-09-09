/**
 * Decode HTML entities to render proper HTML
 * Prevents double-encoding issues in JSON responses
 */
const decodeHtmlEntities = (text) => {
  if (!text || typeof text !== "string") return text;

  const decoder = {
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&amp;": "&", // Must be last to avoid double-decoding
  };

  let decoded = text;
  for (const [encoded, decoded_char] of Object.entries(decoder)) {
    decoded = decoded.split(encoded).join(decoded_char);
  }
  return decoded;
};

/**
 * Ensure content is not double-encoded
 * Use this before sending HTML content in JSON responses
 */
const ensureProperHtmlEncoding = (content) => {
  if (!content || typeof content !== "string") return content;

  // If content looks like it's double-encoded, decode it once
  if (content.includes("&lt;") && content.includes("&gt;")) {
    return decodeHtmlEntities(content);
  }

  return content;
};

module.exports = {
  decodeHtmlEntities,
  ensureProperHtmlEncoding,
};
