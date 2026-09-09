"use strict";

const rupees = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? `₹${n.toLocaleString("en-IN")}` : null;
};

const pickName = (item) => item?.name || item?.product_name || item?.title || item?.model || "this result";

const compactFacts = (item = {}) => {
  const facts = [];
  const price = rupees(item.price);
  if (price) facts.push(price);
  if (item.hook_score !== undefined) facts.push(`MobilesX score ${Number(item.hook_score).toFixed(1)}`);
  if (item.trending_score !== undefined) facts.push(`trend ${Number(item.trending_score).toFixed(1)}`);
  if (item.buyer_intent !== undefined) facts.push(`buyer intent ${Number(item.buyer_intent).toFixed(1)}`);
  return facts.slice(0, 3).join(" • ");
};

const defaultGenerate = ({ interpretation, result }) => {
  const items = Array.isArray(result?.items) ? result.items : [];
  const top = items[0];
  const brand = interpretation.entities?.brand;
  const price = interpretation.filters?.maxPrice;
  const budget = price ? ` under ${rupees(price)}` : "";
  if (!items.length) return { title: "No matching results", message: "I couldn't find a matching result in the current MobilesX data." };

  if (interpretation.intent === "recommend") {
    const facts = compactFacts(top);
    return {
      title: `${brand ? `${brand} ` : ""}${interpretation.domain === "tv" ? "TV recommendations" : "phone recommendations"}`,
      message: `Based on the current MobilesX data, ${pickName(top)} is the leading option${budget}. ${facts ? `${facts}. ` : ""}I found ${items.length} matching option${items.length === 1 ? "" : "s"}.`,
    };
  }
  if (interpretation.intent === "compare") {
    const winner = result?.comparison?.overallWinner;
    const winnerItem = items.find((item) => Number(item.product_id || item.id) === Number(winner));
    return {
      title: "MobilesX comparison",
      message: winnerItem ? `${pickName(winnerItem)} leads this comparison based on the existing MobilesX comparison engine.` : "The comparison is ready using the existing MobilesX comparison engine.",
    };
  }
  if (interpretation.intent === "trending") return { title: `Trending ${interpretation.domain === "tv" ? "TVs" : "smartphones"}`, message: `These results use the current MobilesX trending signals. ${items.length} item${items.length === 1 ? " is" : "s are"} available.` };
  if (interpretation.intent === "latest" || interpretation.intent === "latest_news") return { title: `Latest ${interpretation.domain === "news" ? "news" : interpretation.domain === "tv" ? "TVs" : "smartphones"}`, message: "Here are the latest published results currently available on MobilesX." };
  if (interpretation.domain === "news") return { title: brand ? `${brand} News` : "MobilesX News", message: `I found ${items.length} matching published news result${items.length === 1 ? "" : "s"}.` };
  if (interpretation.intent === "specification") return { title: `${pickName(top)} specifications`, message: `Here is the current specification data for ${pickName(top)}.` };
  return { title: `${brand ? `${brand} ` : ""}${interpretation.domain === "tv" ? "TV" : "smartphone"} results`, message: `I found ${items.length} matching result${items.length === 1 ? "" : "s"} using current MobilesX data.` };
};

const createResponseGenerator = ({ llmClient = null } = {}) => ({
  async generate({ interpretation, result }) {
    const base = defaultGenerate({ interpretation, result });
    if (!llmClient) return base;
    const safeResult = {
      type: interpretation.responseType,
      domain: interpretation.domain,
      intent: interpretation.intent,
      title: base.title,
      products: Array.isArray(result?.items) ? result.items.slice(0, 8) : [],
      comparison: result?.comparison || null,
    };
    const content = await llmClient.generate({
      system: "You are MobilesX assistant. Use only supplied MobilesX data. Do not invent specifications, prices, dates, rankings, or news. Be concise, factual, and consumer-friendly. Do not output markdown tables. Preserve product names exactly.",
      user: JSON.stringify({ request: interpretation.message, data: safeResult }),
    });
    return content ? { ...base, message: content.trim() } : base;
  },
});

module.exports = { createResponseGenerator };
