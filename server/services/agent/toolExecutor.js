"use strict";

const productIdsFromContext = (context) =>
  [...(context.selectedProducts || []), ...(context.comparisonProducts || [])]
    .map((product) => Number(product?.id || product?.product_id || product))
    .filter((id) => Number.isInteger(id) && id > 0);

const extractProductReferences = (message) => {
  const text = String(message || "").trim();
  const comparePart = text.match(/(?:compare|between)\s+(.+)/i)?.[1] || text;
  const pieces = comparePart
    .split(/\s+(?:and|vs\.?|versus)\s+/i)
    .map((piece) => piece.replace(/[?.!,]+$/g, "").trim())
    .filter(Boolean);
  return pieces.length >= 2 ? pieces.slice(0, 2) : [];
};

const extractSingleProductReference = (message) => {
  const match = String(message || "").match(
    /\b(?:of|about|for)\s+(.+?)(?:\?|$)/i,
  );
  return match?.[1]?.replace(/[?.!,]+$/g, "").trim() || "";
};

const createAgentToolExecutor = ({ capabilities } = {}) => {
  if (!capabilities) throw new Error("capabilities are required");

  const execute = async (interpretation) => {
    const { context, filters, intent, domain, message, tool } = interpretation;
    const limit = Math.min(
      50,
      Math.max(1, Number(context.filters?.limit || 20)),
    );

    if (
      context.currentResults.length &&
      /\b(which one|these|them|that one)\b/i.test(message)
    ) {
      return {
        source: "context.currentResults",
        items: context.currentResults,
        raw: context.currentResults,
      };
    }

    if (tool === "filterSmartphones" || tool === "filterTVs") {
      const items = await capabilities.listProducts({
        domain,
        filters,
        limit,
        recommend: false,
      });
      return { source: "catalog.listProducts", items, raw: items };
    }

    if (tool === "recommendSmartphones" || tool === "recommendTVs") {
      const list = capabilities.recommendProducts || capabilities.listProducts;
      const items = await list({
        domain,
        filters,
        preferences: interpretation.preferences,
        limit,
        ...(capabilities.recommendProducts ? {} : { recommend: true }),
      });
      return { source: "catalog.recommendProducts", items, raw: items };
    }

    if (tool === "searchSmartphones" || tool === "searchTVs") {
      const search = capabilities.searchProducts || capabilities.listProducts;
      const items = await search({
        domain,
        filters,
        query: message,
        limit,
        ...(search === capabilities.listProducts ? { recommend: false } : {}),
      });
      return { source: "catalog.searchProducts", items, raw: items };
    }

    if (tool === "getTrendingSmartphones" || tool === "getTrendingTVs") {
      const list =
        capabilities.getTrendingProducts || capabilities.listProducts;
      const items = await list({
        domain,
        filters,
        limit,
        ...(capabilities.getTrendingProducts ? {} : { recommend: true }),
      });
      return { source: "catalog.getTrendingProducts", items, raw: items };
    }

    if (tool === "getLatestSmartphones" || tool === "getLatestTVs") {
      const list = capabilities.getLatestProducts || capabilities.listProducts;
      const items = await list({
        domain,
        filters,
        limit,
        ...(capabilities.getLatestProducts ? {} : { recommend: false }),
      });
      return { source: "catalog.getLatestProducts", items, raw: items };
    }

    if (tool === "getArticle") {
      const ids = productIdsFromContext(context);
      const article = await capabilities.getArticle({
        id: context.articleId || null,
        slug: context.articleSlug || null,
        currentResults: context.currentResults,
        message,
      });
      return article
        ? { source: "catalog.getArticle", items: [article], raw: article }
        : { source: null, items: [], raw: null, needsClarification: true };
    }

    if (tool === "getSmartphoneSpecScore") {
      const ids = productIdsFromContext(context);
      const references = extractProductReferences(message);
      const singleReference = extractSingleProductReference(message);
      const names = references.length
        ? references
        : singleReference
          ? [singleReference]
          : [];
      const product = await capabilities.getSmartphoneSpecScore({
        id: ids[0],
        names,
      });
      return product
        ? {
            source: "catalog.getSmartphoneSpecScore",
            items: [product],
            raw: product,
          }
        : { source: null, items: [], raw: null, needsClarification: true };
    }

    if (
      tool === "getLatestNews" ||
      tool === "searchNews" ||
      tool === "getCategoryNews" ||
      tool === "getBrandNews" ||
      tool === "getProductNews"
    ) {
      const newsInput = {
        brand: filters.brand,
        topic: filters.topic || interpretation.entities.topic,
        date: filters.date || interpretation.entities.date,
        query: tool === "searchNews" ? message : null,
        limit,
      };
      const product = filters.product || interpretation.entities.product;
      if (product) newsInput.product = product;
      const items = await capabilities.searchNews(newsInput);
      return { source: "catalog.searchNews", items, raw: items };
    }

    if (tool === "getSmartphoneDetails" || tool === "getTVDetails") {
      const ids = productIdsFromContext(context);
      const references = extractProductReferences(message);
      const singleReference = extractSingleProductReference(message);
      const resolvedReferences = references.length
        ? references
        : singleReference
          ? [singleReference]
          : [];
      const resolved = resolvedReferences.length
        ? await capabilities.resolveProducts({
            names: resolvedReferences,
            domain,
          })
        : [];
      const product = await capabilities.getProduct(
        ids[0] || resolved[0]?.product_id,
      );
      return product
        ? { source: "catalog.getProduct", items: [product], raw: product }
        : { source: null, items: [], raw: null, needsClarification: true };
    }

    if (tool === "compareSmartphones" || tool === "compareTVs") {
      const ids = productIdsFromContext(context);
      const references = extractProductReferences(message);
      const resolved = references.length
        ? await capabilities.resolveProducts({ names: references, domain })
        : [];
      const resolvedIds = resolved.map((product) => Number(product.product_id));
      const comparison = await capabilities.compareProducts(
        [...new Set([...ids, ...resolvedIds])].slice(0, 2),
      );
      return comparison
        ? {
            source: "catalog.compareProducts",
            items: comparison.devices || [],
            raw: comparison,
            comparison,
          }
        : { source: null, items: [], raw: null, needsClarification: true };
    }

    return { source: null, items: [], raw: null, needsClarification: true };
  };

  return { execute };
};

module.exports = {
  createAgentToolExecutor,
  extractProductReferences,
  extractSingleProductReference,
  productIdsFromContext,
};
