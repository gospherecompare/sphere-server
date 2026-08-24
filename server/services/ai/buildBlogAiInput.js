const stripHtml = (value = "") =>
  String(value || "")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const buildBlogAiInput = (blog) => {
  const articleText = stripHtml(blog.content_rendered || "");

  return {
    title: String(blog.title || "").trim(),
    excerpt: String(blog.excerpt || "").trim(),
    category: String(blog.category || "").trim(),
    brand: String(blog.brand_name || "").trim(),
    product: String(blog.product_name || "").trim(),
    productType: String(blog.product_type || "").trim(),
    tags: Array.isArray(blog.tags) ? blog.tags : [],
    article: articleText,
  };
};

module.exports = {
  buildBlogAiInput,
};
