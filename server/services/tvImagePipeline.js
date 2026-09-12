"use strict";

const { OFFICIAL_DOMAINS } = require("./tvSourceVerifier");

const normalizeHost = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/^www\./, "")
    .split("/")[0];
const hostMatches = (host, domains = []) =>
  domains.some((domain) => host === domain || host.endsWith(`.${domain}`));

const resolveOfficialDomains = (brandName) => {
  const key = String(brandName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  return OFFICIAL_DOMAINS[key] || [];
};

const normalizeIdentity = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const findOfficialImageCandidate = (candidates = [], brandName, model) => {
  const domains = resolveOfficialDomains(brandName);
  const modelIdentity = normalizeIdentity(model);
  const modelCompact = modelIdentity.replace(/\s+/g, "");
  return (
    (Array.isArray(candidates) ? candidates : []).find((candidate) => {
      try {
        const imageUrl =
          candidate?.image_url || candidate?.url || candidate?.imageUri;
        const url = new URL(imageUrl);
        if (
          !["http:", "https:"].includes(url.protocol) ||
          !hostMatches(normalizeHost(url.hostname), domains)
        )
          return false;
        if (!modelIdentity) return true;
        const identityText = normalizeIdentity(
          [
            candidate?.model,
            candidate?.title,
            candidate?.name,
            candidate?.source_page_url,
            candidate?.sourceUrl,
            candidate?.page_url,
            candidate?.pageUrl,
            imageUrl,
          ]
            .filter(Boolean)
            .join(" "),
        );
        return (
          identityText.includes(modelIdentity) ||
          identityText.replace(/\s+/g, "").includes(modelCompact)
        );
      } catch {
        return false;
      }
    }) || null
  );
};

const downloadOfficialImage = async (
  candidate,
  { fetchImpl = globalThis.fetch } = {},
) => {
  if (!candidate || typeof fetchImpl !== "function")
    throw new Error("Official image candidate or fetch is unavailable");
  const sourceUrl = candidate.image_url || candidate.imageUri || candidate.url;
  const response = await fetchImpl(sourceUrl, {
    redirect: "follow",
    signal: AbortSignal.timeout(15000),
  });
  const finalHost = normalizeHost(new URL(response.url || sourceUrl).hostname);
  const allowed = resolveOfficialDomains(
    candidate.brand_name || candidate.brandName,
  );
  if (!response.ok)
    throw new Error(`Official image returned HTTP ${response.status}`);
  if (allowed.length && !hostMatches(finalHost, allowed))
    throw new Error("Official image redirected to an untrusted domain");
  const contentType = String(
    response.headers?.get?.("content-type") || "",
  ).toLowerCase();
  if (!contentType.startsWith("image/"))
    throw new Error("Official image URL did not return an image");
  return {
    buffer: Buffer.from(await response.arrayBuffer()),
    contentType,
    sourceUrl,
    finalHost,
  };
};

const verifyOfficialImageSourcePage = async (
  candidate,
  { brandName, model, fetchImpl = globalThis.fetch } = {},
) => {
  const pageUrl = candidate?.source_page_url || candidate?.sourcePageUrl;
  if (!pageUrl) throw new Error("Official image source page is required");
  const domains = resolveOfficialDomains(brandName);
  const parsed = new URL(pageUrl);
  if (!hostMatches(normalizeHost(parsed.hostname), domains))
    throw new Error("Official image source page is on an untrusted domain");
  const response = await fetchImpl(parsed.href, {
    redirect: "follow",
    signal: AbortSignal.timeout(8000),
  });
  if (!response.ok)
    throw new Error(
      `Official image source page returned HTTP ${response.status}`,
    );
  const finalHost = normalizeHost(
    new URL(response.url || parsed.href).hostname,
  );
  if (!hostMatches(finalHost, domains))
    throw new Error(
      "Official image source page redirected to an untrusted domain",
    );
  const text = normalizeIdentity(await response.text());
  const brandText = normalizeIdentity(brandName);
  const modelText = normalizeIdentity(model);
  if (
    !text.includes(brandText) ||
    !(
      text.includes(modelText) ||
      text.replace(/\s+/g, "").includes(modelText.replace(/\s+/g, ""))
    )
  ) {
    throw new Error(
      "Official image source page does not identify the requested TV model",
    );
  }
  return { pageUrl: parsed.href, finalHost };
};

const uploadToCloudinary = async (
  buffer,
  { cloudName, uploadPreset, folder, publicId, fetchImpl = globalThis.fetch },
) => {
  if (!cloudName || !uploadPreset)
    throw new Error("Cloudinary cloud name and upload preset are required");
  const form = new FormData();
  form.append("file", new Blob([buffer]), publicId || "tv-main");
  form.append("upload_preset", uploadPreset);
  if (folder) form.append("folder", folder);
  if (publicId) form.append("public_id", publicId);
  const response = await fetchImpl(
    `https://api.cloudinary.com/v1_1/${encodeURIComponent(cloudName)}/image/upload`,
    { method: "POST", body: form },
  );
  const payload = await response.json();
  if (!response.ok || !payload.secure_url)
    throw new Error(payload.error?.message || "Cloudinary upload failed");
  return {
    secure_url: payload.secure_url,
    public_id: payload.public_id,
    source: "official_brand",
  };
};

const processOfficialTvImage = async ({
  candidates,
  brandName,
  model,
  fetchImpl,
  cloudName = process.env.CLOUDINARY_CLOUD_NAME,
  uploadPreset = process.env.CLOUDINARY_PRESET_APPLIANCE ||
    process.env.CLOUDINARY_UPLOAD_PRESET,
}) => {
  const candidate = findOfficialImageCandidate(candidates, brandName, model);
  if (!candidate)
    return {
      status: "manual_review",
      images_json: [],
      reason: "No official-domain image candidate found",
    };
  if (!cloudName || !uploadPreset)
    return {
      status: "manual_review",
      images_json: [],
      reason: "Cloudinary is not configured",
    };
  await verifyOfficialImageSourcePage(candidate, {
    brandName,
    model,
    fetchImpl,
  });
  const downloaded = await downloadOfficialImage(
    { ...candidate, brand_name: brandName },
    { fetchImpl },
  );
  const uploaded = await uploadToCloudinary(downloaded.buffer, {
    cloudName,
    uploadPreset,
    fetchImpl,
    folder: `mobilesx/tvs/${String(model)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")}`,
    publicId: "main",
  });
  return {
    status: "uploaded",
    images_json: [uploaded.secure_url],
    asset: uploaded,
    source_url: downloaded.sourceUrl,
  };
};

module.exports = {
  findOfficialImageCandidate,
  verifyOfficialImageSourcePage,
  downloadOfficialImage,
  uploadToCloudinary,
  processOfficialTvImage,
};
