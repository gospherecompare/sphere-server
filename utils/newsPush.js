const { getFirebaseAdmin, isFirebaseAdminConfigured } = require("./firebaseAdmin");

const DEFAULT_SITE_ORIGIN =
  String(process.env.PUBLIC_SITE_ORIGIN || "https://tryhook.shop").trim() ||
  "https://tryhook.shop";
const NEWS_PUSH_TOPIC = "news-all";
const DEFAULT_NOTIFICATION_ICON = `${DEFAULT_SITE_ORIGIN}/hook-logo.png`;

const cleanText = (value = "") =>
  String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const clipText = (value = "", maxLength = 140) => {
  const text = cleanText(value);
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
};

const createNewsPushUrl = (slug = "") => {
  const normalizedSlug = String(slug || "").trim();
  if (!normalizedSlug) return `${DEFAULT_SITE_ORIGIN}/news`;
  return `${DEFAULT_SITE_ORIGIN}/news/${encodeURIComponent(normalizedSlug)}`;
};

const getAdminMessaging = () => {
  const admin = getFirebaseAdmin();
  if (!admin) return null;
  return admin.messaging();
};

const subscribeTokenToTopic = async (token, topic = NEWS_PUSH_TOPIC) => {
  const messaging = getAdminMessaging();
  if (!messaging) {
    throw new Error("Firebase Admin is not configured");
  }
  return messaging.subscribeToTopic([token], topic);
};

const unsubscribeTokenFromTopic = async (token, topic = NEWS_PUSH_TOPIC) => {
  const messaging = getAdminMessaging();
  if (!messaging) {
    throw new Error("Firebase Admin is not configured");
  }
  return messaging.unsubscribeFromTopic([token], topic);
};

const sendPublishedNewsPush = async (blog = {}) => {
  const messaging = getAdminMessaging();
  if (!messaging) {
    return {
      ok: false,
      skipped: true,
      reason: "firebase-admin-not-configured",
    };
  }

  const slug = String(blog.slug || "").trim();
  const title = cleanText(blog.title || "Hooks News");
  const body = clipText(
    blog.excerpt ||
      blog.meta_description ||
      "Fresh mobile news and launch coverage from the Hooks newsroom.",
    140,
  );
  const url = createNewsPushUrl(slug);
  const image = String(blog.hero_image || blog.heroImage || "").trim();

  const message = {
    topic: NEWS_PUSH_TOPIC,
    notification: {
      title,
      body,
    },
    data: {
      kind: "news",
      slug,
      url,
    },
    webpush: {
      fcmOptions: {
        link: url,
      },
      notification: {
        title,
        body,
        icon: DEFAULT_NOTIFICATION_ICON,
        badge: DEFAULT_NOTIFICATION_ICON,
        image: image || undefined,
        tag: slug ? `news-${slug}` : "news-latest",
        renotify: true,
        data: {
          url,
        },
      },
    },
  };

  const messageId = await messaging.send(message);
  return {
    ok: true,
    topic: NEWS_PUSH_TOPIC,
    messageId,
    url,
  };
};

module.exports = {
  NEWS_PUSH_TOPIC,
  createNewsPushUrl,
  isFirebaseAdminConfigured,
  sendPublishedNewsPush,
  subscribeTokenToTopic,
  unsubscribeTokenFromTopic,
};
