const path = require("node:path");
const admin = require("firebase-admin");

let cachedInitError = null;

const getServiceAccountPath = () =>
  String(process.env.FIREBASE_SERVICE_ACCOUNT_PATH || "").trim();

const getFirebaseAdmin = () => {
  if (admin.apps.length) return admin;
  if (cachedInitError) throw cachedInitError;

  const serviceAccountPath = getServiceAccountPath();
  if (!serviceAccountPath) {
    return null;
  }

  try {
    const resolvedPath = path.resolve(serviceAccountPath);
    const serviceAccount = require(resolvedPath);

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });

    return admin;
  } catch (err) {
    cachedInitError = err;
    throw err;
  }
};

const isFirebaseAdminConfigured = () => Boolean(getServiceAccountPath());

module.exports = {
  getFirebaseAdmin,
  isFirebaseAdminConfigured,
};
