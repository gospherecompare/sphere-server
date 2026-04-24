const fs = require("node:fs");
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

const isFirebaseAdminConfigured = () => {
  const serviceAccountPath = getServiceAccountPath();
  if (!serviceAccountPath) return false;

  try {
    return fs.existsSync(path.resolve(serviceAccountPath));
  } catch (_err) {
    return false;
  }
};

module.exports = {
  getFirebaseAdmin,
  isFirebaseAdminConfigured,
};
