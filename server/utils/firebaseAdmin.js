const fs = require("node:fs");
const path = require("node:path");
const admin = require("firebase-admin");

let cachedInitError = null;

const getServiceAccountPath = () =>
  String(process.env.FIREBASE_SERVICE_ACCOUNT_PATH || "").trim();

const readServiceAccount = () => {
  const serviceAccountPath = getServiceAccountPath();
  if (!serviceAccountPath) return null;

  const resolvedPath = path.resolve(serviceAccountPath);
  const raw = fs.readFileSync(resolvedPath, "utf8").trim();
  if (!raw) {
    throw new Error("Firebase service account file is empty");
  }

  return {
    resolvedPath,
    serviceAccount: JSON.parse(raw),
  };
};

const getFirebaseAdmin = () => {
  if (admin.apps.length) return admin;
  if (cachedInitError) throw cachedInitError;

  const serviceAccountRecord = readServiceAccount();
  if (!serviceAccountRecord) {
    return null;
  }

  try {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccountRecord.serviceAccount),
    });

    return admin;
  } catch (err) {
    cachedInitError = err;
    throw err;
  }
};

const isFirebaseAdminConfigured = () => {
  try {
    const serviceAccountRecord = readServiceAccount();
    if (!serviceAccountRecord) return false;

    const serviceAccount = serviceAccountRecord.serviceAccount || {};
    return Boolean(
      String(serviceAccount.project_id || "").trim() &&
        String(serviceAccount.client_email || "").trim() &&
        String(serviceAccount.private_key || "").trim(),
    );
  } catch (_err) {
    return false;
  }
};

module.exports = {
  getFirebaseAdmin,
  isFirebaseAdminConfigured,
};
