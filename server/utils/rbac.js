const normalizeToken = (value = "") =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/-/g, "_");

const normalizeRole = (value = "") => {
  const role = normalizeToken(value);
  if (
    [
      "ceo",
      "chiefexecutiveofficer",
      "chief_executive_officer",
      "chiefexecutive",
      "chief_executive",
    ].includes(role)
  )
    return "ceo";
  if (["superadmin", "super_admin"].includes(role)) return "admin";
  if (["contentmanager", "content_manager"].includes(role))
    return "content_admin";
  if (["productmanager", "product_manager"].includes(role))
    return "product_manager";
  if (["reportanalyst", "report_analyst", "analyst"].includes(role))
    return "analyst";
  if (["seomanager", "seo_manager", "seoeditor", "seo_editor"].includes(role))
    return "seo";
  if (["moderator", "reviewer"].includes(role)) return "moderator";
  if (["author", "writer"].includes(role)) return "author";
  if (["editor", "content_editor"].includes(role)) return "editor";
  if (["viewer", "user", "member"].includes(role)) return "viewer";
  return role || "viewer";
};

const RBAC_ACTIONS = [
  "view",
  "create",
  "edit",
  "delete",
  "publish",
  "schedule",
  "approve",
  "reject",
  "feature",
  "pin",
  "assign",
  "manage",
  "export",
  "import",
];

const RBAC_MODULES = [
  { key: "dashboard", label: "Dashboard", actions: ["view"] },
  {
    key: "content.news",
    label: "News & Articles",
    actions: [
      "view",
      "create",
      "edit",
      "delete",
      "publish",
      "schedule",
      "approve",
      "reject",
      "feature",
      "pin",
    ],
  },
  {
    key: "products",
    label: "Products",
    actions: [
      "view",
      "create",
      "edit",
      "delete",
      "publish",
      "schedule",
      "feature",
      "pin",
      "export",
      "import",
    ],
  },
  { key: "reports", label: "Reports", actions: ["view", "export"] },
  { key: "users", label: "Users", actions: ["view", "create", "edit", "delete", "assign", "manage"] },
  { key: "roles", label: "Roles", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "permissions", label: "Permissions", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "activity", label: "Recent Activity", actions: ["view", "export"] },
  { key: "settings", label: "Settings", actions: ["view", "edit", "manage"] },
  { key: "media", label: "Media Library", actions: ["view", "create", "edit", "delete"] },
  { key: "marketing", label: "Marketing", actions: ["view", "create", "edit", "delete"] },
  { key: "seo", label: "SEO", actions: ["view", "edit", "manage"] },
  { key: "customers", label: "Customers", actions: ["view", "create", "edit", "delete"] },
];

const buildPermissionCode = (moduleKey, action) => {
  const modulePart = normalizeToken(moduleKey);
  const actionPart = normalizeToken(action);
  if (!modulePart || !actionPart) return "";
  return `${modulePart}.${actionPart}`;
};

const getPermissionMatrix = () =>
  RBAC_MODULES.map((module) => ({
    ...module,
    permissions: module.actions.map((action) => ({
      moduleKey: module.key,
      action,
      code: buildPermissionCode(module.key, action),
    })),
  }));

const getAllPermissionCodes = () =>
  RBAC_MODULES.flatMap((module) =>
    module.actions.map((action) => buildPermissionCode(module.key, action)),
  );

const ROLE_PRESETS = {
  ceo: {
    label: "CEO",
    description:
      "Executive full-control role with complete access to users, roles, permissions, and all modules.",
    permissions: ["*"],
  },
  admin: {
    label: "Super Admin",
    description:
      "Full access to every workspace, workflow, and access control screen.",
    permissions: ["*"],
  },
  content_admin: {
    label: "Content Admin",
    description:
      "Owns newsroom, media, SEO, and editorial publishing workflows.",
    permissions: [
      "dashboard.view",
      "content.news.*",
      "media.*",
      "reports.view",
      "activity.view",
      "seo.*",
    ],
  },
  editor: {
    label: "Editor",
    description: "Creates, edits, schedules, and publishes editorial content.",
    permissions: [
      "dashboard.view",
      "content.news.view",
      "content.news.create",
      "content.news.edit",
      "content.news.schedule",
      "content.news.publish",
      "content.news.feature",
      "content.news.pin",
      "media.view",
      "reports.view",
      "activity.view",
    ],
  },
  author: {
    label: "Author",
    description: "Drafts stories and submits content for review.",
    permissions: [
      "dashboard.view",
      "content.news.view",
      "content.news.create",
      "content.news.edit",
      "media.view",
    ],
  },
  product_manager: {
    label: "Product Manager",
    description:
      "Manages product pages, uploads, and product-related content.",
    permissions: [
      "dashboard.view",
      "products.*",
      "media.view",
      "reports.view",
      "activity.view",
    ],
  },
  analyst: {
    label: "Analyst",
    description: "Views and exports reports, trends, and dashboard data.",
    permissions: ["dashboard.view", "reports.view", "reports.export", "activity.view"],
  },
  seo: {
    label: "SEO Editor",
    description: "Optimizes editorial content and search performance.",
    permissions: [
      "dashboard.view",
      "content.news.view",
      "content.news.edit",
      "seo.*",
      "reports.view",
      "activity.view",
    ],
  },
  moderator: {
    label: "Moderator",
    description: "Reviews and approves content before it goes live.",
    permissions: [
      "dashboard.view",
      "content.news.view",
      "content.news.approve",
      "content.news.reject",
      "activity.view",
      "reports.view",
    ],
  },
  viewer: {
    label: "Viewer",
    description: "Read-only access to public-facing admin screens.",
    permissions: ["dashboard.view", "content.news.view", "products.view", "reports.view"],
  },
};

const getRolePreset = (value = "") => {
  const key = normalizeRole(value);
  return ROLE_PRESETS[key] || ROLE_PRESETS.viewer;
};

const getDefaultPermissionsForRole = (value = "") =>
  Array.from(
    new Set((getRolePreset(value).permissions || []).map(normalizeToken)),
  );

const permissionMatches = (granted = "", requested = "") => {
  const grant = normalizeToken(granted);
  const need = normalizeToken(requested);
  if (!grant || !need) return false;
  if (grant === "*" || grant === need) return true;
  if (grant.endsWith(".*")) {
    const prefix = grant.slice(0, -2);
    return need === prefix || need.startsWith(`${prefix}.`);
  }
  return false;
};

const hasPermissionSet = (grantedPermissions = [], requested = "") => {
  const permissions = Array.isArray(grantedPermissions)
    ? grantedPermissions
    : String(grantedPermissions || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
  return permissions.some((permission) =>
    permissionMatches(permission, requested),
  );
};

const hasAnyPermissionSet = (
  grantedPermissions = [],
  requestedPermissions = [],
) => {
  const requests = Array.isArray(requestedPermissions)
    ? requestedPermissions
    : [requestedPermissions];
  if (!requests.length) return true;
  return requests.some((permission) => hasPermissionSet(grantedPermissions, permission));
};

const hasAllPermissionsSet = (
  grantedPermissions = [],
  requestedPermissions = [],
) => {
  const requests = Array.isArray(requestedPermissions)
    ? requestedPermissions
    : [requestedPermissions];
  if (!requests.length) return true;
  return requests.every((permission) => hasPermissionSet(grantedPermissions, permission));
};

const getModulePermissionCode = (moduleKey, action = "view") =>
  buildPermissionCode(moduleKey, action);

const getModuleLabel = (moduleKey = "") => {
  const normalized = normalizeToken(moduleKey);
  const module = RBAC_MODULES.find(
    (entry) => normalizeToken(entry.key) === normalized,
  );
  return module?.label || moduleKey;
};

const getModuleActions = (moduleKey = "") => {
  const normalized = normalizeToken(moduleKey);
  const module = RBAC_MODULES.find(
    (entry) => normalizeToken(entry.key) === normalized,
  );
  return module?.actions || ["view"];
};

const isActionSupported = (moduleKey = "", action = "view") =>
  getModuleActions(moduleKey).includes(normalizeToken(action));

module.exports = {
  normalizeRole,
  RBAC_ACTIONS,
  RBAC_MODULES,
  ROLE_PRESETS,
  buildPermissionCode,
  getPermissionMatrix,
  getAllPermissionCodes,
  getRolePreset,
  getDefaultPermissionsForRole,
  permissionMatches,
  hasPermissionSet,
  hasAnyPermissionSet,
  hasAllPermissionsSet,
  getModulePermissionCode,
  getModuleLabel,
  getModuleActions,
  isActionSupported,
  normalizePermissionToken: normalizeToken,
};
