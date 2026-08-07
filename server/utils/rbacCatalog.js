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
  ) {
    return "ceo";
  }
  if (
    [
      "admin",
      "administrator",
      "superadmin",
      "super_admin",
      "superadministrator",
      "super_administrator",
    ].includes(role)
  ) {
    return "admin";
  }
  if (["contentadmin", "content_admin", "contentmanager", "content_manager"].includes(role)) {
    return "content_admin";
  }
  if (["productmanager", "product_manager"].includes(role)) return "product_manager";
  if (["reportanalyst", "report_analyst", "analyst"].includes(role)) return "analyst";
  if (["seomanager", "seo_manager", "seoeditor", "seo_editor"].includes(role)) return "seo";
  if (["moderator", "reviewer"].includes(role)) return "moderator";
  if (["support", "supportagent", "support_agent"].includes(role)) return "support_agent";
  if (["marketing", "marketingmanager", "marketing_manager"].includes(role)) {
    return "marketing_manager";
  }
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
  { key: "search", label: "Global Search", actions: ["view"] },
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
      "manage",
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
      "manage",
    ],
  },
  {
    key: "products.smartphones",
    label: "Smartphones",
    actions: ["view", "create", "edit", "delete", "publish", "schedule", "feature", "pin", "export", "import", "manage"],
  },
  {
    key: "products.laptops",
    label: "Laptops",
    actions: ["view", "create", "edit", "delete", "publish", "schedule", "feature", "pin", "export", "import", "manage"],
  },
  {
    key: "products.tvs",
    label: "TVs & Home Appliances",
    actions: ["view", "create", "edit", "delete", "publish", "schedule", "feature", "pin", "export", "import", "manage"],
  },
  {
    key: "specifications",
    label: "Specifications",
    actions: ["view", "create", "edit", "delete", "manage", "import", "export"],
  },
  {
    key: "specifications.categories",
    label: "Categories",
    actions: ["view", "create", "edit", "delete", "manage", "import", "export"],
  },
  {
    key: "specifications.brands",
    label: "Brands",
    actions: ["view", "create", "edit", "delete", "manage", "import", "export"],
  },
  {
    key: "specifications.stores",
    label: "Stores",
    actions: ["view", "create", "edit", "delete", "manage", "import", "export"],
  },
  {
    key: "specifications.memory_storage",
    label: "Memory & Storage",
    actions: ["view", "create", "edit", "delete", "manage", "import", "export"],
  },
  { key: "reports", label: "Reports", actions: ["view", "export", "manage"] },
  { key: "reports.product_categories", label: "Product Category Report", actions: ["view", "export"] },
  { key: "reports.product_publish_status", label: "Product Publish Status", actions: ["view", "export"] },
  { key: "reports.launch_timing", label: "Launch Timing Report", actions: ["view", "export"] },
  { key: "reports.user_activity", label: "Published by User Report", actions: ["view", "export"] },
  { key: "reports.recent_activity", label: "Recent Publish Activity", actions: ["view", "export"] },
  { key: "reports.trending", label: "Trending Manager", actions: ["view", "edit", "manage", "export"] },
  { key: "reports.hook_score", label: "Hook Score Report", actions: ["view", "export"] },
  { key: "reports.feature_clicks", label: "Feature Clicks Report", actions: ["view", "export"] },
  { key: "reports.search_popularity", label: "Search Popularity Report", actions: ["view", "export"] },
  { key: "reports.career_applications", label: "Career Applications", actions: ["view", "edit", "export"] },
  { key: "reports.contact_submissions", label: "Contact Inbox", actions: ["view", "edit", "export"] },
  { key: "users", label: "Users", actions: ["view", "create", "edit", "delete", "assign", "manage"] },
  { key: "roles", label: "Roles", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "permissions", label: "Permissions", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "activity", label: "Recent Activity", actions: ["view", "export"] },
  { key: "settings", label: "Settings", actions: ["view", "edit", "manage"] },
  { key: "settings.compare_pages", label: "Compare Pages", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "settings.compare_scoring", label: "Compare Scoring", actions: ["view", "edit", "manage"] },
  { key: "settings.device_field_profiles", label: "Device Field Profiles", actions: ["view", "edit", "manage"] },
  { key: "settings.api_tester", label: "API Tester", actions: ["view", "manage"] },
  { key: "media", label: "Media Library", actions: ["view", "create", "edit", "delete"] },
  { key: "marketing", label: "Marketing", actions: ["view", "create", "edit", "delete", "manage", "export"] },
  { key: "marketing.banners", label: "Banners", actions: ["view", "create", "edit", "delete", "manage"] },
  { key: "marketing.affiliate_links", label: "Affiliate Links", actions: ["view", "create", "edit", "delete", "manage", "export"] },
  { key: "seo", label: "SEO", actions: ["view", "edit", "manage"] },
  { key: "customers", label: "Customers", actions: ["view", "create", "edit", "delete", "manage", "export"] },
  { key: "account", label: "Account Settings", actions: ["view", "edit"] },
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
    description: "Full access to every workspace, workflow, and access control screen.",
    permissions: ["*"],
  },
  content_admin: {
    label: "Content Admin",
    description: "Owns newsroom, media, SEO, and editorial publishing workflows.",
    permissions: [
      "dashboard.view",
      "account.*",
      "content.news.*",
      "media.*",
      "marketing.banners.view",
      "marketing.banners.edit",
      "reports.view",
      "reports.user_activity.view",
      "reports.recent_activity.view",
      "activity.view",
      "seo.*",
    ],
  },
  editor: {
    label: "Editor",
    description: "Creates, edits, schedules, and publishes editorial content.",
    permissions: [
      "dashboard.view",
      "account.*",
      "content.news.view",
      "content.news.create",
      "content.news.edit",
      "content.news.schedule",
      "content.news.publish",
      "content.news.feature",
      "content.news.pin",
      "media.view",
      "media.create",
      "reports.view",
      "reports.user_activity.view",
      "activity.view",
    ],
  },
  author: {
    label: "Author",
    description: "Drafts stories and submits content for review.",
    permissions: [
      "dashboard.view",
      "account.*",
      "content.news.view",
      "content.news.create",
      "content.news.edit",
      "media.view",
    ],
  },
  product_manager: {
    label: "Product Manager",
    description: "Manages product pages, uploads, and product-related content.",
    permissions: [
      "dashboard.view",
      "account.*",
      "products.*",
      "specifications.*",
      "media.view",
      "reports.view",
      "reports.product_categories.view",
      "reports.product_publish_status.view",
      "reports.launch_timing.view",
      "activity.view",
    ],
  },
  analyst: {
    label: "Analyst",
    description: "Views and exports reports, trends, and dashboard data.",
    permissions: ["dashboard.view", "account.*", "reports.*", "activity.view"],
  },
  seo: {
    label: "SEO Editor",
    description: "Optimizes editorial content and search performance.",
    permissions: [
      "dashboard.view",
      "account.*",
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
      "account.*",
      "content.news.view",
      "content.news.approve",
      "content.news.reject",
      "activity.view",
      "reports.view",
    ],
  },
  marketing_manager: {
    label: "Marketing Manager",
    description: "Manages banners, affiliate placements, and marketing reports.",
    permissions: [
      "dashboard.view",
      "account.*",
      "marketing.*",
      "content.news.view",
      "reports.view",
      "reports.feature_clicks.view",
      "reports.search_popularity.view",
      "activity.view",
    ],
  },
  support_agent: {
    label: "Support Agent",
    description: "Handles customer support, contact inbox, and read-only product context.",
    permissions: [
      "dashboard.view",
      "account.*",
      "customers.view",
      "customers.edit",
      "products.view",
      "products.smartphones.view",
      "products.laptops.view",
      "products.tvs.view",
      "reports.contact_submissions.view",
      "activity.view",
    ],
  },
  viewer: {
    label: "Viewer",
    description: "Read-only access to public-facing admin screens.",
    permissions: [
      "dashboard.view",
      "account.*",
      "search.view",
      "content.news.view",
      "products.view",
      "products.smartphones.view",
      "products.laptops.view",
      "products.tvs.view",
      "reports.view",
    ],
  },
};

const getRolePreset = (value = "") => {
  const key = normalizeRole(value);
  return ROLE_PRESETS[key] || ROLE_PRESETS.viewer;
};

const getDefaultPermissionsForRole = (value = "") =>
  Array.from(new Set((getRolePreset(value).permissions || []).map(normalizeToken)));

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
  return permissions.some((permission) => permissionMatches(permission, requested));
};

const expandPermissionSet = (grantedPermissions = []) => {
  const permissions = Array.isArray(grantedPermissions)
    ? grantedPermissions
    : String(grantedPermissions || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
  const normalized = Array.from(
    new Set(permissions.map((permission) => normalizeToken(permission)).filter(Boolean)),
  );
  const expanded = new Set();

  getAllPermissionCodes().forEach((permission) => {
    if (hasPermissionSet(normalized, permission)) {
      expanded.add(permission);
    }
  });

  normalized.forEach((permission) => {
    if (permission !== "*" && !permission.endsWith(".*")) {
      expanded.add(permission);
    }
  });

  return Array.from(expanded).sort();
};

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
  expandPermissionSet,
  normalizePermissionToken: normalizeToken,
};
