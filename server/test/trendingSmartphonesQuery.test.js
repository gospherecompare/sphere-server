const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const source = fs.readFileSync(path.join(__dirname, "..", "index.js"), "utf8");

const hasSelectedField = /s\.launch_status_mode/.test(source);
const hasGroupedField = /GROUP BY[\s\S]*s\.launch_status_mode/.test(source);

assert.equal(hasSelectedField, true, "Query should select launch_status_mode");
assert.equal(
  hasGroupedField,
  true,
  "Query should include launch_status_mode in GROUP BY",
);

test("trending smartphones query includes launch_status_mode in the grouped columns", () => {
  assert.equal(/s\.launch_status_mode/.test(source), true);
  assert.equal(/GROUP BY[\s\S]*s\.launch_status_mode/.test(source), true);
});
