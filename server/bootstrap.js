"use strict";

const path = require("path");
const Module = require("module");
const dotenv = require("dotenv");

const serverDir = __dirname;
const projectRoot = path.resolve(serverDir, "..");
const serverNodeModules = path.join(serverDir, "node_modules");
const envPath = path.join(serverDir, ".env");

dotenv.config({ path: envPath });

const existingNodePath = (process.env.NODE_PATH || "")
  .split(path.delimiter)
  .filter(Boolean);

if (!existingNodePath.includes(serverNodeModules)) {
  process.env.NODE_PATH = [serverNodeModules, ...existingNodePath].join(
    path.delimiter,
  );
  Module._initPaths();
}

module.exports = {
  envPath,
  projectRoot,
  serverDir,
  serverNodeModules,
};
