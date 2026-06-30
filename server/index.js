"use strict";

const path = require("path");
const { projectRoot } = require("./bootstrap");

process.chdir(projectRoot);

module.exports = require(path.join(projectRoot, "index.js"));
