const { existsSync, mkdirSync } = require("node:fs");

const mkdirIfNotExists = (dirPath) => {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath);
  }
};

module.exports = {
  mkdirIfNotExists,
};
