const dotenv = require("dotenv");
const { existsSync, readFileSync } = require("node:fs");
const { join } = require("node:path");
dotenv.config();

function accessCacheFolder(key) {
  const outRoot = process.env.IDEASCRAPE_CACHE_FOLDER;
  if (!existsSync(outRoot)) {
    throw new Error("No out folder created " + outRoot);
  }
  const jsonFile = join(outRoot, "directory-structure.json");
  const fileContents = readFileSync(jsonFile, "utf-8");
  const fileJson = JSON.parse(fileContents);
  const folderPath = join(outRoot, fileJson[key]);
  return folderPath;
}

module.exports = {
  accessCacheFolder,
};
