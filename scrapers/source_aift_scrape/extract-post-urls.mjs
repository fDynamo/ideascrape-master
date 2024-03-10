import { join } from "path";
import { readdirSync, writeFileSync } from "fs";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { readCsvFile } from "../../custom_helpers_js/read-csv.js";

const main = async () => {
  const VALID_EXTRACT_TYPES = ["all", "latest"];

  // Process input arguments
  const argv = yargs(hideBin(process.argv)).argv;
  let { outFile, extractType, listsFolder } = argv;
  if (!outFile || !listsFolder) {
    console.log("Invalid arguments");
    return;
  }
  if (!extractType) extractType = VALID_EXTRACT_TYPES[0];

  // Folder variables
  const LISTS_FOLDER = listsFolder;
  const OUT_FILE = outFile;

  // Get urls list
  const files = readdirSync(LISTS_FOLDER);
  let urlsList = [];
  const filesRead = [];

  // All
  if (extractType == VALID_EXTRACT_TYPES[0]) {
    // Get all files
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      if (!file.endsWith("-data.csv")) continue;
      filesRead.push(file);
    }
  }

  // Latest
  if (extractType == VALID_EXTRACT_TYPES[1]) {
    // Find oldest file
    let oldestFilename = "";
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      if (!file.endsWith("-data.csv")) continue;

      if (!oldestFilename) oldestFilename = file;
      else {
        if (file > oldestFilename) oldestFilename = file;
      }
    }
    filesRead.push(oldestFilename);
  }

  for (let i = 0; i < filesRead.length; i++) {
    const file = filesRead[i];
    const filepath = join(LISTS_FOLDER, file);
    const fileRows = await readCsvFile(filepath);
    fileRows.forEach((row) => {
      const url = row.source_url;
      urlsList.push(url);
    });
  }

  urlsList = [...new Set(urlsList)];

  const toWrite = {
    count: urlsList.length,
    files: filesRead,
    urls: urlsList,
  };
  writeFileSync(OUT_FILE, JSON.stringify(toWrite), { encoding: "utf-8" });
};
main();
