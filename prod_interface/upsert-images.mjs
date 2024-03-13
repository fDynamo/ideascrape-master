import { readFileSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { getPercentageString } from "../custom_helpers_js/string-formatters.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { timeoutPromise } from "../custom_helpers_js/index.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import registerGracefulExit from "../custom_helpers_js/graceful-exit.js";

async function main() {
  dotenv.config();

  let RUN_DELAY = 500;
  const argv = yargs(hideBin(process.argv)).argv;
  const {
    imagesFolderPath,
    prod,
    recordsFolderPath,
    startIndex: inStartIndex,
  } = argv;
  if (!imagesFolderPath) {
    console.error("Invalid inputs");
    process.exit(1);
  }

  let supabase = createSupabaseClient(prod);

  const imagesFolder = imagesFolderPath;
  const recordFile = join(imagesFolderPath, "_record.csv"); // Get filename from RECORD_FILE constant
  const recordsList = await readCsvFile(recordFile);

  const startIndex = inStartIndex ? parseInt(inStartIndex) : 0;

  const errorRecordsList = [];
  let countSuccess = 0;
  let countError = 0;
  let lastIndex = 0;

  let forcedToQuit = false;
  registerGracefulExit(() => {
    forcedToQuit = true;
  });

  for (let i = startIndex; i < recordsList.length; i++) {
    if (forcedToQuit) {
      break;
    }
    lastIndex = i;
    const file = recordsList[i].image_filename;
    if (!file || file.endsWith("svg") || file.endsWith("csv") || file == "ER") {
      console.log("skipping", i, file);
      continue;
    }

    const srcFile = join(imagesFolder, file);
    console.log("uploading", i, file, srcFile);

    const fileBody = readFileSync(srcFile);
    const extensionIndex = srcFile.lastIndexOf(".");
    const extension =
      extensionIndex > -1
        ? srcFile.substring(extensionIndex + 1).toLowerCase()
        : "";

    console.log(extension);
    let contentType = "image/png";
    if (extension == "csv") {
      continue;
    }
    if (extension == "svg") {
      contentType = "image/svg+xml";
    } else if (extension == "gif") {
      contentType = "image/gif";
    } else if (extension == "webp") {
      contentType = "image/webp";
    }

    const PRODUCT_IMAGES_BUCKET = "product_images";

    try {
      const { data, error } = await supabase.storage
        .from(PRODUCT_IMAGES_BUCKET)
        .upload(file, fileBody, {
          contentType,
          upsert: true,
        });
      if (error) throw error;

      const percentage = getPercentageString(i + 1, 0, recordsList.length);
      console.log("done", i, percentage);
      console.log("done file", file);
      countSuccess += 1;
    } catch (error) {
      console.error("Error", i, file);
      console.error(error);
      countError += 1;
      errorRecordsList.push({
        file,
        index: i,
        error,
      });
    }

    await timeoutPromise(RUN_DELAY);
  }

  if (recordsFolderPath) {
    let errorFile = join(recordsFolderPath, "upsert_images_errors");
    let uploadRecordsFile = join(recordsFolderPath, "upsert_images_records");

    if (prod) {
      errorFile += "_prod.json";
      uploadRecordsFile += "_prod.json";
    } else {
      errorFile += "_local.json";
      uploadRecordsFile += "_local.json";
    }

    const errorToWrite = {
      errorRecordsList,
      countError,
    };

    const recordsToWrite = {
      countError,
      countSuccess,
      lastIndex,
    };

    writeFileSync(errorFile, JSON.stringify(errorToWrite));
    writeFileSync(uploadRecordsFile, JSON.stringify(recordsToWrite));
  }

  process.exit();
}

main();
