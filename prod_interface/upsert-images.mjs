import { appendFileSync, readFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { getPercentageString } from "../custom_helpers_js/string-formatters.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { timeoutPromise } from "../custom_helpers_js/index.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";

async function main() {
  dotenv.config();

  let RUN_DELAY = 500;
  const argv = yargs(hideBin(process.argv)).argv;
  const { imagesFolderPath, prod, errorFile, startIndex: inStartIndex } = argv;
  if (!imagesFolderPath || !errorFile) {
    console.log("Invalid inputs");
    return;
  }

  let supabase = createSupabaseClient(prod);

  const imagesFolder = imagesFolderPath;
  const recordFile = join(imagesFolderPath, "_record.csv"); // Get filename from RECORD_FILE constant
  const recordsList = await readCsvFile(recordFile);

  const startIndex = inStartIndex ? parseInt(inStartIndex) : 0;

  for (let i = startIndex; i < recordsList.length; i++) {
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
    } catch (error) {
      console.error(error);
      const toWriteString = `
      file: ${file}
      index: ${i}
      error: ${error}
      `;
      appendFileSync(errorFile, toWriteString);
    }

    await timeoutPromise(RUN_DELAY);
  }
}

main();
