import { appendFileSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers/create-supabase-client.js";
import { getPercentageString } from "../custom_helpers/string-formatters.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  const { imagesFolderPath, prod, errorFile, startWith } = argv;
  if ((!imagesFolderPath, !errorFile)) {
    console.log("Invalid inputs");
    return;
  }

  let supabase = createSupabaseClient(prod);

  /**
   * TODO:
   * - Get from to upload instead of poc
   * - Upload to actual project online
   */
  const imagesFolder = imagesFolderPath;
  const files = readdirSync(imagesFolder);

  const startIndex = startWith ? parseInt(startWith) : 0;

  for (let i = startIndex; i < files.length; i++) {
    const file = files[i];
    console.log("uploading", i, file);

    const srcFile = join(imagesFolder, file);
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

      const percentage = getPercentageString(i + 1, 0, files.length);
      console.log("done", i, percentage);
      console.log("done file", file);
    } catch (error) {
      const toWriteString = `
      file: ${file}
      index: ${i}
      error: ${error}
      `;
      appendFileSync(errorFile, toWriteString);
    }
  }
}

main();
