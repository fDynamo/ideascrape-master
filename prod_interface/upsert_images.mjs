import { readFileSync, readdirSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { getPercentageString } from "../custom_helpers_js/string-formatters.js";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { timeoutPromise } from "../custom_helpers_js/index.js";
import registerGracefulExit from "../custom_helpers_js/graceful-exit.js";

async function main() {
  let RUN_DELAY = 500;
  const argv = yargs(hideBin(process.argv))
    .options({
      i: {
        alias: "in-folder-path",
        demandOption: true,
        type: "string",
        normalize: true,
      },
      r: {
        alias: "records-folder-path",
        demandOption: true,
        type: "string",
        normalize: true,
      },
      prod: {
        type: "boolean",
        default: false,
      },
      "start-index": {
        type: "number",
        default: 0,
      },
    })
    .parse();

  let { inFolderPath, recordsFolderPath, prod, startIndex } = argv;

  let supabase = createSupabaseClient(prod);

  const errorRecordsList = [];
  let countSuccess = 0;
  let countError = 0;
  let lastIndex = 0;

  let forcedToQuit = false;
  registerGracefulExit(() => {
    forcedToQuit = true;
  });

  const fileNameList = readdirSync(inFolderPath);
  for (let fileIdx = startIndex; fileIdx < fileNameList.length; fileIdx++) {
    if (forcedToQuit) {
      break;
    }
    lastIndex = fileIdx;

    const fileName = fileNameList[fileIdx];
    if (
      !fileName ||
      fileName.endsWith("svg") ||
      fileName.endsWith("csv") ||
      fileName == "ER"
    ) {
      console.log("skipping", fileIdx, fileName);
      continue;
    }

    const srcFile = join(inFolderPath, fileName);
    console.log("uploading", fileIdx, fileName, srcFile);

    const fileBody = readFileSync(srcFile);
    const extensionIndex = srcFile.lastIndexOf(".");
    const extension =
      extensionIndex > -1
        ? srcFile.substring(extensionIndex + 1).toLowerCase()
        : "";

    let contentType = "image/png";
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
        .upload(fileName, fileBody, {
          contentType,
          upsert: true,
        });
      if (error) throw error;

      const percentage = getPercentageString(
        fileIdx + 1,
        0,
        fileNameList.length
      );
      console.log("done", fileIdx, percentage);
      console.log("done file", fileName);
      countSuccess += 1;
    } catch (error) {
      console.error("Error", fileIdx, fileName);
      console.error(error);
      countError += 1;
      errorRecordsList.push({
        fileName,
        index: fileIdx,
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
