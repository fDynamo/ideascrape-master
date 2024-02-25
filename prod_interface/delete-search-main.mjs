import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import {
  MAX_DELETE_BATCH_SIZE,
  MAX_FETCH_BATCH_SIZE,
  MAX_UPDATE_BATCH_SIZE,
  batchActionIn,
} from "../custom_helpers_js/prod-interface-helpers.mjs";
import { writeFileSync } from "fs";

/**
 * TODO:
 * - Handle duplicate rows
 * - Batch uploads to not do too many changes at once!
 */

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { deleteListFilePath, prod, recordsFolder } = argv;
  if (!deleteListFilePath) {
    console.error("Invalid inputs");
    process.exit(1);
  }

  try {
    const rejectedRecords = await readCsvFile(deleteListFilePath);
    let supabase = createSupabaseClient(prod);

    const urlList = rejectedRecords.map((record) => record.url);

    // Delete
    const deleteRes = await batchActionIn(supabase, {
      action: "delete",
      tableName: "search_main",
      selectCols: "id,product_url",
      inColName: "product_url",
      inList: urlList,
      batchSize: MAX_DELETE_BATCH_SIZE,
      shouldLog: true,
    });

    if (deleteRes.error) {
      throw deleteRes.error;
    }

    if (recordsFolder) {
      const countDeleted = deleteRes.data.length;

      const toWriteObj = {
        countDeleted,
        deleteRes,
      };

      const toWrite = JSON.stringify(toWriteObj);
      const fileSuffix = prod ? "prod.json" : "local.json";
      const recordsFilePath = join(
        recordsFolder,
        "delete_search_main_" + fileSuffix
      );

      writeFileSync(recordsFilePath, toWrite);
    }
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

main();
