import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { writeFileSync } from "fs";
import { join } from "path";

/**
 * This currently just works for similarweb ids for blink-remap-search-main-ids
 *
 */
async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { inputFilePath, prod, recordsFolder } = argv;
  if (!inputFilePath) {
    console.error("Invalid inputs");
    process.exit(1);
  }

  const errorList = [];
  let countSuccess = 0;
  let countError = 0;
  try {
    const inRecords = await readCsvFile(inputFilePath);
    let supabase = createSupabaseClient(prod);

    for (let i = 0; i < inRecords.length; i++) {
      const record = inRecords[i];
      console.log("Updating", i, record);

      let { id, sup_similarweb_id } = record;
      if (!sup_similarweb_id) {
        sup_similarweb_id = null;
      } else {
        sup_similarweb_id = Math.floor(sup_similarweb_id);
      }

      const { error } = await supabase
        .from("search_main")
        .update({ sup_similarweb_id })
        .eq("id", id);

      if (error) {
        countError += 1;
        errorList.push({
          record,
          error,
        });
      } else {
        countSuccess += 1;
      }

      console.log("Finish", i, record);
    }

    if (recordsFolder) {
      const toWriteObj = {
        countSuccess,
        countError,
        errorList,
      };

      const toWrite = JSON.stringify(toWriteObj);
      const fileSuffix = prod ? "prod.json" : "local.json";
      const recordsFilePath = join(
        recordsFolder,
        "update_search_main_ids_" + fileSuffix
      );

      writeFileSync(recordsFilePath, toWrite);
    }
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

main();
