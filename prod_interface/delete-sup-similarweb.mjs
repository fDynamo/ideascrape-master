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

/**
 * TODO:
 * - Handle duplicate rows
 * - Batch uploads to not do too many changes at once!
 */

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { rejectedFilePath, prod } = argv;
  if (!rejectedFilePath) {
    console.error("Invalid inputs");
    process.exit(1);
  }

  try {
    const rejectedRecords = await readCsvFile(rejectedFilePath);
    let supabase = createSupabaseClient(prod);

    // Get all rows first
    const domainsList = rejectedRecords.map((obj) => obj.domain);
    const getRes = await batchActionIn(supabase, {
      action: "fetch",
      tableName: "sup_similarweb",
      selectCols: "id",
      inColName: "source_domain",
      inList: domainsList,
      batchSize: MAX_FETCH_BATCH_SIZE,
      shouldLog: true,
    });

    if (getRes.error) {
      throw getRes.error;
    }

    const domainIds = getRes.data.map((obj) => obj.id);
    if (!domainIds.length) {
      console.log("Nothing to delete");
      process.exit();
    }

    // Update search main
    const updateRes = await batchActionIn(supabase, {
      action: "update",
      tableName: "search_main",
      selectCols: "product_url",
      inColName: "similarweb_id",
      inList: domainIds,
      batchSize: MAX_UPDATE_BATCH_SIZE,
      shouldLog: true,
      updateVal: { similarweb_id: null },
    });

    if (updateRes.error) {
      throw updateRes.error;
    }

    // Delete
    const deleteRes = await batchActionIn(supabase, {
      action: "delete",
      tableName: "sup_similarweb",
      selectCols: "id,source_domain",
      inColName: "id",
      inList: domainIds,
      batchSize: MAX_DELETE_BATCH_SIZE,
      shouldLog: true,
    });

    if (deleteRes.error) {
      throw deleteRes.error;
    }

    const toLog = {
      deleteRes,
      updateRes,
      getRes,
    };
    console.log(JSON.stringify(toLog));
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

main();
