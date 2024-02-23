import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import {
  createSearchMainRecordsCsvWriter,
  readSearchMainRecords,
} from "../custom_helpers_js/search-main-records-helpers.mjs";

/**
 */
async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { startIndex, endIndex, prod, reset } = argv;
  if (!startIndex || !endIndex || startIndex > endIndex || startIndex < 1) {
    console.log("Invalid inputs, HINT: start index has to start at 1");
    return;
  }

  const MAX_RECORDS = 1000;
  let supabase = createSupabaseClient(prod);

  const batchesList = [];
  while (startIndex < endIndex) {
    const newEndIndex = Math.min(startIndex + MAX_RECORDS, endIndex + 1);
    batchesList.push([startIndex, newEndIndex]);
    startIndex += MAX_RECORDS;
  }

  // Create CSV writer
  const csvWriter = createSearchMainRecordsCsvWriter(true, {
    append: reset ? false : true,
  });

  const downloadedRecords = [];
  for (let i = 0; i < batchesList.length; i++) {
    const [start, end] = batchesList[i];
    const { data, error } = await supabase
      .from("search_main")
      .select(
        `
    id, product_url
    `
      )
      .gte("id", start)
      .lt("id", end);

    if (error) {
      throw error;
    }

    await csvWriter.writeRecords(data);

    data.forEach((record) => {
      downloadedRecords.push(record);
    });

    if (data.length == 0) {
      break;
    }
    console.log("Downloaded ", i);
  }

  // Fix list
  if (!reset) {
    let recordsList = await readSearchMainRecords(prod);
    recordsList = [...recordsList, ...downloadedRecords];

    const dupeSet = {};
    const finalList = [];

    for (let i = 0; i < recordsList.length; i++) {
      const record = recordsList[i];
      if (!record.product_url) {
        continue;
      }
      if (dupeSet[record.id]) {
        continue;
      }
      dupeSet[record.id] = true;
      finalList.push(record);
    }

    const finalCsvWriter = createSearchMainRecordsCsvWriter(prod, {
      append: false,
    });

    await finalCsvWriter.writeRecords(finalList);
  }
}

main();
