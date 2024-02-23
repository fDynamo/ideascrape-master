import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { accessCacheFolder } from "../custom_helpers_js/get-paths.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { createObjectCsvWriter } from "csv-writer";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";

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
  let cacheKey = "file_search_main_records";
  if (prod) cacheKey += "_prod";
  else cacheKey += "_local";

  const RECORDS_FILEPATH = accessCacheFolder(cacheKey);
  let supabase = createSupabaseClient(prod);

  const batchesList = [];
  while (startIndex < endIndex) {
    const newEndIndex = Math.min(startIndex + MAX_RECORDS, endIndex + 1);
    batchesList.push([startIndex, newEndIndex]);
    startIndex += MAX_RECORDS;
  }

  // Create CSV writer
  const CSV_HEADER = [
    { id: "id", title: "id" },
    { id: "product_url", title: "product_url" },
  ];

  const csvWriter = createObjectCsvWriter({
    path: RECORDS_FILEPATH,
    header: CSV_HEADER,
    append: reset ? false : true,
  });

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
    if (data.length < MAX_RECORDS) {
      break;
    }
  }

  // Fix list
  if (!reset) {
    const recordsList = await readCsvFile(RECORDS_FILEPATH);

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

    const finalCsvWriter = createObjectCsvWriter({
      path: RECORDS_FILEPATH,
      header: CSV_HEADER,
      append: false,
    });

    await finalCsvWriter.writeRecords(finalList);
  }
}

main();
