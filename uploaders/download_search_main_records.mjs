import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { accessCacheFolder } from "../custom_helpers_js/get-paths.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { createObjectCsvWriter } from "csv-writer";

/**
 */
async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { startIndex, endIndex, prod } = argv;
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
  const CSV_HEADER = [
    { id: "id", title: "id" },
    { id: "product_url", title: "product_url" },
    { id: "aift_id", title: "aift_id" },
    { id: "ph_id", title: "ph_id" },
    { id: "similarweb_id", title: "similarweb_id" },
  ];

  const csvWriter = createObjectCsvWriter({
    path: accessCacheFolder("file_search_main_records"),
    header: CSV_HEADER,
    append: true,
  });

  for (let i = 0; i < batchesList.length; i++) {
    const [start, end] = batchesList[i];
    const { data, error } = await supabase
      .from("search_main")
      .select(
        `
    id, product_url, aift_id, ph_id, similarweb_id
    `
      )
      .gte("id", start)
      .lt("id", end);

    if (error) {
      throw error;
    }

    await csvWriter.writeRecords(data);
  }
}

main();
