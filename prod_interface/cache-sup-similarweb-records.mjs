import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import {
  createSupSimilarwebRecordsCsvWriter,
  readSupSimilarwebRecords,
} from "../custom_helpers_js/cache-folder-helpers.mjs";

export async function appendAndFixSupSimilarwebRecordsCache(prod, newRecords) {
  let recordsList = await readSupSimilarwebRecords(prod);
  recordsList = [...recordsList, ...newRecords];

  const dupeSet = {};
  const finalList = [];

  for (let i = 0; i < recordsList.length; i++) {
    const record = recordsList[i];
    if (!record.source_domain) {
      continue;
    }
    if (dupeSet[record.id]) {
      continue;
    }
    dupeSet[record.id] = true;
    finalList.push(record);
  }

  const finalCsvWriter = createSupSimilarwebRecordsCsvWriter(prod, {
    append: false,
  });

  await finalCsvWriter.writeRecords(finalList);
}

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { startIndex, endIndex, prod, reset } = argv;

  // Validate inputs
  if (startIndex && !endIndex) {
    console.log("No end index provided");
    process.exit(1);
  }

  if (endIndex && !startIndex) {
    startIndex = 0;
  }

  if (!startIndex && !endIndex) {
    startIndex = 0;
    endIndex = 999999; // Arbitrarily high number signalling max amount
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
  const csvWriter = createSupSimilarwebRecordsCsvWriter(prod, {
    append: reset ? false : true,
  });

  const downloadedRecords = [];
  for (let i = 0; i < batchesList.length; i++) {
    const [start, end] = batchesList[i];
    const { data, error } = await supabase
      .from("sup_similarweb")
      .select(
        `
    id, source_domain
    `
      )
      .gte("id", start)
      .lt("id", end);

    if (error) {
      throw error;
    }

    if (data.length == 0) {
      break;
    }
    await csvWriter.writeRecords(data);

    data.forEach((record) => {
      downloadedRecords.push(record);
    });

    console.log("Downloaded ", i);
  }

  // Fix list
  if (!reset) {
    await appendAndFixSupSimilarwebRecordsCache(prod, downloadedRecords);
  }
}

main();
