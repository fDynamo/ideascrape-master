import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv))
    .options({
      o: {
        alias: "out-folder-path",
        type: "string",
        default: "./tmp_cache_search_main",
      },
      prod: {
        type: "boolean",
      },
      "start-index": {
        type: "number",
        default: 0,
      },
      "end-index": {
        type: "number",
        default: 9999999, // Arbitrarily high number signalling max amount
      },
    })
    .parse();
  let { startIndex, endIndex, prod, outFolderPath } = argv;

  // Create folder if not exists
  if (!existsSync(outFolderPath)) mkdirSync(outFolderPath);

  const MAX_RECORDS_PER_BATCH = 1000;
  let supabase = createSupabaseClient(prod);

  const batchesList = [];
  while (startIndex < endIndex) {
    const newEndIndex = Math.min(
      startIndex + MAX_RECORDS_PER_BATCH,
      endIndex + 1
    );
    batchesList.push([startIndex, newEndIndex]);
    startIndex += MAX_RECORDS_PER_BATCH;
  }

  for (let i = 0; i < batchesList.length; i++) {
    const [start, end] = batchesList[i];
    const { data, error } = await supabase
      .from("search_main")
      .select(
        `
    id, product_url, info_added_at, info_updated_at
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

    const savePath = join(outFolderPath, "batch_" + i + ".json");
    writeFileSync(savePath, JSON.stringify(data, null, 3));
    console.log("Retrieved ", i);
  }
}

main();
