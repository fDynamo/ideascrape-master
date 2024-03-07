import { existsSync, readFileSync, writeFileSync } from "fs";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";

async function main() {
  dotenv.config();

  const idMapFilepath = "./tmp/images_fix_map.csv";
  const idMap = await readCsvFile(idMapFilepath);

  let supabase = createSupabaseClient(true);

  const toWriteObj = {};
  for (let i = 0; i < idMap.length; i++) {
    const entry = idMap[i];
    const oldFilename = entry["old_filename"];
    const newFilename = entry["new_filename"];

    // Update in search_main
    const updateRes = await supabase
      .from("search_main")
      .update({ product_image_filename: newFilename })
      .eq("product_image_filename", oldFilename)
      .select("id, sup_similarweb_id");

    console.log(i, "updated");

    toWriteObj["id_" + i] = {
      updateRes,
    };
  }

  const toWrite = JSON.stringify(toWriteObj);
  const outFilePath = "./tmp/fix_image_records.json";
  writeFileSync(outFilePath, toWrite);
}

main();
