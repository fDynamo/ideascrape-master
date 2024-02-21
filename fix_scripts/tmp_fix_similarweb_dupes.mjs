import { existsSync, readFileSync, writeFileSync } from "fs";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";

async function main() {
  dotenv.config();

  const idMapFilepath = "./tmp/source_domain_id_map.csv";
  const idMap = await readCsvFile(idMapFilepath);

  let supabase = createSupabaseClient(true);

  const toWriteObj = {};
  for (let i = 0; i < idMap.length; i++) {
    const entry = idMap[i];
    const toKeepId = entry["to_keep_id"];
    const toRemoveId = entry["to_remove_id"];

    // Update in search_main
    const updateRes = await supabase
      .from("search_main")
      .update({ similarweb_id: toKeepId })
      .eq("similarweb_id", toRemoveId)
      .select("id, similarweb_id");

    console.log(i, "updated");

    // Remove in similarweb
    const deleteRes = await supabase
      .from("sup_similarweb")
      .delete()
      .eq("id", toRemoveId)
      .select("id");

    console.log(i, "deleted");
    toWriteObj["id_" + i] = {
      updateRes,
      deleteRes,
    };
  }

  const toWrite = JSON.stringify(toWriteObj);
  const outFilePath = "./tmp/fix_records.json";
  writeFileSync(outFilePath, toWrite);
}

main();
