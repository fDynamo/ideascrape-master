import { readFileSync, readdirSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { batchData, isNumber } from "../custom_helpers_js/index.js";

async function main() {
  const argv = yargs(hideBin(process.argv))
    .options({
      i: {
        alias: "in-folder-path",
        demandOption: true,
        type: "string",
        normalize: true,
      },
      r: {
        alias: "records-folder-path",
        demandOption: true,
        type: "string",
        normalize: true,
      },
      prod: {
        type: "boolean",
        default: false,
      },
      "only-action": {
        type: "string",
      },
      "one-file-index": {
        type: "number",
      },
      "one-batch-index": {
        type: "number",
      },
    })
    .parse();

  let {
    inFolderPath,
    recordsFolderPath,
    prod,
    onlyAction,
    oneFileIndex,
    oneBatchIndex,
  } = argv;

  const supabase = createSupabaseClient(prod);

  const upsyncFileList = readdirSync(inFolderPath);
  for (let fileIdx = 0; fileIdx < upsyncFileList.length; fileIdx++) {
    if (isNumber(oneFileIndex) && fileIdx !== oneFileIndex) {
      continue;
    }

    const fileName = upsyncFileList[fileIdx];
    const filePath = join(inFolderPath, fileName);
    const recordList = JSON.parse(readFileSync(filePath, "utf8"));

    // Sort records to action lists
    const upsertList = [];
    const deleteList = [];
    for (let recordIdx = 0; recordIdx < recordList.length; recordIdx++) {
      const recordObj = recordList[recordIdx];
      const upsyncAction = recordObj["upsync_action"];
      if (upsyncAction == "upsert") {
        delete recordObj["upsync_action"];
        upsertList.push(recordObj);
      }
      if (upsyncAction == "delete") {
        delete recordObj["upsync_action"];
        deleteList.push(recordObj["product_url"]);
      }
    }

    // Upsert
    const shouldUpsert = !onlyAction || onlyAction == "upsert";
    if (shouldUpsert) {
      const UPSERT_BATCH_SIZE = 1000;
      const upsertBatchList = batchData(upsertList, UPSERT_BATCH_SIZE);
      for (let batchIdx = 0; batchIdx < upsertBatchList.length; batchIdx++) {
        if (isNumber(oneBatchIndex) && batchIdx !== oneBatchIndex) {
          continue;
        }

        console.log("[upserting] file idx", fileIdx, "batch idx", batchIdx);

        const batchObj = upsertBatchList[batchIdx];
        const { data, error } = await supabase
          .from("search_main")
          .upsert(batchObj, {
            onConflict: "product_url",
            ignoreDuplicates: false,
          })
          .select();

        if (error) {
          console.log(
            "[failed upsert] file idx",
            fileIdx,
            "batch idx",
            batchIdx
          );
          console.error(error);
          process.exit(1);
        }

        if (data) {
          const uploadedList = data.map((obj) => {
            return { id: obj.id, product_url: obj.product_url };
          });
          const toRecordObj = {
            uploaded_list: uploadedList,
            num_uploaded: uploadedList.length,
          };

          let recordFileName = "upsync_upsert-";
          if (prod) {
            recordFileName += "prod-";
          } else {
            recordFileName += "local-";
          }
          recordFileName += "batch_" + batchIdx + "-" + fileName;

          const recordFilePath = join(recordsFolderPath, recordFileName);
          writeFileSync(recordFilePath, JSON.stringify(toRecordObj, null, 3));
        }
      }
    }

    const shouldDelete = !onlyAction || onlyAction == "delete";
    if (shouldDelete) {
      const DELETE_BATCH_SIZE = 1000;
      const deleteBatchList = batchData(deleteList, DELETE_BATCH_SIZE);
      for (let batchIdx = 0; batchIdx < deleteBatchList.length; batchIdx++) {
        if (isNumber(oneBatchIndex) && batchIdx !== oneBatchIndex) {
          continue;
        }

        console.log("[deleting] file idx", fileIdx, "batch idx", batchIdx);

        const batchObj = deleteBatchList[batchIdx];
        const { data, error } = await supabase
          .from("search_main")
          .delete()
          .in("product_url", batchObj)
          .select("id,product_url");

        if (error) {
          console.log(
            "[failed delete] file idx",
            fileIdx,
            "batch idx",
            batchIdx
          );
          console.error(error);
          process.exit(1);
        }

        if (data) {
          const uploadedList = data.map((obj) => {
            return { id: obj.id, product_url: obj.product_url };
          });
          const toRecordObj = {
            uploaded_list: uploadedList,
            num_uploaded: uploadedList.length,
          };

          let recordFileName = "upsync_delete-";
          if (prod) {
            recordFileName += "prod-";
          } else {
            recordFileName += "local-";
          }
          recordFileName += "batch_" + batchIdx + "-" + fileName;

          const recordFilePath = join(recordsFolderPath, recordFileName);
          writeFileSync(recordFilePath, JSON.stringify(toRecordObj, null, 3));
        }
      }
    }
  }

  process.exit(0);
}

main();
