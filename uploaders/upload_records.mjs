import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

/**
 * TODO:
 * - Handle duplicate rows
 * - Batch uploads to not do too many changes at once!
 */

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { toUploadFolderPath, fileName, recordsFolder, prod } = argv;
  const TYPE_TO_TABLE = {
    source_aift: "source_aift",
    source_ph: "source_ph",
    sup_similarweb: "sup_similarweb",
    search_main: "search_main",
  };
  const FILE_TYPES_LIST = Object.keys(TYPE_TO_TABLE);

  if (
    !toUploadFolderPath ||
    !fileName ||
    !recordsFolder ||
    !FILE_TYPES_LIST.includes(fileName)
  ) {
    console.log("Invalid inputs");
    return;
  }

  const filePath = join(toUploadFolderPath, fileName + ".csv");

  let inDataList = await readCsvFile(filePath);
  let supabase = createSupabaseClient(prod);

  let table = TYPE_TO_TABLE[fileName];

  let toWriteObj = {};
  let onConflictVal = "";
  if (fileName == TYPE_TO_TABLE.search_main) {
    onConflictVal = "product_url";

    // Read other record files
    let source_aift_record = null;
    let aiftIdList = [];
    const source_aift_record_file = join(recordsFolder, "source_aift.json");
    if (existsSync(source_aift_record_file)) {
      source_aift_record = JSON.parse(readFileSync(source_aift_record_file));
      aiftIdList = source_aift_record.idList;
    }

    let source_ph_record = null;
    let phIdList = [];
    const source_ph_record_file = join(recordsFolder, "source_ph.json");
    if (existsSync(source_ph_record_file)) {
      source_ph_record = JSON.parse(readFileSync(source_ph_record_file));
      phIdList = source_ph_record.idList;
    }

    let sup_similarweb_record = null;
    const sup_similarweb_record_file = join(
      recordsFolder,
      "sup_similarweb.json"
    );
    let similarwebIdList = [];
    if (existsSync(sup_similarweb_record_file)) {
      sup_similarweb_record = JSON.parse(
        readFileSync(sup_similarweb_record_file)
      );
      similarwebIdList = sup_similarweb_record.idList;
    }

    // Modify ids in data
    inDataList = inDataList.map((record) => {
      let aiftId = record.aift_id || null;
      if (source_aift_record && aiftId) {
        aiftId = aiftIdList[parseInt(aiftId) - 1];
      }

      let phId = record.ph_id || null;
      if (source_ph_record && phId) {
        phId = phIdList[parseInt(phId) - 1];
      }

      let similarwebId = record.similarweb_id || null;
      if (sup_similarweb_record && similarwebId) {
        similarwebId = similarwebIdList[parseInt(similarwebId) - 1];
      }

      return {
        ...record,
        similarweb_id: similarwebId,
        aift_id: aiftId,
        ph_id: phId,
      };
    });
  } else if (fileName == TYPE_TO_TABLE.source_aift) {
    onConflictVal = "source_url";
  } else if (fileName == TYPE_TO_TABLE.source_ph) {
    onConflictVal = "source_url";
  } else if (fileName == TYPE_TO_TABLE.sup_similarweb) {
    onConflictVal = "source_domain";
  }

  // Break apart and try one by one
  const BREAK_SIZE = 1000;
  const BREAK_NUM =
    BREAK_SIZE < inDataList.length
      ? Math.ceil(inDataList.length / BREAK_SIZE)
      : 1;
  const SLICE_PREFIX_STR = "slice_";
  console.log("num slices", BREAK_NUM);

  for (let i = 0; i < BREAK_NUM; i++) {
    const startIndex = i * BREAK_SIZE;
    const isLastSlice = i + 1 == BREAK_NUM;
    const endIndex = isLastSlice ? inDataList.length : (i + 1) * BREAK_SIZE;
    const toAdd = inDataList.slice(startIndex, endIndex);

    const { data, error } = await supabase
      .from(table)
      .upsert(toAdd, {
        onConflict: onConflictVal,
        ignoreDuplicates: false,
      })
      .select();

    const sliceNum = i + 1;
    if (error) {
      console.log("Error adding slice", sliceNum);
      console.log("Start end", startIndex, endIndex);
      console.error(error);
      toWriteObj.lastSlice = {
        sliceNum,
        startIndex,
        endIndex,
        error,
      };
      break;
    }

    if (data) {
      console.log("Success adding", sliceNum);
      toWriteObj[SLICE_PREFIX_STR + sliceNum] = {
        numInserted: data.length,
        idList: data.map((obj) => obj.id),
      };
    }
  }

  const toWriteKeys = Object.keys(toWriteObj);
  const successSliceKeys = toWriteKeys.filter((val) =>
    val.startsWith(SLICE_PREFIX_STR)
  );

  let totalNumInserted = 0;
  let totalIdList = [];

  successSliceKeys.forEach((keyStr) => {
    const itemObj = toWriteObj[keyStr];
    totalNumInserted += itemObj.numInserted;
    totalIdList = [...totalIdList, ...itemObj.idList];

    delete toWriteObj[keyStr];
  });

  toWriteObj.numInserted = totalNumInserted;
  toWriteObj.idList = totalIdList;

  const toWrite = JSON.stringify(toWriteObj);
  const outFilePath = join(recordsFolder, fileName + ".json");
  writeFileSync(outFilePath, toWrite);
}

main();
