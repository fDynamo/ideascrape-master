import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import {
  createSearchMainRecordsCsvWriter,
  readSearchMainRecords,
} from "../custom_helpers_js/cache-folder-helpers.mjs";

/**
 * TODO:
 * - Handle duplicate rows
 * - Batch uploads to not do too many changes at once!
 */

async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { toUploadFolderPath, fileName, recordsFolder, prod } = argv;
  const TABLE_DICT = {
    source_aift: {
      table: "source_aift",
      recordFilename: "source_aift",
    },
    source_ph: {
      table: "source_ph",
      recordFilename: "source_ph",
    },
    sup_similarweb: {
      table: "sup_similarweb",
      recordFilename: "sup_similarweb",
    },
    search_main: {
      table: "search_main",
      recordFilename: "search_main",
    },
  };
  const ACCEPTED_FILENAMES = Object.keys(TABLE_DICT);

  if (
    !toUploadFolderPath ||
    !fileName ||
    !recordsFolder ||
    !ACCEPTED_FILENAMES.includes(fileName)
  ) {
    console.log("Invalid inputs");
    return;
  }

  // Change records filenames
  ACCEPTED_FILENAMES.forEach((keyStr) => {
    let newRecordFilename = TABLE_DICT[keyStr].recordFilename;
    if (prod) {
      newRecordFilename += "_prod.json";
    } else {
      newRecordFilename += "_local.json";
    }

    TABLE_DICT[keyStr].recordFilename = newRecordFilename;
  });

  const tableDictObj = TABLE_DICT[fileName];

  const inDataFilePath = join(toUploadFolderPath, fileName + ".csv");
  const recordsFilePath = join(recordsFolder, tableDictObj.recordFilename);

  let inDataList = await readCsvFile(inDataFilePath);
  let supabase = createSupabaseClient(prod);

  let table = tableDictObj.table;

  let toWriteObj = {};
  let onConflictVal = "";
  let isSearchMain = false;

  if (fileName == "search_main") {
    onConflictVal = "product_url";
    isSearchMain = true;

    // Read other record files
    let source_aift_record = null;
    let aiftIdList = [];
    const source_aift_record_file = join(
      recordsFolder,
      TABLE_DICT["source_aift"].recordFilename
    );
    if (existsSync(source_aift_record_file)) {
      source_aift_record = JSON.parse(readFileSync(source_aift_record_file));
      aiftIdList = source_aift_record.idList;
    }

    let source_ph_record = null;
    let phIdList = [];
    const source_ph_record_file = join(
      recordsFolder,
      TABLE_DICT["source_ph"].recordFilename
    );
    if (existsSync(source_ph_record_file)) {
      source_ph_record = JSON.parse(readFileSync(source_ph_record_file));
      phIdList = source_ph_record.idList;
    }

    let sup_similarweb_record = null;
    const sup_similarweb_record_file = join(
      recordsFolder,
      TABLE_DICT["sup_similarweb"].recordFilename
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
  } else if (fileName == "source_aift") {
    onConflictVal = "source_url";
  } else if (fileName == "source_ph") {
    onConflictVal = "source_url";
  } else if (fileName == "sup_similarweb") {
    onConflictVal = "source_domain";
  }

  // Break apart and try one by one
  const BREAK_SIZE = 1000;
  const BREAK_NUM =
    BREAK_SIZE < inDataList.length
      ? Math.ceil(inDataList.length / BREAK_SIZE)
      : 1;
  console.log("num slices", BREAK_NUM);

  const uploadedIds = [];
  const upsertedSearchMainRecords = [];
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
      console.log("Num added", data.length);
      data.forEach((obj) => {
        uploadedIds.push(obj.id);
        if (isSearchMain) {
          upsertedSearchMainRecords.push({
            id: obj.id,
            product_url: obj.product_url,
          });
        }
      });
    }
  }

  toWriteObj.numInserted = uploadedIds.length;
  toWriteObj.idList = uploadedIds;

  // Update cache after upload
  if (isSearchMain) {
    const searchMainRecords = await readSearchMainRecords(prod);
    const oldRecordsCount = searchMainRecords.length;

    const dupeSet = {};

    const newData = [...searchMainRecords, ...upsertedSearchMainRecords];
    const toWriteRecords = [];
    for (let i = 0; i < newData.length; i++) {
      const record = newData[i];
      if (!record.product_url) continue;
      if (dupeSet[record.id]) continue;

      dupeSet[record.id] = true;
      toWriteRecords.push(record);
    }

    const newRecordsCount = toWriteRecords.length;

    const searchMainRecordsWriter = createSearchMainRecordsCsvWriter(prod, {
      append: false,
    });
    await searchMainRecordsWriter.writeRecords(toWriteRecords);

    toWriteObj.newRecordsCount = newRecordsCount;
    toWriteObj.newRecords = newRecordsCount - oldRecordsCount;
  }

  const toWrite = JSON.stringify(toWriteObj);
  writeFileSync(recordsFilePath, toWrite);
}

main();
