import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers_js/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers_js/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import {
  appendAndFixSearchMainRecordsCache,
  appendAndFixSupSimilarwebRecordsCache,
} from "../custom_helpers_js/cache-helpers.mjs";

const TABLE_DICT = {
  source_aift: {
    name: "source_aift",
    table: "source_aift",
    recordFilename: "source_aift",
    conflictVal: "source_url",
  },
  source_ph: {
    name: "source_ph",
    table: "source_ph",
    recordFilename: "source_ph",
    conflictVal: "source_url",
  },
  sup_similarweb: {
    name: "sup_similarweb",
    table: "sup_similarweb",
    recordFilename: "sup_similarweb",
    conflictVal: "source_domain",
  },
  search_main: {
    name: "search_main",
    table: "search_main",
    recordFilename: "search_main",
    conflictVal: "product_url",
  },
};
const ACCEPTED_FILENAMES = Object.keys(TABLE_DICT);

async function main() {
  const argv = yargs(hideBin(process.argv)).argv;
  let {
    toUploadFolderPath,
    fileName,
    recordsFolderPath,
    prod,
    searchMainOnly,
  } = argv;

  if (
    !toUploadFolderPath ||
    !recordsFolderPath ||
    (fileName && !ACCEPTED_FILENAMES.includes(fileName))
  ) {
    console.log("Invalid inputs");
    process.exit(1);
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

  if (fileName) {
    await upsertTable(
      fileName,
      toUploadFolderPath,
      recordsFolderPath,
      prod,
      searchMainOnly
    );
  } else {
    // NOTE: Filenames constructured so that search main is always last
    for (let i = 0; i < ACCEPTED_FILENAMES.length; i++) {
      const fileName = ACCEPTED_FILENAMES[i];
      await upsertTable(
        fileName,
        toUploadFolderPath,
        recordsFolderPath,
        prod,
        searchMainOnly
      );
    }
  }

  process.exit(0);
}

async function upsertTable(
  fileName,
  toUploadFolderPath,
  recordsFolderPath,
  prod = false,
  searchMainOnly = false
) {
  let tableDictObj = TABLE_DICT.search_main;
  if (fileName == TABLE_DICT.search_main.name) {
  } else if (fileName == TABLE_DICT.source_aift.name) {
    tableDictObj = TABLE_DICT.source_aift;
  } else if (fileName == TABLE_DICT.source_ph.name) {
    tableDictObj = TABLE_DICT.source_ph;
  } else if (fileName == TABLE_DICT.sup_similarweb.name) {
    tableDictObj = TABLE_DICT.sup_similarweb;
  } else {
    console.log("Invalid fileName");
    process.exit(1);
  }

  const inDataFilePath = join(toUploadFolderPath, fileName + ".csv");
  const recordsFilePath = join(recordsFolderPath, tableDictObj.recordFilename);

  let inDataList = await readCsvFile(inDataFilePath);
  let supabase = createSupabaseClient(prod);

  if (tableDictObj.name == TABLE_DICT.search_main.name) {
    if (searchMainOnly) {
      inDataList = inDataList.map((record) => {
        delete record.source_aift_id;
        delete record.source_ph_id;
        delete record.sup_similarweb_id;
        return record;
      });
    } else {
      // Read other record files
      let source_aift_record = null;
      let aiftIdList = [];

      let source_ph_record = null;
      let phIdList = [];

      let sup_similarweb_record = null;
      let similarwebIdList = [];

      const source_aift_record_file = join(
        recordsFolderPath,
        TABLE_DICT.source_aift.recordFilename
      );
      if (existsSync(source_aift_record_file)) {
        source_aift_record = JSON.parse(readFileSync(source_aift_record_file));
        aiftIdList = source_aift_record.idList;
      }

      const source_ph_record_file = join(
        recordsFolderPath,
        TABLE_DICT.source_ph.recordFilename
      );
      if (existsSync(source_ph_record_file)) {
        source_ph_record = JSON.parse(readFileSync(source_ph_record_file));
        phIdList = source_ph_record.idList;
      }

      const sup_similarweb_record_file = join(
        recordsFolderPath,
        TABLE_DICT.sup_similarweb.recordFilename
      );
      if (existsSync(sup_similarweb_record_file)) {
        sup_similarweb_record = JSON.parse(
          readFileSync(sup_similarweb_record_file)
        );
        similarwebIdList = sup_similarweb_record.idList;
      }

      // Modify ids in data
      inDataList = inDataList.map((record) => {
        let aiftId = record.source_aift_id || null;
        let phId = record.source_ph_id || null;
        let similarwebId = record.sup_similarweb_id || null;

        if (source_aift_record && aiftId) {
          aiftId = aiftIdList[parseInt(aiftId) - 1];
        }

        if (source_ph_record && phId) {
          phId = phIdList[parseInt(phId) - 1];
        }

        if (sup_similarweb_record && similarwebId) {
          similarwebId = similarwebIdList[parseInt(similarwebId) - 1];
        }

        record = {
          ...record,
          sup_similarweb_id: similarwebId,
          source_aift_id: aiftId,
          source_ph_id: phId,
        };

        return record;
      });
    }
  } else {
    if (searchMainOnly) {
      console.log("searchMainOnly used incorrectly");
      process.exit(1);
    }
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
  const upsertedSupSimilarwebRecords = [];

  const toWriteObj = {};
  const table = tableDictObj.table;
  const onConflictVal = tableDictObj.conflictVal;

  let isSuccessful = true;
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
      isSuccessful = false;
      break;
    }

    if (data) {
      console.log("Success adding", sliceNum);
      console.log("Num added", data.length);
      data.forEach((obj) => {
        uploadedIds.push(obj.id);

        // Update respective records
        if (tableDictObj.name == TABLE_DICT.search_main.name) {
          upsertedSearchMainRecords.push({
            id: obj.id,
            product_url: obj.product_url,
          });
        }

        if (tableDictObj.name == TABLE_DICT.sup_similarweb.name) {
          upsertedSupSimilarwebRecords.push({
            id: obj.id,
            source_domain: obj.source_domain,
          });
        }
      });
    }
  }

  toWriteObj.numRowsModified = uploadedIds.length;
  toWriteObj.idList = uploadedIds;

  // Update cache after upload
  if (upsertedSearchMainRecords.length) {
    appendAndFixSearchMainRecordsCache(prod, upsertedSearchMainRecords);
  }
  if (upsertedSupSimilarwebRecords.length) {
    appendAndFixSupSimilarwebRecordsCache(prod, upsertedSupSimilarwebRecords);
  }

  const toWrite = JSON.stringify(toWriteObj);
  writeFileSync(recordsFilePath, toWrite);

  if (!isSuccessful) {
    process.exit(1);
  }
}

main();
