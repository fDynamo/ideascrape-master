import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import createSupabaseClient from "../custom_helpers/create-supabase-client.js";
import { readCsvFile } from "../custom_helpers/read-csv.js";
import * as dotenv from "dotenv";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

/**
 * TODO: Handle duplicate rows
 *
 *  node add_records.mjs --masterOutFolder non_poc --recordsFolder non_poc --fileName search_main
 * FOR TESTING:
 * If for AIFT started at 1683
 */
async function main() {
  dotenv.config();

  const argv = yargs(hideBin(process.argv)).argv;
  let { toUploadPath, fileName, recordsFolder, prod } = argv;
  const TYPE_TO_TABLE = {
    source_aift: "source_aift",
    source_ph: "source_ph",
    sup_similarweb: "sup_similarweb",
    search_main: "search_main",
  };
  const FILE_TYPES_LIST = Object.keys(TYPE_TO_TABLE);

  if (
    !toUploadPath ||
    !fileName ||
    !recordsFolder ||
    !FILE_TYPES_LIST.includes(fileName)
  ) {
    console.log("Invalid inputs");
    return;
  }

  const filePath = join(toUploadPath, fileName + ".csv");

  let inDataList = await readCsvFile(filePath);
  let supabase = createSupabaseClient(prod);

  let table = TYPE_TO_TABLE[fileName];

  let toWriteObj = {};

  if (fileName == TYPE_TO_TABLE.search_main) {
    let source_aift_record = null;
    const source_aift_record_file = join(recordsFolder, "source_aift.json");
    if (existsSync(source_aift_record_file)) {
      source_aift_record = JSON.parse(readFileSync(source_aift_record_file));
    }

    let source_ph_record = null;
    const source_ph_record_file = join(recordsFolder, "source_ph.json");
    if (existsSync(source_ph_record_file)) {
      source_ph_record = JSON.parse(readFileSync(source_ph_record_file));
    }

    let sup_similarweb_record = null;
    const sup_similarweb_record_file = join(
      recordsFolder,
      "sup_similarweb.json"
    );
    if (existsSync(sup_similarweb_record_file)) {
      sup_similarweb_record = JSON.parse(
        readFileSync(sup_similarweb_record_file)
      );
    }

    inDataList = inDataList.map((record) => {
      let aiftId = record.aift_id || null;
      if (source_aift_record) {
        aiftId = aiftId
          ? source_aift_record.startId - 1 + parseInt(aiftId)
          : null;
      }

      let phId = record.ph_id || null;
      if (source_ph_record) {
        phId = phId ? source_ph_record.startId - 1 + parseInt(phId) : null;
      }

      let similarwebId = record.similarweb_id || null;
      if (sup_similarweb_record) {
        similarwebId = similarwebId
          ? sup_similarweb_record.startId - 1 + parseInt(similarwebId)
          : null;
      }

      return {
        ...record,
        similarweb_id: similarwebId,
        aift_id: aiftId,
        ph_id: phId,
      };
    });

    async function safeAdd(tableName, inData) {
      try {
        const { data, error } = await supabase
          .from(tableName)
          .insert(inData)
          .select();
        if (error) {
          throw error;
        }
        return { isError: false, resData: data };
      } catch (error) {
        const errString = error + " ";
        return { isError: true, errString, error };
      }
    }

    // Try go all at once
    const { isError, errString, error, resData } = await safeAdd(
      table,
      inDataList
    );
    if (isError) {
      const RANGE_ERROR_STRING = "RangeError: Invalid string length";
      if (!errString.includes(RANGE_ERROR_STRING)) {
        console.log("ERROR", error);
        return;
      } else {
        console.log("Could not go all at once, slicing up input");
        // Break apart and try one by one
        const BREAK_NUM = 10;
        const BREAK_SIZE = Math.floor(inDataList.length / BREAK_NUM);
        for (let i = 0; i < BREAK_NUM; i++) {
          const startIndex = i * BREAK_SIZE;
          const isLastSlice = i + 1 == BREAK_NUM;
          const endIndex = isLastSlice
            ? inDataList.length
            : (i + 1) * BREAK_SIZE;
          const toAdd = inDataList.slice(startIndex, endIndex);
          const { isError, error } = await safeAdd(table, toAdd);

          const sliceNum = i + 1;
          if (isError) {
            console.log("Error adding slice", sliceNum);
            console.log("Start end", startIndex, endIndex);
            console.log(error);
            return;
          } else {
            console.log("Success adding", sliceNum);
            toWriteObj["success_" + sliceNum] = {
              startIndex,
              endIndex,
            };
          }
        }
      }
    } else {
      toWriteObj = {
        message: "success in 1 go",
        addedSize: inDataList.length,
      };
    }

    const toWrite = JSON.stringify(toWriteObj);
    const outFilePath = join(recordsFolder, fileName + ".json");
    writeFileSync(outFilePath, toWrite);

    return;
  }

  const { data, error } = await supabase
    .from(table)
    .insert(inDataList)
    .select();
  if (error) {
    console.log(error);
    toWriteObj.runError = error;
  }
  if (data) {
    const runData = data;
    const numInserted = runData.length;
    const startId = runData[0].id;
    const endId = runData[numInserted - 1].id;
    toWriteObj = {
      numInserted,
      startId,
      endId,
      idList: runData.map((obj) => obj.id),
    };
    console.log(numInserted + " rows inserted");
  }

  const toWrite = JSON.stringify(toWriteObj);
  const outFilePath = join(recordsFolder, fileName + ".json");
  writeFileSync(outFilePath, toWrite);
}

main();
