import { createObjectCsvWriter } from "csv-writer";
import { accessCacheFolder } from "./get-paths.js";
import { readCsvFile } from "./read-csv.js";

export function getSearchMainRecordsFilepath(prod) {
  let cacheKey = "file_search_main_records";
  if (prod) cacheKey += "_prod";
  else cacheKey += "_local";
  return accessCacheFolder(cacheKey);
}

export function createSearchMainRecordsCsvWriter(prod, options) {
  // Handle options
  let shouldAppend = false;
  if (options) {
    if (options.append) shouldAppend = true;
  }

  const RECORDS_FILEPATH = getSearchMainRecordsFilepath(prod);

  const CSV_HEADER = [
    { id: "id", title: "id" },
    { id: "product_url", title: "product_url" },
  ];

  const csvWriter = createObjectCsvWriter({
    path: RECORDS_FILEPATH,
    header: CSV_HEADER,
    append: shouldAppend,
  });

  return csvWriter;
}

export async function readSearchMainRecords(prod) {
  const RECORDS_FILEPATH = getSearchMainRecordsFilepath(prod);
  return await readCsvFile(RECORDS_FILEPATH);
}

export function getSupSimilarwebRecordsFilepath(prod) {
  let cacheKey = "file_sup_similarweb_records";
  if (prod) cacheKey += "_prod";
  else cacheKey += "_local";
  return accessCacheFolder(cacheKey);
}

export function createSupSimilarwebRecordsCsvWriter(prod, options) {
  // Handle options
  let shouldAppend = false;
  if (options) {
    if (options.append) shouldAppend = true;
  }

  const RECORDS_FILEPATH = getSupSimilarwebRecordsFilepath(prod);

  const CSV_HEADER = [
    { id: "id", title: "id" },
    { id: "source_domain", title: "source_domain" },
  ];

  const csvWriter = createObjectCsvWriter({
    path: RECORDS_FILEPATH,
    header: CSV_HEADER,
    append: shouldAppend,
  });

  return csvWriter;
}

export async function readSupSimilarwebRecords(prod) {
  const RECORDS_FILEPATH = getSupSimilarwebRecordsFilepath(prod);
  return await readCsvFile(RECORDS_FILEPATH);
}
