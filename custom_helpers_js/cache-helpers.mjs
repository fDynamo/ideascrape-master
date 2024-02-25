import {
  createSupSimilarwebRecordsCsvWriter,
  readSupSimilarwebRecords,
  createSearchMainRecordsCsvWriter,
  readSearchMainRecords,
} from "./cache-folder-helpers.mjs";

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

export async function appendAndFixSearchMainRecordsCache(prod, newRecords) {
  let recordsList = await readSearchMainRecords(prod);
  recordsList = [...recordsList, ...newRecords];

  const dupeSet = {};
  const finalList = [];

  for (let i = 0; i < recordsList.length; i++) {
    const record = recordsList[i];
    if (!record.product_url) {
      continue;
    }
    if (dupeSet[record.id]) {
      continue;
    }
    dupeSet[record.id] = true;
    finalList.push(record);
  }

  const finalCsvWriter = createSearchMainRecordsCsvWriter(prod, {
    append: false,
  });

  await finalCsvWriter.writeRecords(finalList);
}
