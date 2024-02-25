import { batchData } from "./index.js";

export const MAX_FETCH_BATCH_SIZE = 1000;
export const MAX_DELETE_BATCH_SIZE = 10000;
export const MAX_UPDATE_BATCH_SIZE = 10000;

export async function batchActionIn(supabaseObj, otherArgs) {
  const {
    action,
    tableName,
    selectCols,
    inColName,
    inList,
    batchSize,
    updateVal,
    shouldLog,
  } = otherArgs;

  const ACCEPTED_ACTIONS = {
    fetch: "fetch",
    delete: "delete",
    update: "update",
  };

  if (!ACCEPTED_ACTIONS[action]) {
    return { error: { errorObj: "Invalid action" } };
  }

  const batchedInList = batchData(inList, batchSize);

  if (shouldLog) {
    console.log("Num batches to " + action, batchedInList.length);
  }

  let toReturn = [];
  for (let i = 0; i < batchedInList.length; i++) {
    if (shouldLog) {
      console.log("Starting batch to " + action, i);
    }
    const batch = batchedInList[i];
    let res = null;
    if (action == ACCEPTED_ACTIONS.fetch) {
      res = await supabaseObj
        .from(tableName)
        .select(selectCols)
        .in(inColName, batch);
    } else if (action == ACCEPTED_ACTIONS.delete) {
      res = await supabaseObj
        .from(tableName)
        .delete(selectCols)
        .in(inColName, batch)
        .select(selectCols);
    } else if (action == ACCEPTED_ACTIONS.update) {
      if (!updateVal) {
        return {
          error: {
            errorObj: "No update val provided",
          },
        };
      }
      res = await supabaseObj
        .from(tableName)
        .update(updateVal)
        .in(inColName, batch)
        .select(selectCols);
    }

    const { data, error } = res;
    if (error) {
      return {
        error: {
          batchIndex: i,
          batch,
          errorObj: error,
        },
      };
    }

    if (shouldLog) {
      console.log("Finished batch to " + action, i);
    }
    toReturn = [...toReturn, ...data];
  }

  return { data: toReturn };
}
