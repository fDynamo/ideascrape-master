import {
  timeoutPromise,
  getPercentageString,
} from "../../custom_helpers_js/index.js";
import { queryPH } from "./graphql-query.js";
import { createRunLogger } from "../../custom_helpers_js/run-logger.mjs";
import registerGracefulExit from "../../custom_helpers_js/graceful-exit.js";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { cleanTextForCsv } from "../../custom_helpers_js/string-formatters.js";

const main = async () => {
  // Process input arguments
  const argv = yargs(hideBin(process.argv)).argv;
  let { outFolder, startIndex, endIndex } = argv;
  if (!outFolder) {
    console.log("Invalid arguments");
    return;
  }
  if (!startIndex) startIndex = 0;
  if (!endIndex) endIndex = 14;

  const dataHeaders = [
    "product_url",
    "count_follower",
    "image_url",
    "listed_at",
    "updated_at",
    "source_url",
    "tagline_text",
    "count_post",
    "count_review",
    "count_reviewer",
    "review_rating",
    "description",
  ];
  const runLogger = await createRunLogger(
    "ph-scrape-homefeed",
    dataHeaders,
    outFolder
  );

  // Run variables
  const RUN_DELAY = 1500;
  const RETRY_DELAY = 5000;
  const MAX_RETRIES = 5;

  // Error strings
  const ERR_STRING_NO_ITEMS = "No items retrieved!";
  const ERR_STRING_NO_NEXT_PAGE = "No next page found";
  const ERR_STRING_MAX_RETRIES_REACHED = "Max retries reached!";

  // Start and end cursor defaults
  let START_CURSOR = startIndex;
  let ENDING_CURSOR = endIndex;

  // Register graceful exit
  let forcedStop = false;
  registerGracefulExit(() => {
    forcedStop = true;
  });

  // Other variables
  let cursor = 0;
  let entriesAdded = 0;
  let countRetries = 0;

  const endLogContents = {};

  await runLogger.addToStartLog({
    argv: JSON.stringify(argv),
    startCursor: START_CURSOR,
    endCursor: ENDING_CURSOR,
  });

  for (cursor = START_CURSOR; cursor < ENDING_CURSOR; cursor++) {
    try {
      await runLogger.addToActionLog({ startedCursor: cursor });
      const requestStartedDate = new Date();
      const requestStartedStr = requestStartedDate.toISOString();

      const queryRes = await queryPH(cursor);

      const requestEndedDate = new Date();
      const requestEndedStr = requestEndedDate.toISOString();
      const requestDurationS =
        (requestEndedDate.getTime() - requestStartedDate.getTime()) / 1000;

      const { pageInfo, edges } = queryRes.data.homefeed;
      const { date: nodeDate, items } = edges[0].node;

      if (!items.length) {
        throw new Error(ERR_STRING_NO_ITEMS);
      }

      const recordsToWrite = [];

      items.forEach((obj) => {
        if (!obj.product) return;
        const { product, thumbnailImageUuid } = obj;

        let image_url = "";
        let listed_at = "";
        let updated_at = "";
        let count_follower = 0;
        let source_url = "";
        let tagline_text = "";
        let count_post = 0;
        let count_review = 0;
        let count_reviewer = 0;
        let review_rating = 0;
        let description = "";

        image_url = thumbnailImageUuid
          ? "https://ph-files.imgix.net/" + thumbnailImageUuid
          : "";
        listed_at = product.structuredData.datePublished;
        updated_at = product.structuredData.dateModified;
        count_follower = product.followersCount;
        source_url = product.url;
        tagline_text = product.tagline;
        count_post = product.postsCount;
        count_review = product.reviewsCount;
        count_reviewer = product.reviewersCount;
        review_rating = product.reviewsRating;
        description = cleanTextForCsv(product.description);

        recordsToWrite.push({
          product_url: product.websiteUrl,
          image_url,
          listed_at,
          updated_at,
          count_follower,
          source_url,
          tagline_text,
          count_post,
          count_review,
          count_reviewer,
          review_rating,
          description,
        });
      });

      await runLogger.addToData(recordsToWrite);

      // Print progress
      const donePercentageString = getPercentageString(
        cursor + 1,
        START_CURSOR,
        ENDING_CURSOR
      );
      await runLogger.addToActionLog({
        finishedCursor: cursor,
        recordsRetrieved: recordsToWrite.length,
        nodeDate,
        percent: donePercentageString,
        reqStartedAt: requestStartedStr,
        reqEndedAt: requestEndedStr,
        reqDurationS: requestDurationS,
      });

      entriesAdded += recordsToWrite.length;

      if (!pageInfo.hasNextPage) {
        throw new Error(ERR_STRING_NO_NEXT_PAGE);
      }

      countRetries = 0;
    } catch (error) {
      const errStr = error + "";

      await runLogger.addToErrorLog({ error: errStr });

      if (errStr == ERR_STRING_NO_ITEMS || errStr == ERR_STRING_NO_NEXT_PAGE) {
        endLogContents.message = "Error";
        endLogContents.error = errStr;
        break;
      }

      // Check if we can retry
      if (countRetries < MAX_RETRIES) {
        countRetries += 1;
        cursor--;
        await timeoutPromise(RETRY_DELAY);
      } else {
        endLogContents.message = "Error";
        endLogContents.error = ERR_STRING_MAX_RETRIES_REACHED;
        break;
      }
    }

    if (forcedStop) {
      endLogContents.message = "Forced stop";
      break;
    }

    if (cursor >= ENDING_CURSOR) {
      endLogContents.message = "Met ending cursor";
      break;
    }

    await timeoutPromise(RUN_DELAY);
  }

  // Write log end
  endLogContents.endCursor = cursor;
  endLogContents.entriesAdded = entriesAdded;

  await runLogger.addToEndLog(endLogContents);
  await runLogger.stopRunLogger();

  if (forcedStop || endLogContents.error) {
    process.exit(1);
  }

  process.exit();
};

main();
