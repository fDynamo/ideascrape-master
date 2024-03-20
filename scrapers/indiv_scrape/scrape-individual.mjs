import puppeteer from "puppeteer-extra";
import {
  evaluateGenericPage,
  evaluateGenericPageCopy,
} from "./evaluate-functions.js";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import {
  getPercentageString,
  timeoutPromise,
} from "../../custom_helpers_js/index.js";
import registerGracefulExit from "../../custom_helpers_js/graceful-exit.js";
import { readCsvFile } from "../../custom_helpers_js/read-csv.js";
import { createRunLogger } from "../../custom_helpers_js/run-logger.mjs";
import UserAgent from "user-agents";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { getFileNameFromUrl } from "../../custom_helpers_js/string-formatters.js";
import { join } from "path";
import { writeFileSync } from "fs";
import { mkdirIfNotExists } from "../../custom_helpers_js/file-helpers.js";

const main = async () => {
  // Process input arguments
  const argv = yargs(hideBin(process.argv)).argv;
  let { outFolder, urlListFilePath, startIndex, endIndex } = argv;
  if (!outFolder || !urlListFilePath) {
    console.log("Invalid arguments");
    process.exit(1);
  }
  if (!startIndex) startIndex = 0;
  if (!endIndex) endIndex = 0;

  // Read url file
  const urlsFileContents = await readCsvFile(urlListFilePath);
  const urls = urlsFileContents.map((row) => row.url);
  let lastIndex = endIndex ? Math.min(endIndex, urls.length) : urls.length;

  // Make directories and folders
  const ESSENTIAL_DATA_FOLDER = join(outFolder, "essential_data");
  mkdirIfNotExists(ESSENTIAL_DATA_FOLDER);
  const PAGE_COPY_FOLDER = join(outFolder, "page_copy");
  mkdirIfNotExists(PAGE_COPY_FOLDER);
  const HEAD_INFO_FOLDER = join(outFolder, "head_info");
  mkdirIfNotExists(HEAD_INFO_FOLDER);

  // Create runLogger
  const dataHeaders = [
    "url",
    "title",
    "description",
    "favicon_url",
    "twitter_meta_tags",
    "og_meta_tags",
    "canonical_url",
    "other_meta_tags",
    "script_tags",
    "link_tags",
    "out_links_list",
    "comments_list",
  ];
  const runLogger = await createRunLogger(
    "indiv-scrape",
    dataHeaders,
    outFolder
  );

  // Run constants
  const NAV_TIMEOUT = 30 * 1000;
  const RUN_DELAY = 100;
  const RETRY_DELAY = 1 * 1000;
  const REFRESH_DELAY = 1 * 1000;
  const MAX_TRIES = 2;
  const MAX_SUCCESSIVE_ERRORS = 20;
  const REQUESTS_PER_REFRESH = 10;
  const JUST_A_MOMENT_DELAY = 10 * 1000;
  const DISCONNECTED_DELAY = 20 * 1000;
  const MAX_LOADING_WAIT_CYCLES = 5;

  // Error constants
  const FORCED_STOP_ERROR_STRING = "Forced stop";
  const SUCCESSIVE_ERROR_STRING = "Max successive errors reached!";
  const NAV_ERROR_SUBSTRING = " Navigation timeout of";
  const INTERNET_DISCONNECTED_ERROR_STRING = "net::ERR_INTERNET_DISCONNECTED";
  const LOADING_TIMEOUT_ERROR_STRING = "Loading for too long";

  // Variables
  let urlToScrape = "";
  let runIndex = 0;

  let countSuccessfulScrapes = 0;
  let countFailedScrapes = 0;
  let countTries = 0; // How many tries for one specific url
  let countSuccessiveErrors = 0; // How many errors in a row

  // Log start
  await runLogger.addToStartLog({
    argv: JSON.stringify(argv),
    urlListFilePath,
    countUrlsToScrape: lastIndex - startIndex,
    startIndex,
    lastIndex,
  });

  // Puppeteer initializers
  const initializeBrowser = async () => {
    return await puppeteer.launch({ headless: "new" });
  };

  const initializePage = async (browser) => {
    const page = await browser.newPage();
    const userAgent = new UserAgent().toString();
    await page.setUserAgent(userAgent);
    await page.setViewport({
      width: 1920,
      height: 1080,
      deviceScaleFactor: 1,
    });
    await page.setRequestInterception(true);
    await page.setDefaultNavigationTimeout(NAV_TIMEOUT);
    page.on("request", (request) => {
      if (request.resourceType() === "image") {
        request.abort();
      } else {
        request.continue();
      }
    });

    return page;
  };

  // Initialize browser and page
  puppeteer.use(StealthPlugin());
  let browser = await initializeBrowser();
  let page = await initializePage(browser);

  // Puppeteer functions
  const refreshBrowser = async () => {
    await browser.close();
    browser = await initializeBrowser();
    page = await initializePage(browser);

    // Write to log
    await runLogger.addToActionLog({
      message: "refreshing browser",
    });
    await timeoutPromise(REFRESH_DELAY);
  };

  // CSV writers related
  const endLogContents = {
    countUrlsToScrape: lastIndex - startIndex,
    startIndex,
    lastIndex,
  };

  // Register graceful exit
  let forcedStop = false;
  registerGracefulExit(() => {
    forcedStop = true;
    browser.close();
  });

  try {
    for (runIndex = startIndex; runIndex < lastIndex; runIndex++) {
      try {
        // Refresh if needed
        const requestsNum = runIndex - startIndex;
        if (requestsNum > 0 && requestsNum % REQUESTS_PER_REFRESH == 0) {
          await refreshBrowser();
        }

        // Handle max successive errors
        if (countSuccessiveErrors >= MAX_SUCCESSIVE_ERRORS) {
          throw new Error(SUCCESSIVE_ERROR_STRING);
        }

        urlToScrape = urls[runIndex];

        // Make request
        const requestStartedDate = new Date();
        const requestStartedStr = requestStartedDate.toISOString();

        await runLogger.addToActionLog({
          runIndex,
          urlToScrape,
          message: "start",
          requestStartedStr,
        });

        await page.goto("https://" + urlToScrape);

        await runLogger.addToActionLog({
          message: "navigated to " + urlToScrape,
        });

        let results = await page.evaluate(evaluateGenericPage);

        // Check if we are told to wait
        const isPageStillLoading = (results) => {
          if (!results.pageTitle) return true;
          const upperPageTitle = results.pageTitle.toUpperCase();
          const substringsToTest = ["JUST A MOMENT", "LOADING"];

          let toReturn = false;
          substringsToTest.forEach((substr) => {
            if (upperPageTitle.includes(substr)) toReturn = true;
          });

          return toReturn;
        };

        for (let i = 0; i < MAX_LOADING_WAIT_CYCLES; i++) {
          if (isPageStillLoading(results)) {
            if (forcedStop) {
              throw new Error(FORCED_STOP_ERROR_STRING);
            }
            await runLogger.addToActionLog({
              message: "waiting for page...",
            });
            await timeoutPromise(JUST_A_MOMENT_DELAY);
            results = await page.evaluate(evaluateGenericPage);
          } else {
            break;
          }
        }

        if (isPageStillLoading(results)) {
          throw LOADING_TIMEOUT_ERROR_STRING;
        }

        const requestEndedDate = new Date();
        const requestEndedStr = requestEndedDate.toISOString();
        const requestDurationS =
          (requestEndedDate.getTime() - requestStartedDate.getTime()) / 1000;

        const saveFileName = getFileNameFromUrl(urlToScrape);
        const essentialDataSavePath = join(
          ESSENTIAL_DATA_FOLDER,
          saveFileName + ".json"
        );
        const pageCopySavePath = join(PAGE_COPY_FOLDER, saveFileName + ".txt");
        const headInfoSavePath = join(HEAD_INFO_FOLDER, saveFileName + ".json");

        // Process results
        const toWriteEssentialData = {
          init_url: urlToScrape,
          end_url: results.locationUrl,
          title: results.pageTitle,
          description: results.pageDescription,
          page_image_url: results.faviconUrl,
        };

        const toWritePageCopy =
          "type: generic\n---\n" +
          (await page.evaluate(evaluateGenericPageCopy));

        const toWriteHeadInfo = {
          twitter_meta_tags: results.twitterMetaTags,
          og_meta_tags: results.ogMetaTags,
          canonical_url: results.canonicalUrl,
          other_meta_tags: results.otherMetaTags,
          script_tags: results.scriptTags,
          link_tags: results.linkTags,
          out_links_list: results.outLinksList,
          comments_list: results.commentsList,
        };

        // Write results
        writeFileSync(
          essentialDataSavePath,
          JSON.stringify(toWriteEssentialData, null, 4),
          { encoding: "utf-8" }
        );
        writeFileSync(pageCopySavePath, toWritePageCopy, { encoding: "utf-8" });
        writeFileSync(
          headInfoSavePath,
          JSON.stringify(toWriteHeadInfo, null, 4),
          { encoding: "utf-8" }
        );

        // Print progress
        const donePercentageString = getPercentageString(
          runIndex + 1,
          startIndex,
          lastIndex
        );

        // Write to log
        await runLogger.addToActionLog({
          successUrl: urlToScrape,
          runIndex,
          percent: donePercentageString,
          reqStartedAt: requestStartedStr,
          reqEndedAt: requestEndedStr,
          reqDurationS: requestDurationS,
        });

        // Reset variables
        countTries = 0;
        countSuccessiveErrors = 0;

        // Add to counts
        countSuccessfulScrapes += 1;

        // Handle forced stop
        if (forcedStop) {
          throw new Error(FORCED_STOP_ERROR_STRING);
        }

        await timeoutPromise(RUN_DELAY);
      } catch (error) {
        // Handle forced stop in case of target closed
        if (forcedStop) {
          throw new Error(FORCED_STOP_ERROR_STRING);
        }

        const errString = error + "";

        // Write to log
        await runLogger.addToActionLog({
          runIndex,
          urlToScrape,
          message: "error",
          error: errString,
        });

        // Decide whether to throw error or not
        const isForcedStop = errString.includes(FORCED_STOP_ERROR_STRING);
        const isSuccessiveErrorMaxReached = errString.includes(
          SUCCESSIVE_ERROR_STRING
        );
        if (isForcedStop || isSuccessiveErrorMaxReached) {
          throw error;
        }

        // Retry on nav time outs
        const isNavTimeout = errString.includes(NAV_ERROR_SUBSTRING);
        const isDisconnected = errString.includes(
          INTERNET_DISCONNECTED_ERROR_STRING
        );
        const canRetry = isNavTimeout || isDisconnected;

        countTries++;

        // If too many tries, skip to next url, might be something wrong
        if (canRetry && countTries < MAX_TRIES) {
          if (isDisconnected) {
            await timeoutPromise(DISCONNECTED_DELAY);
          }
          await refreshBrowser();

          // Write to log
          await runLogger.addToActionLog({
            runIndex,
            urlToScrape,
            message: "retrying",
            countTries,
          });

          // Get ready to retry
          runIndex--;
          await timeoutPromise(RETRY_DELAY);
        } else {
          countTries = 0;

          // Write to log
          await runLogger.addToActionLog({
            failedUrl: urlToScrape,
            runIndex,
            message: "FAILED! Skipping",
            error: errString,
          });

          await runLogger.addToFailedLog({
            url: urlToScrape,
            error: errString,
          });

          countFailedScrapes += 1;
          countSuccessiveErrors += 1;

          await timeoutPromise(RUN_DELAY);
        }

        continue;
      }
    }

    // Process ending variables
    urlToScrape = "";
    runIndex++;
    endLogContents.message = "Success";
  } catch (error) {
    endLogContents.error = "" + error;
  }

  try {
    if (browser) await browser.close();
  } catch {}

  endLogContents.lastUrlToScrape = urlToScrape;
  endLogContents.lastRunIndex = runIndex;
  endLogContents.countSuccessfulScrapes = countSuccessfulScrapes;
  endLogContents.countFailedScrapes = countFailedScrapes;

  await runLogger.addToEndLog(endLogContents);
  await runLogger.stopRunLogger();

  if (forcedStop || endLogContents.error) {
    process.exit(1);
  } else {
    process.exit();
  }
};

main();
