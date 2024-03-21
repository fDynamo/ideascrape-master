import puppeteer from "puppeteer-extra";
import path from "path";
import { evaluateTasks } from "./evaluate-functions.js";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import registerGracefulExit from "../../custom_helpers_js/graceful-exit.js";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { existsSync, readFileSync } from "fs";
import { createRunLogger } from "../../custom_helpers_js/run-logger.mjs";
import {
  getPercentageString,
  timeoutPromise,
} from "../../custom_helpers_js/index.js";
import UserAgent from "user-agents";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const main = async () => {
  // Process input arguments
  const argv = yargs(hideBin(process.argv)).argv;
  let { outFolder, presetName } = argv;
  if (!outFolder) {
    console.log("Invalid arguments");
    return;
  }

  const dataHeaders = [
    "product_url",
    "count_save",
    "image_url",
    "source_url",
    "is_verified",
    "corner_icon_url",
    "price_text",
  ];

  const runLogger = await createRunLogger(
    "aift-scrape-lists",
    dataHeaders,
    outFolder
  );

  // Run variables
  const NAV_TIMEOUT = 5 * 60 * 1000;
  const WAIT_TIMEOUT = 5 * 60 * 1000;
  const RUN_DELAY = 1500;

  // List file
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const LIST_PRESETS_FOLDER = path.join(__dirname, "list-presets");

  const presetFilename = presetName + ".json";
  const presetFilepath = path.join(LIST_PRESETS_FOLDER, presetFilename);
  const LIST_FILEPATH = presetFilepath;

  if (!LIST_FILEPATH || !existsSync(LIST_FILEPATH)) {
    console.log("Preset file not found!", LIST_FILEPATH);
    process.exit();
  }

  // Read urls file
  let instructionList = [];
  try {
    const listFileContents = readFileSync(LIST_FILEPATH, "utf-8");
    const listFileObj = JSON.parse(listFileContents);
    instructionList = listFileObj.list;
  } catch {
    console.log("Cannot read list file!", LIST_FILEPATH);
    process.exit();
  }

  await runLogger.addToStartLog({
    argv: JSON.stringify(argv),
    instructionList: JSON.stringify(instructionList),
  });

  // Puppeteer initializers
  puppeteer.use(StealthPlugin());
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
    await page.setDefaultNavigationTimeout(NAV_TIMEOUT);
    await page.setRequestInterception(true);
    const blockResourceType = [
      "beacon",
      "csp_report",
      "font",
      "image",
      "imageset",
      "media",
      "object",
      "texttrack",
      "stylesheet",
    ];
    const blockResourceName = [
      "adition",
      "adzerk",
      "analytics",
      "cdn.api.twitter",
      "clicksor",
      "clicktale",
      "doubleclick",
      "exelator",
      "facebook",
      "fontawesome",
      "google",
      "google-analytics",
      "googletagmanager",
      "mixpanel",
      "optimizely",
      "quantserve",
      "sharethrough",
      "tiqcdn",
      "zedo",
    ];

    page.on("request", (request) => {
      const requestUrl = request._url ? request._url.split("?")[0] : "";
      if (
        request.resourceType() in blockResourceType ||
        blockResourceName.some((resource) => requestUrl.includes(resource))
      ) {
        request.abort();
      } else {
        request.continue();
      }
    });
    return page;
  };

  // Variables
  let browser = await initializeBrowser();
  let page = await initializePage(browser);
  let entriesAdded = 0;

  const endLogContents = {};

  // Register graceful exit
  let forcedStop = false;
  registerGracefulExit(() => {
    forcedStop = true;
  });

  const START_INDEX = 0;
  const END_INDEX = instructionList.length;
  for (let i = START_INDEX; i < END_INDEX; i++) {
    if (forcedStop) {
      endLogContents.message = "Forced stop";
      break;
    }
    try {
      const { url, taskSelectors } = instructionList[i];
      await runLogger.addToActionLog({ startedUrl: url });

      const requestStartedDate = new Date();
      const requestStartedStr = requestStartedDate.toISOString();

      await page.goto(url);

      const scrapeResults = [];
      for (let j = 0; j < taskSelectors.length; j++) {
        const tasksSelector = taskSelectors[j];
        await page.waitForSelector(tasksSelector, { timeout: WAIT_TIMEOUT });
        const results = await page.evaluate(evaluateTasks, tasksSelector);
        results.forEach((item) => {
          scrapeResults.push(item);
        });
      }

      const requestEndedDate = new Date();
      const requestEndedStr = requestEndedDate.toISOString();
      const requestDurationS =
        (requestEndedDate.getTime() - requestStartedDate.getTime()) / 1000;

      // Process results
      const recordsToWrite = scrapeResults.map((obj) => {
        return {
          product_url: obj.sourceUrl,
          count_save: obj.countSaves,
          image_url: obj.imageUrl,
          source_url: obj.postUrl,
          is_verified: obj.isVerified,
          corner_icon_url: obj.cornerIconUrl,
          price_text: obj.priceText,
        };
      });

      // Write results
      await runLogger.addToData(recordsToWrite);

      // Print progress
      const donePercentageString = getPercentageString(
        i + 1,
        START_INDEX,
        END_INDEX
      );
      await runLogger.addToActionLog({
        finishedUrl: url,
        recordsRetrieved: recordsToWrite.length,
        percent: donePercentageString,
        reqStartedAt: requestStartedStr,
        reqEndedAt: requestEndedStr,
        reqDurationS: requestDurationS,
      });
      entriesAdded += recordsToWrite.length;

      await timeoutPromise(RUN_DELAY);
    } catch (error) {
      runLogger.addToErrorLog({
        error: error + "",
      });
      endLogContents.message = "Error";
      endLogContents.error = error + "";
      break;
    }
  }

  if (page) await page.close();
  if (browser) await browser.close();

  // Write log end
  if (!endLogContents.message) {
    endLogContents.message = "Success";
  }
  endLogContents.entriesAdded = entriesAdded;

  await runLogger.addToEndLog(endLogContents);
  await runLogger.stopRunLogger();
  process.exit();
};

main();
