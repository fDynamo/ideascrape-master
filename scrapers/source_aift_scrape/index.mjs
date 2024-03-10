import path from "path";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { exec } from "child_process";
import { fileURLToPath } from "url";
import { mkdirIfNotExists } from "../../custom_helpers_js/file-helpers.js";
import registerGracefulExit from "../../custom_helpers_js/graceful-exit.js";

function promisedExec(in_cmd) {
  const childProcess = exec(in_cmd);

  childProcess.stdout.on("data", (data) => {
    console.log(data);
  });

  childProcess.stderr.on("data", (data) => {
    console.error(data);
  });

  const resolvePromise = new Promise((res) => {
    childProcess.on("close", (code) => {
      res(code);
    });
  });

  return [childProcess, resolvePromise];
}

const main = async () => {
  // Process input arguments
  const argv = yargs(hideBin(process.argv)).argv;
  let { outFolder } = argv;
  if (!outFolder) {
    console.log("Invalid arguments");
    return;
  }

  const outListsFolder = path.join(outFolder, "lists");
  const outPostsFolder = path.join(outFolder, "posts");
  const urlsListFilepath = path.join(outListsFolder, "urls_list.json");

  mkdirIfNotExists(outListsFolder);
  mkdirIfNotExists(outPostsFolder);

  const __dirname = path.dirname(fileURLToPath(import.meta.url));

  const processesList = [];
  registerGracefulExit(async () => {
    /**
     * NOTE: This does not work on windows for some reason
     * So no graceful exit for now lmao, hope everything goes well
     */
    for (let i = 0; i < processesList.length; i++) {
      const childProcess = processesList[i];
      childProcess.kill("SIGINT");
    }

    process.exit();
  });

  // Run scrape front page
  const scrapeListsFilepath = path.join(__dirname, "scrape-lists.mjs");
  const [scrapeListProcess, scrapeListPromise] = promisedExec(
    `node "${scrapeListsFilepath}" --outFolder "${outListsFolder}" --presetName front-page`
  );

  processesList.push(scrapeListProcess);
  await scrapeListPromise;
  processesList.splice(0, 1);

  // Run extract urls
  const extractPostUrlsFilepath = path.join(__dirname, "extract-post-urls.mjs");
  const [extractPostUrlsProcess, extractPostUrlsPromise] = promisedExec(
    `node "${extractPostUrlsFilepath}" --outFile "${urlsListFilepath}" --listsFolder "${outListsFolder}"`
  );

  processesList.push(extractPostUrlsProcess);
  await extractPostUrlsPromise;
  processesList.splice(0, 1);

  // Run scrape posts
  const scrapePostsFilepath = path.join(__dirname, "scrape-posts.mjs");
  const [scrapePostsProcess, scrapePostsPromise] = promisedExec(
    `node "${scrapePostsFilepath}" --outFolder "${outPostsFolder}" --urlsFilepath "${urlsListFilepath}" --extractType all`
  );

  processesList.push(scrapePostsProcess);
  await scrapePostsPromise;
  processesList.splice(0, 1);

  process.exit();
};

main();
