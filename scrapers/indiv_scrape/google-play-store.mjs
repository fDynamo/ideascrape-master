export async function scrapeGooglePlayStore(page) {
  const essentialData = await page.evaluate(evaluateEssentialData);
  const pageCopyResults = await page.evaluate(evaluatePageCopy);
  const pageCopy = "type: google_play_store\n---\n" + pageCopyResults;

  return {
    essentialData,
    pageCopy,
    headInfo: null,
  };
}

function evaluateEssentialData() {
  const titleEl = document.querySelector("head title");
  let title = "";
  if (titleEl) {
    title = titleEl.innerText;
  }

  let description = "";
  const metaDescriptionEl = document.querySelector('meta[name="description"]');
  if (metaDescriptionEl) {
    description += metaDescriptionEl.content.trim();
  }

  const pageDescriptionEl = document.querySelector(
    'div[data-g-id="description"]'
  );
  if (pageDescriptionEl) {
    description += " " + pageDescriptionEl.innerText.trim();
  }
  description = description.trim();

  let imageUrl = "";
  const imageEl = document.querySelector('img[alt="Icon image"]');
  if (imageEl) {
    imageUrl = imageEl.src;
  }

  return {
    title,
    description,
    page_image_url: imageUrl,
  };
}

function evaluatePageCopy() {
  const metricsElList = document.querySelectorAll("div.w7Iutd>div");
  let countDownloads = "";
  for (let i = 0; i < metricsElList.length; i++) {
    const currEl = metricsElList[i];
    const topVal = currEl.firstChild.innerText;
    const botVal = currEl.lastChild.innerText;
    if (botVal.toLowerCase() == "downloads") {
      countDownloads = topVal;
    }
  }

  const updatedEl = document.querySelector("div.xg1aie");
  let updatedAt = "";
  if (updatedEl) {
    updatedAt = updatedEl.innerText;
  }

  return JSON.stringify(
    {
      countDownloads,
      updatedAt,
    },
    null,
    3
  );
}
