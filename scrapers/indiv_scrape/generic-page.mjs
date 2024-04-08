export async function scrapeGenericPage(page) {
  await page.evaluate(evaluateScrollAllTheWayDown);
  const results = await page.evaluate(evaluatePage);
  const pageCopyResults = await page.evaluate(evaluatePageCopy);

  // Process results
  const essentialData = {
    title: results.pageTitle,
    description: results.pageDescription,
    page_image_url: results.faviconUrl,
  };

  const pageCopy = "type: generic\n---\n" + pageCopyResults;

  const headInfo = {
    twitter_meta_tags: results.twitterMetaTags,
    og_meta_tags: results.ogMetaTags,
    canonical_url: results.canonicalUrl,
    other_meta_tags: results.otherMetaTags,
    script_tags: results.scriptTags,
    link_tags: results.linkTags,
    out_links_list: results.outLinksList,
    comments_list: results.commentsList,
  };

  return {
    essentialData,
    pageCopy,
    headInfo,
  };
}

const evaluatePage = () => {
  // Helper functions
  function getAllComments(node) {
    if (node.nodeType === Node.COMMENT_NODE) {
      const nodeValue = node.nodeValue;
      const MAX_VALUE_LENGTH = 80;
      if (nodeValue.length > MAX_VALUE_LENGTH) {
        return [];
      }
      return [node.nodeValue];
    }

    if (node.childNodes) {
      const toReturn = [];
      for (var i = 0; i < node.childNodes.length; i++) {
        const res = getAllComments(node.childNodes[i]);
        res.forEach((el) => {
          if (el) toReturn.push(el);
        });
      }
      return toReturn;
    } else return [];
  }

  // Start
  const titleEl = document.querySelector("head title");
  let pageTitle = "";
  if (titleEl) {
    pageTitle = titleEl.innerText;
  }

  const linkElList = document.querySelectorAll("head link");
  let canonicalUrl = "";
  let faviconUrl = "";
  const linkTags = [];

  // Link tags
  linkElList.forEach((linkEl) => {
    const rel = linkEl.getAttribute("rel");
    const href = linkEl.href;
    if (rel == "canonical") {
      canonicalUrl = href;
    }

    // TODO: Store or choose best image url
    if (rel == "shortcut icon" || rel == "icon" || rel == "icon shortcut") {
      faviconUrl = href;
    }

    linkTags.push({ rel, href });
  });

  // Meta tags
  const metaElList = document.querySelectorAll("head meta");
  let pageDescription = "";
  let contentLanguage = "";
  let contentType = "";
  let ogUrl = "";
  let ogLocale = "";
  const twitterMetaTags = [];
  const ogMetaTags = [];
  const otherMetaTags = [];
  metaElList.forEach((metaEl) => {
    const metaName = metaEl.getAttribute("name");
    const metaProperty = metaEl.getAttribute("property");
    const metaContent = metaEl.getAttribute("content");
    const metaHttpEquiv = metaEl.getAttribute("http-equiv");

    const metaInfo = {
      metaName,
      metaProperty,
      metaContent,
      metaHttpEquiv,
    };

    if (metaName == "description") {
      pageDescription = metaContent;
      return;
    }

    if (metaHttpEquiv == "Content-Language") {
      contentLanguage = metaContent;
      return;
    }

    if (metaHttpEquiv == "Content-Type") {
      contentType = metaContent;
      return;
    }

    if (metaProperty && metaProperty.startsWith("og:")) {
      if (metaProperty.includes("locale")) {
        ogLocale = metaContent;
      }

      if (metaProperty.includes("url")) {
        ogUrl = metaContent;
      }

      ogMetaTags.push(metaInfo);
      return;
    }

    if (metaName && metaName.startsWith("twitter:")) {
      twitterMetaTags.push(metaInfo);
      return;
    }

    otherMetaTags.push(metaInfo);
  });

  // Script tags
  const scriptTags = [];
  const scriptTagList = document.querySelectorAll("head script");
  scriptTagList.forEach((tagEl) => {
    const src = tagEl.getAttribute("src");
    const scriptType = tagEl.getAttribute("type");
    if (src || scriptType) {
      scriptTags.push({ src, scriptType });
    }
  });

  // anchor tags
  const outLinksList = [];
  const anchorTagsList = document.querySelectorAll("a");
  anchorTagsList.forEach((aTag) => {
    const href = aTag.href;
    if (href && !outLinksList.includes(href)) {
      outLinksList.push(href);
    }
  });

  const commentsList = [...new Set(getAllComments(document))];

  const pageCopy = document.body.innerText;

  const locationUrl = window.location.toString();
  return {
    locationUrl,
    pageTitle,
    canonicalUrl,
    pageDescription,
    contentLanguage,
    contentType,
    ogUrl,
    ogLocale,
    twitterMetaTags,
    ogMetaTags,
    otherMetaTags,
    faviconUrl,
    scriptTags,
    linkTags,
    outLinksList,
    commentsList,
    pageCopy,
  };
};

const evaluatePageCopy = () => {
  return document.body.innerHTML.toString();
};

const evaluateScrollAllTheWayDown = async () => {
  const distance = 100;
  const delay = 100;
  const MAX_SCROLL_CYCLES = 30;

  for (let i = 0; i < MAX_SCROLL_CYCLES; i++) {
    const keepGoing =
      document.scrollingElement.scrollTop + window.innerHeight <
      document.scrollingElement.scrollHeight;

    if (!keepGoing) break;

    document.scrollingElement.scrollBy(0, distance);
    await new Promise((resolve) => {
      setTimeout(resolve, delay);
    });
  }

  return true;
};
