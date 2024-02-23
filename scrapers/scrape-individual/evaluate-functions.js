const { getAllComments } = require("../../custom_helpers_js/scraping-helpers");

const evaluateGenericPage = async () => {
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
    const href = linkEl.getAttribute("href");
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
    outLinksList.push(href);
  });

  const commentsList = getAllComments(document);

  const pageCopy = document.body.innerText;

  return {
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

module.exports = {
  evaluateGenericPage,
};
