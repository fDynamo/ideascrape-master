"use client";

import { ChangeEvent, KeyboardEvent, useEffect, useRef, useState } from "react";
import styles from "./page.module.scss";
import {
  SearchResultExportObj,
  SearchResultObj,
} from "@/utilities/customTypes";
import { sendAPISearchRequest } from "@/utilities/useAPI";
import useSessionStorage from "./hooks/useSessionStorage";
import SafeImage from "@/components/SafeImage";
import ModalBase from "@/components/ModalBase";
import { FaArrowUp } from "react-icons/fa";

const socialFilters: { label: string; value: string }[] = [
  { label: "facebook", value: "facebook" },
  { label: "X / Twitter", value: "twitter" },
  { label: "instagram", value: "instagram" },
  { label: "TikTok", value: "tiktok" },
  { label: "YouTube", value: "youtube" },
  { label: "discord", value: "discord" },
  { label: "Reddit", value: "reddit" },
  { label: "LinkedIn", value: "linkedin" },
];

const SM_IMG_MAP: { [smStr: string]: string } = {
  email: "/si_email.png",
  facebook: "/si_facebook.png",
  discord: "/si_discord.png",
  youtube: "/si_youtube.png",
  tiktok: "/si_tiktok.png",
  instagram: "/si_instagram.png",
  twitter: "/si_twitter.png",
  reddit: "/si_reddit.png",
  linkedin: "/si_linkedin.png",
};

const INITIAL_PAGE_NUM = 1;
const PAGE_SIZE = 50;
const MAX_PAGE_NUM = 5;
export default function Home() {
  // search settings
  const [smList, setSmList] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchSortType, setSearchSortType] = useState("recent");
  const hasChangedSortType = useRef(false);
  const [searchConcatType, setSearchConcatType] = useState<"AND" | "OR">("AND");
  const [searchQueryType, setSearchQueryType] = useState<"auto" | "vs" | "ft">(
    "auto"
  );
  const [vsRelevanceCap, setVsRelevanceCap] = useState(1);

  // Other states
  const [searchResults, setSearchResults] = useState<SearchResultObj[]>([]);
  const [isLoadingSearch, setIsLoadingSearch] = useState(false);
  const [isAdvancedOptionsOpen, setIsAdvancedOptionsOpen] = useState(false);
  const [isExportOptionsOpen, setIsExportOptionsOpen] = useState(false);
  const [isModifiedSearch, setIsModifiedSearch] = useState(false);
  const [hasMoreResults, setHasMoreResults] = useState(true);
  const [hasSearchedOnce, setHasSearchedOnce] = useState(false);
  const [pageNum, setPageNum] = useState(INITIAL_PAGE_NUM);
  const [selectedResults, setSelectedResults] = useState<SearchResultObj[]>([]);
  const [showScrollUpWidget, setShowScrollUpWidget] = useState(false);
  const [
    lastQueryEmbeddings,
    setLastQueryEmbeddings,
    isLastQueryEmbeddingsLoaded,
    clearLastQueryEmbeddings,
  ] = useSessionStorage<string>("last-query-embeddings", "");

  // Effects
  useEffect(() => {
    window.addEventListener("scroll", handleScroll);

    return () => {
      window.removeEventListener("scroll", handleScroll);
    };
  }, []);

  useEffect(() => {
    setIsModifiedSearch(true);
    setPageNum(INITIAL_PAGE_NUM);
    setHasMoreResults(true);
  }, [
    searchQuery,
    searchSortType,
    searchConcatType,
    searchQueryType,
    smList,
    vsRelevanceCap,
  ]);

  // Special functions

  // Executes search based on search param states
  const executeSearch = async (overrides?: any, options?: any) => {
    if (isLoadingSearch) return;

    // TODO: Validate
    setIsLoadingSearch(true);

    const isModified = isModifiedSearch;
    if (!isModified && !hasMoreResults && !overrides) return;

    // Handle overrides
    let fSortType = searchSortType;
    if (overrides?.searchSortType) {
      fSortType = overrides.searchSortType;
    }

    let fPageNum = INITIAL_PAGE_NUM;
    if (overrides?.pageNum) {
      fPageNum = overrides.pageNum;
    }

    let payload: any = {
      sm_list: smList,
      concat_type: searchConcatType, // TODO
      sorted_by: fSortType,
      pagination: {
        pageSize: PAGE_SIZE,
        page: fPageNum,
      },
      options: {
        vector_search_relevance_cap: vsRelevanceCap,
      },
    };

    // Determine search query type
    let fSearchQueryType = searchQueryType;
    if (fSearchQueryType == "auto") {
      if (searchQuery.split(" ").length > 3) {
        fSearchQueryType = "vs";
      } else {
        fSearchQueryType = "ft";
      }
    }

    if (fSearchQueryType == "vs" && lastQueryEmbeddings) {
      payload = {
        ...payload,
        search_query_type: fSearchQueryType,
        search_query_embedding: lastQueryEmbeddings,
      };
    } else if (searchQuery) {
      payload = {
        ...payload,
        search_query_type: fSearchQueryType,
        search_query: searchQuery,
      };
    }

    const res = await sendAPISearchRequest(payload);

    setIsLoadingSearch(false);
    setHasSearchedOnce(true);

    if (isModified && !options?.keepSelected) {
      setSelectedResults([]);
    }

    if (res.isError) {
      // TODO
      window.alert("Something went wrong, please try again later");
      console.error(res.error);
      return;
    }

    setIsModifiedSearch(false);
    const newResults = res.data.results;

    if (isModified || options?.reset) setSearchResults(newResults);
    else
      setSearchResults((curr) => {
        const toReturn = [...curr];
        newResults.forEach((obj) => {
          const idx = toReturn.findIndex(
            (val) => val.product_url == obj.product_url
          );
          if (idx === -1) {
            toReturn.push(obj);
          }
        });
        return toReturn;
      });

    if (res.data.extra?.generated_query_vector) {
      setLastQueryEmbeddings(res.data.extra?.generated_query_vector);
    }

    if (newResults.length < PAGE_SIZE) {
      setHasMoreResults(false);
    }
  };

  const exportResults = (exportType: string) => {
    let toExport: SearchResultObj[] = [];
    if (exportType == "all") {
      toExport = searchResults;
    } else if (exportType == "selected") {
      toExport = selectedResults;
    }

    // Transform objects
    const exportData = toExport.map((resObj) => {
      const toAdd: SearchResultExportObj = {
        product_url: resObj.product_url,
        product_name: resObj.product_name,
        product_description: resObj.product_description,
        listed_at: resObj.ph_listed_at,
        sm_email: resObj.sm_email,
        sm_instagram: resObj.sm_instagram,
        sm_facebook: resObj.sm_facebook,
        sm_twitter: resObj.sm_twitter,
        sm_discord: resObj.sm_discord,
        sm_linkedin: resObj.sm_linkedin,
        sm_youtube: resObj.sm_youtube,
        sm_tiktok: resObj.sm_tiktok,
        sm_reddit: resObj.sm_reddit,
        popularity: resObj.popularity,
      };

      return toAdd;
    });

    let stringToSave = Object.keys(exportData[0]).join(",") + "\n";
    stringToSave += exportData
      .map((resObj) => {
        const valList = Object.values(resObj);
        return valList
          .map((val) => {
            if (typeof val == "string") {
              val = val.replaceAll(",", ",");
              val = val.replaceAll('"', '""');
              val = '"' + val + '"';
            }
            return val;
          })
          .join(",");
      })
      .join("\n");

    var element = document.createElement("a");
    element.setAttribute(
      "href",
      "data:text/csv;charset=utf-8," + encodeURIComponent(stringToSave)
    );
    element.setAttribute("download", "search_results.csv");

    element.style.display = "none";
    document.body.appendChild(element);

    element.click();

    document.body.removeChild(element);
  };

  // Event handlers
  const handleScroll = (e: Event) => {
    const scrollY = window.scrollY;
    if (scrollY > 200) setShowScrollUpWidget(true);
    else {
      setShowScrollUpWidget(false);
    }
  };

  const handleSearchClick = async () => {
    executeSearch();
  };

  const handleSortChange = (e: ChangeEvent<HTMLSelectElement>) => {
    if (!hasChangedSortType.current) {
      hasChangedSortType.current = true;
    }

    const newVal = e.target.value;
    setSearchSortType(newVal);
    if (searchResults.length) {
      executeSearch({ searchSortType: newVal, pageNum: 0 }, { reset: true });
    }
  };

  const handleChangeSearchInput = (e: ChangeEvent<HTMLInputElement>) => {
    const newVal = e.target.value;
    if (!newVal && searchSortType == "relevant") {
      setSearchSortType("recent");
    }
    if (newVal && !hasChangedSortType.current) {
      setSearchSortType("relevant");
    }
    clearLastQueryEmbeddings();

    setSearchQuery(newVal);
  };

  const handleKeyDownSearchInput = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key == "Enter") {
      if (searchQuery) executeSearch();
      e.preventDefault();
    }
  };

  const handleClickSocialControl = (socialVal: string) => {
    setSmList((curr) => {
      const newVal = [...curr];
      const idx = newVal.indexOf(socialVal);
      if (idx > -1) {
        newVal.splice(idx, 1);
      } else {
        newVal.push(socialVal);
      }
      return newVal;
    });
  };

  const openAdvancedOptions = () => {
    setIsAdvancedOptionsOpen(true);
  };

  const closeAdvancedOptions = () => {
    setIsAdvancedOptionsOpen(false);
  };

  const handleSelectSearchConcatTypeChange = (
    e: ChangeEvent<HTMLSelectElement>
  ) => {
    const newVal = e.target.value;
    setSearchConcatType(newVal as "AND" | "OR");
  };

  const handleSelectSearchQueryTypeChange = (
    e: ChangeEvent<HTMLSelectElement>
  ) => {
    const newVal = e.target.value;
    setSearchQueryType(newVal as "auto" | "ft" | "vs");
  };

  const handleClickExport = () => {
    setIsExportOptionsOpen(true);
  };

  const handleClickMoreResults = () => {
    let newPageNum = pageNum + 1;

    setPageNum(newPageNum);
    executeSearch({ pageNum: newPageNum }, { keepSelected: true });
  };

  const handleResultSelect = (selectedObj: SearchResultObj) => {
    setSelectedResults((curr) => {
      const newList = [...curr];
      const tmpIdx = newList.findIndex(
        (e) => e.product_url == selectedObj.product_url
      );
      if (tmpIdx == -1) {
        newList.push(selectedObj);
      } else {
        newList.splice(tmpIdx, 1);
      }
      return newList;
    });
  };

  const handleClickScrollUp = () => {
    window.scrollTo(0, 0);
  };

  // Render functions
  const renderResultSmList = (searchObj: SearchResultObj) => {
    const resSmList: { sm: string; url: string }[] = [];

    if (searchObj.sm_email) {
      resSmList.push({ sm: "email", url: "mailto:" + searchObj.sm_email });
    }
    if (searchObj.sm_twitter) {
      resSmList.push({ sm: "twitter", url: searchObj.sm_twitter });
    }
    if (searchObj.sm_instagram) {
      resSmList.push({ sm: "instagram", url: searchObj.sm_instagram });
    }
    if (searchObj.sm_tiktok) {
      resSmList.push({ sm: "tiktok", url: searchObj.sm_tiktok });
    }
    if (searchObj.sm_youtube) {
      resSmList.push({ sm: "youtube", url: searchObj.sm_youtube });
    }
    if (searchObj.sm_discord) {
      resSmList.push({ sm: "discord", url: searchObj.sm_discord });
    }
    if (searchObj.sm_facebook) {
      resSmList.push({ sm: "facebook", url: searchObj.sm_facebook });
    }
    if (searchObj.sm_reddit) {
      resSmList.push({ sm: "reddit", url: searchObj.sm_reddit });
    }
    if (searchObj.sm_linkedin) {
      resSmList.push({ sm: "linkedin", url: searchObj.sm_linkedin });
    }

    return resSmList.map((smObj) => {
      const { url, sm } = smObj;
      const fUrl = url;
      return (
        <a
          key={searchObj.product_url + "-sm-" + sm}
          href={fUrl}
          target="_blank"
          className={styles["result-block__sm-icon-link"]}
        >
          <img src={SM_IMG_MAP[sm]} alt="" />
        </a>
      );
    });
  };

  const getDateString = (inDateStr: string) => {
    const dateObj = new Date(inDateStr);

    return Intl.DateTimeFormat("en", {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }).format(dateObj);
  };

  const renderResults = () => {
    return searchResults.map((searchObj) => {
      const popularityString = searchObj.popularity;
      let popularityClassName = "low";
      if (popularityString.includes("medium")) {
        popularityClassName = "mid";
      }
      if (
        popularityString.includes("high") ||
        popularityString.includes("excellent")
      ) {
        popularityClassName = "high";
      }

      let imgUrl = "";
      if (searchObj.product_image_file_name) {
        imgUrl =
          process.env.NEXT_PUBLIC_IMAGE_BUCKET_URL +
          searchObj.product_image_file_name;
      }

      let description = searchObj.product_description;
      const MAX_DESCRIPTION_LENGTH = 450;
      if (description.length > MAX_DESCRIPTION_LENGTH) {
        description = description.slice(0, MAX_DESCRIPTION_LENGTH - 3) + "...";
      }

      const isSelected = !!selectedResults.find(
        (e) => e.product_url === searchObj.product_url
      );

      return (
        <div className={styles["result-block"]} key={searchObj.product_url}>
          <div className={styles["result-block__head"]}>
            <a
              className={styles["result-block__head-link"]}
              href={"https://" + searchObj.product_url}
              target="_blank"
            >
              <SafeImage
                src={imgUrl}
                alt=""
                className={styles["result-block__img"]}
              />
            </a>
            <a
              className={styles["result-block__head-link"]}
              href={"https://" + searchObj.product_url}
              target="_blank"
            >
              <div className={styles["result-block__head-copy"]}>
                <span className={styles["result-block__url-text"]}>
                  {searchObj.product_url}
                </span>
                <span className={styles["result-block__name"]}>
                  {searchObj.product_name}
                </span>
              </div>
            </a>
            <div className={styles["result-block__head-select-container"]}>
              <input
                type="checkbox"
                checked={isSelected}
                readOnly
                onClick={() => handleResultSelect(searchObj)}
              />
            </div>
          </div>
          <p className={styles["result-block__description"]}>{description}</p>
          <div className={styles["result-block__info-container"]}>
            <p className={styles["result-block__date"]}>
              {getDateString(searchObj.ph_listed_at)}
            </p>
            <p
              className={
                styles["result-block__popularity"] +
                " " +
                styles[popularityClassName]
              }
            >
              <span className={styles["result-block__popularity-label"]}>
                popularity:
              </span>{" "}
              {popularityString}
            </p>
          </div>
          <div className={styles["result-block__sm-list"]}>
            {renderResultSmList(searchObj)}
          </div>
        </div>
      );
    });
  };

  const renderSocialControls = () => {
    return socialFilters.map((obj) => {
      const imgFilePath = SM_IMG_MAP[obj.value];
      return (
        <div
          key={obj.value}
          className={styles["social__control"]}
          onClick={() => handleClickSocialControl(obj.value)}
        >
          <input
            type="checkbox"
            checked={smList.includes(obj.value)}
            readOnly
          />
          <img src={imgFilePath} alt="" />
          <span>{obj.label}</span>
        </div>
      );
    });
  };

  const renderScrollUpWidget = () => {
    if (!showScrollUpWidget) return null;
    return (
      <div className={styles["scroll-up-widget"]}>
        <button onClick={handleClickScrollUp}>
          <FaArrowUp size={24} />
        </button>
      </div>
    );
  };

  // Modal renders
  const modalAdvancedOptions = () => (
    <ModalBase
      title="Advanced options"
      onClose={closeAdvancedOptions}
      closeOnBgClick
      isOpen={isAdvancedOptionsOpen}
    >
      <>
        <div className={styles["advanced-options__config"]}>
          <select
            onChange={handleSelectSearchConcatTypeChange}
            value={searchConcatType}
          >
            <option value="AND">all</option>
            <option value="OR">at least one</option>
          </select>
          <span>
            Businesses must have ___ of the selected social media accounts
          </span>
          <select
            onChange={handleSelectSearchQueryTypeChange}
            value={searchQueryType}
          >
            <option value="auto">auto</option>
            <option value="ft">full text / keywords</option>
            <option value="vs">vector semantic</option>
          </select>
          <span>Method to use for product description search queries</span>
          <input
            type="number"
            value={vsRelevanceCap}
            onChange={(e) => {
              let newVal = parseFloat(e.target.value);
              setVsRelevanceCap(newVal);
            }}
          />
          <span>Vector semantic relevance cap</span>
        </div>
        <p className={styles["advanced-options__disclaimer-text"]}>
          All changes are auto-saved
        </p>
      </>
    </ModalBase>
  );

  const modalExportOptions = () => (
    <ModalBase
      title="Export"
      onClose={() => setIsExportOptionsOpen(false)}
      closeOnBgClick
      isOpen={isExportOptionsOpen}
      containerClassName={styles["export-content"]}
    >
      <>
        <span>Exports results to a .csv file</span>
        <div className={styles["export-buttons"]}>
          <button onClick={() => exportResults("all")}>export all</button>
          {!!selectedResults.length && (
            <button onClick={() => exportResults("selected")}>
              export selected
            </button>
          )}
        </div>
      </>
    </ModalBase>
  );

  // Main render
  return (
    <>
      {modalAdvancedOptions()}
      {modalExportOptions()}
      {renderScrollUpWidget()}
      <div className={styles["mobile-gate"]}>
        hackersearch is only available on desktop
      </div>
      <header className={styles.header}>
        <nav className={styles.nav}>
          <div className={styles["nav__container"]}>
            <a href="mailto:fbdlabs@outlook.com">contact</a>
            <a href="https://insigh.to/b/hackersearch" target="_blank">
              suggest a feature
            </a>
            <a href="https://test.com">survey</a>
            <button className={styles["sign-in-button"]}>sign in</button>
          </div>
        </nav>
      </header>
      <main className={styles.main}>
        <div className={styles.container}>
          <div className={styles["hero"]}>
            <h1>hackersearch</h1>
            <h2>
              find tech businesses filtered by their social media accounts
            </h2>
          </div>
          <div className={styles["socials"]}>{renderSocialControls()}</div>
          <input
            type="text"
            className={styles["search-input"]}
            value={searchQuery}
            onChange={handleChangeSearchInput}
            placeholder="Describe a business / product to narrow down search (optional)"
            onKeyDown={handleKeyDownSearchInput}
          />
          <div className={styles["below-search-input"]}>
            <button
              className={styles["more-button"]}
              onClick={openAdvancedOptions}
            >
              advanced options
            </button>
          </div>
          <div className={styles["search-action"]}>
            <button
              className={styles["search-button"]}
              onClick={handleSearchClick}
              disabled={isLoadingSearch}
            >
              search
            </button>
          </div>
          <div className={styles["result-controls"]}>
            <select
              onChange={handleSortChange}
              value={searchSortType}
              className={styles["sort-input"]}
            >
              <option value="recent">Recently added</option>
              <option value="popular">Popularity</option>
              {!!searchQuery && <option value="relevant">Relevance</option>}
            </select>
          </div>
          {!!searchResults.length && (
            <>
              <div className={styles["export-controls"]}>
                <button
                  className={styles["export__button"]}
                  onClick={handleClickExport}
                  disabled={isLoadingSearch}
                >
                  export
                </button>
              </div>
              <div className={styles["results-list"]}>
                {renderResults()}
                {hasSearchedOnce &&
                  hasMoreResults &&
                  pageNum < MAX_PAGE_NUM && (
                    <div className={styles["more-results"]}>
                      <button
                        className={styles["more-results__button"]}
                        onClick={handleClickMoreResults}
                        disabled={isLoadingSearch}
                      >
                        more results
                      </button>
                    </div>
                  )}
              </div>
            </>
          )}
        </div>
      </main>
    </>
  );
}
