import json
from os.path import dirname, join, realpath
import validators
from langdetect import detect

# Open external files
dir_path = dirname(realpath(__file__))

# Open url filters
url_filters_path = join(dir_path, "url_filters.json")
url_filters_file = open(url_filters_path)
url_filters = json.load(url_filters_file)
url_filters_file.close()

invalid_description_contents_path = join(dir_path, "invalid_description_contents.json")
invalid_description_contents_file = open(invalid_description_contents_path)
invalid_description_contents = json.load(invalid_description_contents_file)
invalid_description_contents_file.close()


def is_url_valid(in_clean_url: str) -> bool:
    if not in_clean_url or not isinstance(in_clean_url, str):
        return False

    if not validators.url("https://" + in_clean_url):
        return False

    substrings = url_filters["substrings"]
    for substring in substrings:
        if substring in in_clean_url:
            return False

    starts = url_filters["starts"]
    for start in starts:
        if in_clean_url.startswith(start):
            return False

    ends = url_filters["ends"]
    for end in ends:
        if in_clean_url.endswith(end):
            return False

    return True


def is_url_individual_scrape(in_url: str) -> bool:
    substrings = url_filters["individual_scrape_substrings"]
    for substring in substrings:
        if substring in in_url:
            return False
    return True


def is_page_title_valid(page_title: str):
    if not page_title:
        return False

    str_to_test = page_title.lower()
    if "404" in str_to_test:
        return False

    if "not found" in str_to_test:
        return False

    if "just a moment" in str_to_test:
        return False

    if "for sale" in str_to_test:
        return False

    if str_to_test.startswith("40"):
        return False

    return True


def is_page_description_valid(in_desc: str) -> bool:
    if not in_desc or in_desc == "":
        return False

    # Is language english
    ACCEPTED_LANGUAGE = "en"
    try:
        desc_lang = detect(in_desc)
        if desc_lang != ACCEPTED_LANGUAGE:
            return False
    except:
        pass

    lower_desc = in_desc.lower()
    substrings = invalid_description_contents["substrings"]
    for substring in substrings:
        if substring in lower_desc:
            return False

    return True


if __name__ == "__main__":
    test_url = "amazon.com/ai/songwraiter/?ref=alternative"

    desc_valid = is_page_description_valid(
        "辨識並去除音檔中的背景雜音，提升人聲的清晰程度。 Identify and extract the background noise to improve the human voice clarity in the uploaded audio file."
    )
    # print(desc_valid)

    # print(detect(" bn vn bi kha bi vn nguyn vn nguyn bn vn bn bi c ti liu "))

    print(detect("Projectsveltos"))
    print(detect("随机密码"))
