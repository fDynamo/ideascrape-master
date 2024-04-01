import json
from os.path import dirname, join, realpath
import validators
from langdetect import detect

# Open external files
dir_path = dirname(realpath(__file__))

# Open url filters
result_filters_path = join(dir_path, "result_filters.json")
result_filters_file = open(result_filters_path)
result_filters = json.load(result_filters_file)
result_filters_file.close()

invalid_description_contents_path = join(dir_path, "invalid_description_contents.json")
invalid_description_contents_file = open(invalid_description_contents_path)
invalid_description_contents = json.load(invalid_description_contents_file)
invalid_description_contents_file.close()


# Returns 'y' if valid
# Returns a cause string if not valid
def is_url_valid(in_clean_url: str) -> str:
    if not in_clean_url or not isinstance(in_clean_url, str):
        return "Malformed url"

    if not validators.url("https://" + in_clean_url):
        return "Validator"

    substrings = result_filters["substrings"]
    for substring in substrings:
        if substring in in_clean_url:
            return "Substring"

    starts = result_filters["starts"]
    for start in starts:
        if in_clean_url.startswith(start):
            return "Starts"

    ends = result_filters["ends"]
    for end in ends:
        if in_clean_url.endswith(end):
            return "Ends"

    return "y"


def is_page_title_valid(page_title: str):
    if not page_title or not isinstance(page_title, str):
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
    if not in_desc or not isinstance(in_desc, str):
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


# Returns 'y' if valid
# Returns a cause string if not valid
def is_domain_similarweb_scrapable(in_domain) -> str:
    substrings = result_filters["similarweb"]["substrings"]
    for substring in substrings:
        if substring in in_domain:
            return "Substring: " + substring

    return "y"


if __name__ == "__main__":
    test_url = "www.milestoneflow.io"

    print(is_url_valid(test_url))
