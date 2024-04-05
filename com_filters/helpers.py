from custom_helpers_py.json_helpers import load_json_as_obj
import validators
from langdetect import detect
from os.path import dirname, join, realpath

# Read files
# Open external files
dir_path = dirname(realpath(__file__))
filter_files_folder_path = join(dir_path, "filter_files")

# Open url filters
indiv_url_filters_file_path = join(filter_files_folder_path, "indiv_url_filters.json")
indiv_url_filters = load_json_as_obj(indiv_url_filters_file_path)


def is_url_valid(in_clean_url: str):
    try:
        if not in_clean_url or not isinstance(in_clean_url, str):
            raise Exception("cannot read URL")

        if not validators.url("https://" + in_clean_url):
            raise Exception("invalid URL form")

        _filter_using_filter_file(in_clean_url, indiv_url_filters)
    except Exception as error:
        return (False, str(error))

    return (True, None)


indiv_title_filters_file_path = join(
    filter_files_folder_path, "indiv_title_filters.json"
)
indiv_title_filters = load_json_as_obj(indiv_title_filters_file_path)


def is_page_title_valid(page_title: str):
    str_to_test = page_title.lower()
    try:
        if not str_to_test or not isinstance(str_to_test, str):
            raise Exception("cannot read title")

        _filter_using_filter_file(str_to_test, indiv_title_filters)
    except Exception as error:
        return (False, str(error))

    return (True, None)


indiv_desc_filters_file_path = join(filter_files_folder_path, "indiv_desc_filters.json")
indiv_desc_filters = load_json_as_obj(indiv_desc_filters_file_path)


def is_page_desc_valid(in_desc: str):
    try:
        if not in_desc or not isinstance(in_desc, str):
            raise Exception("cannot read desc")

        # Is language english
        ACCEPTED_LANGUAGE = "en"
        try:
            desc_lang = detect(in_desc)
            if desc_lang != ACCEPTED_LANGUAGE:
                raise Exception("not english")
        except:
            pass

        lower_desc = in_desc.lower()
        _filter_using_filter_file(lower_desc, indiv_desc_filters)
    except Exception as error:
        return (False, str(error))

    return (True, None)


sw_domain_filters_file_path = join(filter_files_folder_path, "sw_domain_filters.json")
sw_domain_filters = load_json_as_obj(sw_domain_filters_file_path)


def is_domain_similarweb_scrapable(in_domain):
    try:
        if not in_domain or not isinstance(in_domain, str):
            raise Exception("cannot read domain")

        _filter_using_filter_file(in_domain, sw_domain_filters)

    except Exception as error:
        return (False, str(error))

    return (True, None)


def _filter_using_filter_file(in_str: str, filter_file_dict: dict):
    substrings = filter_file_dict.get("substrings", None)
    if substrings:
        for substring in substrings:
            if substring in in_str:
                raise Exception("substring: " + substring)

    starts = filter_file_dict.get("starts", None)
    if starts:
        for start in starts:
            if in_str.startswith(start):
                raise Exception("start: " + start)

    ends = filter_file_dict.get("ends", None)
    if ends:
        for end in ends:
            if in_str.endswith(end):
                raise Exception("end: " + end)
