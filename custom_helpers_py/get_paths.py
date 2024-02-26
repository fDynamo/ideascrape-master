from dotenv import load_dotenv
from os import environ
from os.path import join
import json

load_dotenv(override=True)


def access_cache_folder(key: str) -> str:
    master_root = environ.get("IDEASCRAPE_CACHE_FOLDER")
    directory_structure_filepath = join(master_root, "directory-structure.json")
    new_folder_path = ""
    with open(directory_structure_filepath, "r", encoding="utf-8") as file:
        directory_structure_json = json.load(file)
        new_folder_path = directory_structure_json[key]

    return join(master_root, new_folder_path)


def get_artifacts_folder_path() -> str:
    return environ.get("RUN_ARTIFACTS_FOLDER")


def get_search_main_records_filepath(prod: bool) -> str:
    cache_key = "file_search_main_records"
    if prod:
        cache_key += "_prod"
    else:
        cache_key += "_local"

    return access_cache_folder(cache_key)


def get_sup_similarweb_records_filepath(prod: bool) -> str:
    cache_key = "file_sup_similarweb_records"
    if prod:
        cache_key += "_prod"
    else:
        cache_key += "_local"

    return access_cache_folder(cache_key)
