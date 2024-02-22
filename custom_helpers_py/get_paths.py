from dotenv import load_dotenv
from os import environ, listdir
from os.path import join
import json

load_dotenv()


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
