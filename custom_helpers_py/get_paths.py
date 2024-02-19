from dotenv import load_dotenv
from os import environ, listdir
from os.path import join
import json

load_dotenv()


def get_out_folder(key: str) -> str:
    master_root = environ.get("MASTER_OUT_FOLDER")
    directory_structure_filepath = join(master_root, "directory-structure.json")
    new_folder_path = ""
    with open(directory_structure_filepath, "r", encoding="utf-8") as file:
        directory_structure_json = json.load(file)
        new_folder_path = directory_structure_json[key]

    return join(master_root, new_folder_path)


def get_most_recent_file_in_out_folder(out_folder_key: str, suffix: str):
    out_folder = get_out_folder(out_folder_key)
    folder_files: list[str] = listdir(out_folder)

    newest_filename = ""
    for filename in folder_files:
        if filename.endswith(suffix):
            if not newest_filename:
                newest_filename = filename
                continue
            if filename > newest_filename:
                newest_filename = filename

    if not newest_filename:
        return None
    return join(out_folder, newest_filename)
