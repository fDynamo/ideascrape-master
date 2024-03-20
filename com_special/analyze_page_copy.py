from os import listdir
from os.path import join
import argparse
import json


"""
Analyzes page copies.

TODO:
- Actually implement this
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("i", "--inFolderPath", type=str, dest="in_folder_path")
    parser.add_argument("-o", "--outFolderPath", type=str, dest="out_folder_path")
    args = parser.parse_args()

    in_folder_path = args.in_folder_path
    out_folder_path = args.out_folder_path

    if not in_folder_path or not out_folder_path:
        print("Invalid inputs")
        exit(1)

    page_copy_folder_path = join(in_folder_path, "page_copy")

    page_copy_file_list = listdir(page_copy_folder_path)
    for file_name in page_copy_file_list:
        # TODO: Replace with analysis
        to_save = {"page_gist": ""}
        save_file_path = join(out_folder_path, file_name)
        with open(save_file_path, "w") as out_file:
            out_file.write(json.dumps(to_save, indent=4))


if __name__ == "__main__":
    main()
