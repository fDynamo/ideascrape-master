from custom_helpers_py.filter_results import (
    is_page_title_valid,
    is_page_description_valid,
)
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
)
import argparse
from os import listdir
from os.path import join
import json
import pandas as pd


"""
This script combines all the data from individual scrapes
Then cleans their title and description
Then filters them based on data and if they can be merged with filtered urls list

TODO:
- Reintroduce rejected filepath for blinks
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--inFolderPath", type=str, dest="in_folder_path")
    parser.add_argument("-o", "--outFilePath", type=str, dest="out_file_path")
    parser.add_argument("-r", "--rejectedFilePath", type=str, dest="rejected_file_path")
    args = parser.parse_args()

    in_folder_path = args.in_folder_path
    out_file_path = args.out_file_path
    rejected_file_path = args.rejected_file_path

    if not in_folder_path or not out_file_path:
        print("Invalid inputs")
        exit(1)

    essential_data_folder_path = join(in_folder_path, "essential_data")

    filtered_list = []
    rejected_list = []
    essential_data_file_list = listdir(essential_data_folder_path)
    for file_name in essential_data_file_list:
        file_obj = None
        file_path = join(essential_data_folder_path, file_name)
        with open(file_path, "r", encoding="utf-8") as in_file:
            file_obj = json.loads(in_file.read())

        title = file_obj["title"]
        if not is_page_title_valid(title):
            rejected_list.append(file_obj)
            continue

        description = file_obj["description"]
        if not is_page_description_valid(description):
            rejected_list.append(file_obj)
            continue

        filtered_list.append(file_obj)

    filtered_df = pd.DataFrame(filtered_list)
    rejected_df = pd.DataFrame(rejected_list)

    # Save
    save_df_as_csv(filtered_df, out_file_path)
    if isinstance(rejected_file_path, str):
        save_df_as_csv(rejected_df, rejected_file_path)


if __name__ == "__main__":
    main()
