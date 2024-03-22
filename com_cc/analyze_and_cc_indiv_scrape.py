from custom_helpers_py.filter_results import (
    is_page_title_valid,
    is_page_description_valid,
)
from custom_helpers_py.string_formatters import remove_unnecessary_spaces_from_string
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
)
import argparse
from os import listdir
from os.path import join
import json
import pandas as pd
from com_cc.analyze_page_copy import analyze_page_copy


"""
This script combines all the data from individual scrapes by filtering and analyzing data. It creates a CC of indiv scrape data to be used in other scripts
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--inFolderPath", type=str, dest="in_folder_path", required=True
    )
    parser.add_argument(
        "-o", "--outFilePath", type=str, dest="out_file_path", required=True
    )
    parser.add_argument("-r", "--rejectedFilePath", type=str, dest="rejected_file_path")
    parser.add_argument(
        "--analyzedPageCopyFolderPath", type=str, dest="analyzed_page_copy_folder_path"
    )

    args = parser.parse_args()

    in_folder_path = args.in_folder_path
    out_file_path = args.out_file_path
    rejected_file_path = args.rejected_file_path
    analyzed_page_copy_folder_path = args.analyzed_page_copy_folder_path

    essential_data_folder_path = join(in_folder_path, "essential_data")
    page_copy_folder_path = join(in_folder_path, "page_copy")

    filtered_list = []
    rejected_list = []
    essential_data_file_list = listdir(essential_data_folder_path)
    for file_name in essential_data_file_list:
        # Read file
        file_obj = None
        essential_data_file_path = join(essential_data_folder_path, file_name)
        with open(essential_data_file_path, "r", encoding="utf-8") as in_file:
            file_obj = json.loads(in_file.read())

        init_url = file_obj["init_url"]
        title = file_obj["title"]
        title = remove_unnecessary_spaces_from_string(title)
        file_obj["title"] = title

        # Filter on page title
        if not is_page_title_valid(title):
            rejected_list.append({"url": init_url, "reason": "Invalid title"})
            continue

        # Analyze page copy
        page_copy_file_path = join(
            page_copy_folder_path, file_name.replace(".json", ".txt")
        )
        analyzed_page_copy_obj = analyze_page_copy(page_copy_file_path)
        if analyzed_page_copy_folder_path:
            analyzed_page_copy_save_file_path = join(
                analyzed_page_copy_folder_path, file_name
            )
            to_write = json.dumps(analyzed_page_copy_obj, indent=4)
            with open(
                analyzed_page_copy_save_file_path, "w", encoding="utf-8"
            ) as out_file:
                out_file.write(to_write)

        # Get new description
        description = file_obj["description"] or ""
        analyzed_description: str = (
            description + " " + analyzed_page_copy_obj["page_gist"]
        )
        analyzed_description = remove_unnecessary_spaces_from_string(
            analyzed_description
        )

        if not description:
            description = analyzed_description
        else:
            description = remove_unnecessary_spaces_from_string(description)

        if not is_page_description_valid(description):
            rejected_list.append({"url": init_url, "reason": "Invalid description"})
            continue

        file_obj["analyzed_description"] = analyzed_description
        file_obj["description"] = description

        filtered_list.append(file_obj)

    filtered_df = pd.DataFrame(filtered_list)
    rejected_df = pd.DataFrame(rejected_list)

    # Save
    save_df_as_csv(filtered_df, out_file_path)
    if isinstance(rejected_file_path, str):
        save_df_as_csv(rejected_df, rejected_file_path)


if __name__ == "__main__":
    main()
