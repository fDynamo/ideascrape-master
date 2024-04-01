from custom_helpers_py.filter_results import (
    is_page_title_valid,
    is_page_description_valid,
)
from custom_helpers_py.string_formatters import remove_unnecessary_spaces_from_string
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
)
from custom_helpers_py.url_formatters import clean_url
import argparse
from os import listdir
from os.path import join
import json
import pandas as pd
from com_analyze.analyze_page_copy import analyze_page_copy
from custom_helpers_py.filter_results import is_url_valid
from copy import deepcopy
from custom_helpers_py.custom_classes.tp_data import TPData


"""
This script combines all the data from individual scrapes by filtering and analyzing data. It creates a CC of indiv scrape data to be used in other scripts
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--in-folder-path", type=str, dest="in_folder_path", required=True
    )
    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    args = parser.parse_args()

    in_folder_path = args.in_folder_path
    tp_folder_path = args.tp_folder_path

    essential_data_folder_path = join(in_folder_path, "essential_data")
    page_copy_folder_path = join(in_folder_path, "page_copy")

    data_list = []
    essential_data_file_list = listdir(essential_data_folder_path)
    REJECTED_PREFIX = "analyze_indiv_scrape: "
    for file_name in essential_data_file_list:
        # Read file
        file_obj = None
        essential_data_file_path = join(essential_data_folder_path, file_name)
        with open(essential_data_file_path, "r", encoding="utf-8") as in_file:
            file_obj = json.loads(in_file.read())

        # Resolve redirects
        file_obj["end_url"] = clean_url(file_obj["end_url"])
        if file_obj["end_url"] != file_obj["init_url"]:
            data_list.append(
                {
                    **file_obj,
                    "rejected": REJECTED_PREFIX
                    + "Redirected to new url "
                    + file_obj["end_url"],
                }
            )
            file_obj = deepcopy(file_obj)
            file_obj["init_url"] = file_obj["end_url"]

            # Since we don't continue, we log the fact that the old init url is rejected
            # But we proceed with logic wit init = end url, so we will see if this new url holds up to scrutiny

        # See if url is valid
        url_valid_res = is_url_valid(file_obj["init_url"])
        if url_valid_res != "y":
            data_list.append(
                {
                    **file_obj,
                    "rejected": REJECTED_PREFIX + "Invalid url " + url_valid_res,
                }
            )
            continue

        # Filter on page title
        title = file_obj["title"]
        title = remove_unnecessary_spaces_from_string(title)
        file_obj["title"] = title

        if not is_page_title_valid(title):
            data_list.append(
                {**file_obj, "rejected": REJECTED_PREFIX + "Invalid title"}
            )
            continue

        # Analyze page copy
        page_copy_file_path = join(
            page_copy_folder_path, file_name.replace(".json", ".txt")
        )
        analyzed_page_copy_obj = analyze_page_copy(page_copy_file_path)

        # Get new description
        description: str = file_obj["description"] or ""
        analyzed_description: str = (
            description + " " + analyzed_page_copy_obj["page_gist"]
        )
        analyzed_description = remove_unnecessary_spaces_from_string(
            analyzed_description
        )

        if not description or description == "":
            description = analyzed_description
        else:
            description = remove_unnecessary_spaces_from_string(description)

        if not is_page_description_valid(description):
            data_list.append(
                {**file_obj, "rejected": REJECTED_PREFIX + "Invalid description"}
            )
            continue

        file_obj["analyzed_description"] = analyzed_description
        file_obj["description"] = description

        data_list.append(file_obj)

    # Process data
    master_df = pd.DataFrame(data_list)
    col_to_drop_list = ["end_url", "request_duration_s"]
    master_df = master_df.drop(columns=col_to_drop_list)
    master_df = master_df.rename(columns={"init_url": "product_url"})
    master_df = master_df.drop_duplicates(subset="product_url", keep="last")

    # Save
    tpd = TPData(folder_path=tp_folder_path)
    tpd.add_data(to_add_df=master_df)


if __name__ == "__main__":
    main()
