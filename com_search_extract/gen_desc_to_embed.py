import pandas as pd
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.string_formatters import convert_url_to_file_name
import json
from os.path import join

"""
TODO:
- Don't embed if embedding already exists
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--ccIndivScrapeFilePath",
        type=str,
        dest="cc_indiv_scrape_file_path",
        required=True,
    )
    parser.add_argument(
        "--analyzedPageCopyFolderPath",
        type=str,
        dest="analyzed_page_copy_folder_path",
        required=True,
    )
    parser.add_argument(
        "-o", "--outFilePath", type=str, dest="out_file_path", required=True
    )
    args = parser.parse_args()

    cc_indiv_scrape_file_path = args.cc_indiv_scrape_file_path
    analyzed_page_copy_folder_path = args.analyzed_page_copy_folder_path
    out_file_path = args.out_file_path

    indiv_scrape_df = read_csv_as_df(cc_indiv_scrape_file_path)
    indiv_scrape_list = indiv_scrape_df.to_dict(orient="records")
    to_return_list = []

    for obj in indiv_scrape_list:
        url_file_name = convert_url_to_file_name(obj["init_url"])
        page_copy_file_path = join(
            analyzed_page_copy_folder_path, url_file_name + ".json"
        )
        with open(page_copy_file_path, "r", encoding="utf-8") as page_copy_file:
            page_copy = json.loads(page_copy_file.read())

        page_gist = page_copy["page_gist"]
        desc: str = obj["description"] + " " + page_gist
        desc = desc.strip()
        to_add = {"product_url": obj["init_url"], "desc": desc}
        to_return_list.append(to_add)

    to_return_df = pd.DataFrame(to_return_list)
    save_df_as_csv(to_return_df, out_file_path)


if __name__ == "__main__":
    main()
