import pandas as pd
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse

"""
TODO:
- Don't embed if embedding already exists
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("i", "--inFilePath", type=str, dest="in_file_path")
    parser.add_argument("-o", "--outFilePath", type=str, dest="out_file_path")
    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_file_path = args.out_file_path

    if not in_file_path or not out_file_path:
        print("Invalid inputs")
        exit(1)

    indiv_scrape_df = read_csv_as_df(in_file_path)
    indiv_scrape_list = indiv_scrape_df.to_dict(orient="records")
    to_return_list = []

    def fix_favicon_url(favicon_url: str, end_url: str):
        if favicon_url.startswith("http"):
            return favicon_url

        if favicon_url.startswith("//"):
            return "https:" + favicon_url

        if not end_url or not isinstance(end_url, str):
            raise Exception("Invalid end url!")

        end_url.removesuffix("/")
        favicon_url.removeprefix("/")

        to_return = end_url + "/" + favicon_url
        return to_return

    for obj in indiv_scrape_list:
        product_image_url = None
        page_image_url: str = obj["page_image_url"]
        end_url: str = obj["end_url"]
        if page_image_url and isinstance(page_image_url, str):
            product_image_url = fix_favicon_url(page_image_url, end_url)

        to_add = {"product_url": obj["init_url"], "image_url": product_image_url}
        to_return_list.append(to_add)

    to_return_df = pd.DataFrame(to_return_list)
    save_df_as_csv(to_return_df, out_file_path)


if __name__ == "__main__":
    main()
