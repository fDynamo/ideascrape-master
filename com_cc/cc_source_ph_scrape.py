import pandas as pd
from custom_helpers_py.url_formatters import clean_url
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse


"""
TODO:
- Add option to select range of dates from folder

python cc-source-ph-scrape.py -i /mnt/c/Users/fdirham/Desktop/code_projects/ideascrape/ideascrape-out-files/files/source_scrapes/ph -o ./test/out/ph-cc.csv
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    master_df = concat_folder_into_df(in_filepath, drop_subset="product_url")
    master_df["updated_at"] = pd.to_datetime(master_df["updated_at"], utc=True)
    master_df["listed_at"] = pd.to_datetime(master_df["listed_at"], utc=True)

    master_df["clean_product_url"] = master_df["product_url"].apply(clean_url)
    master_df = master_df.drop_duplicates(subset="clean_product_url", keep="last")

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
