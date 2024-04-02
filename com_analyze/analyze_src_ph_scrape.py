import pandas as pd
from custom_helpers_py.url_formatters import clean_url
from custom_helpers_py.pandas_helpers import concat_folder_into_df
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--in-folder-path", type=str, required=True, dest="in_folder_path"
    )
    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    args = parser.parse_args()

    in_folder_path = args.in_folder_path
    tp_folder_path = args.tp_folder_path

    master_df = concat_folder_into_df(in_folder_path, drop_subset="product_url")
    master_df["product_url"] = master_df["product_url"].apply(clean_url)
    master_df = master_df.drop_duplicates(subset="product_url", keep="last")

    master_df["ph_updated_at"] = pd.to_datetime(master_df["updated_at"], utc=True)
    master_df["ph_listed_at"] = pd.to_datetime(master_df["listed_at"], utc=True)
    master_df = master_df.rename(
        columns={"source_url": "ph_url", "count_follower": "ph_count_follower"}
    )
    master_df = master_df[
        ["product_url", "ph_url", "ph_count_follower", "ph_listed_at", "ph_updated_at"]
    ]

    tpd = TPData(folder_path=tp_folder_path)
    tpd.add_data(to_add_df=master_df, part_name="source_ph")


if __name__ == "__main__":
    main()
