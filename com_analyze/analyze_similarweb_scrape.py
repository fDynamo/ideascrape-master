import pandas as pd
from custom_helpers_py.pandas_helpers import (
    concat_folder_into_df,
)
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData


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

    try:
        master_df = concat_folder_into_df(in_folder_path, drop_subset="domain")
    except:
        print("No similarweb data found, skipping")
        quit(0)

    # Delete malformed rows
    master_df = master_df.dropna(subset="total_visits_last_month")

    master_df["sw_created_at"] = pd.to_datetime(master_df["data_created_at"])

    def format_last_month_traffic(in_str: str):
        if "K" in in_str:
            in_str = in_str[:-1]
            to_return = float(in_str)
            to_return = to_return * 1_000
        elif "M" in in_str:
            in_str = in_str[:-1]
            to_return = float(in_str)
            to_return = to_return * 1_000_000
        elif "B" in in_str:
            in_str = in_str[:-1]
            to_return = float(in_str)
            to_return = to_return * 1_000_000_000
        else:
            to_return = int(in_str)
        return int(to_return)

    master_df["sw_visits_last_month"] = master_df["total_visits_last_month"].apply(
        format_last_month_traffic
    )

    master_df["sw_url"] = "https://www.similarweb.com/website/" + master_df["domain"]
    master_df["product_domain"] = master_df["domain"]

    cols_to_keep = ["product_domain"] + [
        col for col in master_df.columns if col.startswith("sw_")
    ]
    master_df = master_df[cols_to_keep]

    tpd = TPData(folder_path=tp_folder_path)
    tpd.add_data(to_add_df=master_df, part_name="sw")


if __name__ == "__main__":
    main()
