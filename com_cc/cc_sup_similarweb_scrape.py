import pandas as pd
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse

"""
What this compressor does:
- Take all scrape files and combine them
- Drop any duplicates, biasing newer entries
- Convert date to datetime
- Convert visits to numbers not strings
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

    master_df = concat_folder_into_df(in_filepath, drop_subset="domain")

    # Delete malformed rows
    master_df = master_df.dropna(subset="total_visits_last_month")

    master_df["data_created_at"] = pd.to_datetime(master_df["data_created_at"])

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

    master_df["total_visits_last_month"] = master_df["total_visits_last_month"].apply(
        format_last_month_traffic
    )

    print(master_df)
    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
