from custom_helpers_py.filter_results import (
    is_page_title_valid,
    is_page_description_valid,
)
from custom_helpers_py.basic_time_logger import log_start, log_end
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse


"""
This script combines all the data from individual scrapes
Then cleans their title and description
Then filters them based on data and if they can be merged with filtered urls list
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    parser.add_argument("-r", "--rejected-filepath", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath
    rejected_filepath = args.rejected_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    start_time = log_start("filter-indiv-scrape")
    master_df = read_csv_as_df(in_filepath)

    # Drop those without title or description
    master_df = master_df.dropna(subset=["page_title", "page_description"])

    # Filter by title and description
    # TODO: More complex yet efficient logic to truly filter trash out
    master_df["is_valid_title"] = master_df["page_title"].apply(is_page_title_valid)
    master_df["is_valid_description"] = master_df["page_description"].apply(
        is_page_description_valid
    )

    invalid_mask = ~master_df["is_valid_title"] | ~master_df["is_valid_description"]
    rejected_df = master_df[invalid_mask]
    filtered_df = master_df[~invalid_mask]
    filtered_df = filtered_df.drop(columns=["is_valid_title", "is_valid_description"])

    # Save
    save_df_as_csv(filtered_df, out_filepath)
    if isinstance(rejected_filepath, str):
        save_df_as_csv(rejected_df, rejected_filepath)

    log_end(start_time)


if __name__ == "__main__":
    main()
