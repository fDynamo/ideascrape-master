from custom_helpers_py.filter_results import (
    is_domain_similarweb_scrapable,
)
from custom_helpers_py.basic_time_logger import log_start, log_end
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    parser.add_argument("-r", "--rejected-filepath", type=str)
    parser.add_argument("-c", "--col-name", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath
    rejected_filepath = args.rejected_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    start_time = log_start("filter-indiv-scrape")
    master_df = read_csv_as_df(in_filepath)

    # Handle col_name arg
    col_name = args.col_name or "domain"
    if args.col_name:
        master_df["domain"] = master_df[col_name]
        master_df = master_df[["domain"]]

    # Drop na
    master_df = master_df.dropna(subset="domain")

    # Filter by title and description
    # TODO: More complex yet efficient logic to truly filter trash out
    master_df["is_valid"] = master_df["domain"].apply(is_domain_similarweb_scrapable)

    invalid_mask = ~master_df["is_valid"]
    rejected_df = master_df[invalid_mask]
    filtered_df = master_df[~invalid_mask]
    filtered_df = filtered_df.drop(columns=["is_valid"])

    # Save
    save_df_as_csv(filtered_df, out_filepath)
    if isinstance(rejected_filepath, str):
        save_df_as_csv(rejected_df, rejected_filepath)

    log_end(start_time)


if __name__ == "__main__":
    main()
