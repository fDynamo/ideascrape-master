import pandas as pd
from custom_helpers_py.filter_results import is_url_valid
from custom_helpers_py.url_formatters import clean_url, get_domain_from_url
from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
from custom_helpers_py.get_paths import get_search_main_records_filepath
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    parser.add_argument("-r", "--rejected-filepath", type=str)
    parser.add_argument("--use-local-cache")
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath
    rejected_filepath = args.rejected_filepath
    use_local_cache = args.use_local_cache  # Just for testing

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    master_df = read_csv_as_df(in_filepath)

    # Clean and get domains from urls
    master_df["url"] = master_df["url"].apply(clean_url)
    master_df["domain"] = master_df["url"].apply(get_domain_from_url)

    master_df = pd.concat([master_df["url"], master_df["domain"]]).to_frame("url")
    master_df = master_df.drop_duplicates(subset="url", keep="last")
    master_df = master_df.sort_values(by="url")

    # Get cache filter
    search_main_records_filepath = get_search_main_records_filepath(
        prod=not use_local_cache
    )
    search_main_records_df = read_csv_as_df(search_main_records_filepath)
    search_main_records_list = search_main_records_df.to_dict(orient="records")
    in_records_set = set()
    for record in search_main_records_list:
        to_add = record["product_url"]
        in_records_set.add(to_add)

    # Filter
    def is_url_valid_helper(in_url):
        if in_url in in_records_set:
            return False

        return is_url_valid(in_url)

    master_df["is_valid"] = master_df["url"].apply(is_url_valid_helper)
    filtered_df = master_df[master_df["is_valid"]][["url"]]
    rejected_df = master_df[~master_df["is_valid"]][["url"]]

    # Save
    save_df_as_csv(filtered_df, out_filepath)
    if isinstance(rejected_filepath, str):
        save_df_as_csv(rejected_df, rejected_filepath)


if __name__ == "__main__":
    main()
