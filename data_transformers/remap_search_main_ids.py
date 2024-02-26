from os.path import join
from custom_helpers_py.string_formatters import clean_text
from custom_helpers_py.url_formatters import get_domain_from_url
from custom_helpers_py.basic_time_logger import log_start, log_end
from download_product_images import ERROR_FILENAME, RECORD_FILENAME
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.get_paths import (
    get_search_main_records_filepath,
    get_sup_similarweb_records_filepath,
)


"""
This script combines all the data from compressed source scrapes + individual scrapes + similarweb scrapes

And performs final round of filtering if necessary
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--out-filepath", type=str)
    parser.add_argument("-r", "--rejected-filepath", type=str)
    parser.add_argument(
        "--prod-env", action=argparse.BooleanOptionalAction, default=False
    )

    args = parser.parse_args()

    out_filepath: str = args.out_filepath
    rejected_filepath: str = args.rejected_filepath
    is_prod_env = args.prod_env

    if not out_filepath:
        print("Invalid inputs")
        return

    start_time = log_start()

    # Get dfs
    search_main_records_cache_filepath = get_search_main_records_filepath(is_prod_env)
    search_main_df = read_csv_as_df(search_main_records_cache_filepath)

    sup_similarweb_records_cache_filepath = get_sup_similarweb_records_filepath(
        is_prod_env
    )
    sup_similarweb_df = read_csv_as_df(sup_similarweb_records_cache_filepath)

    # Get domain
    search_main_df["source_domain"] = search_main_df["product_url"].apply(
        get_domain_from_url
    )

    # Merge together
    sup_similarweb_df = sup_similarweb_df.rename(columns={"id": "similarweb_id"})

    master_df = search_main_df.merge(sup_similarweb_df, on="source_domain", how="left")

    if rejected_filepath:
        rejected_df = sup_similarweb_df[
            ~sup_similarweb_df["source_domain"].isin(master_df["source_domain"])
        ]
        save_df_as_csv(rejected_df, rejected_filepath)

    master_df = master_df.drop(columns=["source_domain"])
    save_df_as_csv(master_df, out_filepath)

    log_end(start_time)


if __name__ == "__main__":
    main()
