from os.path import join
from custom_helpers_py.string_formatters import clean_text
from custom_helpers_py.basic_time_logger import log_start, log_end
from download_product_images import ERROR_FILENAME, RECORD_FILENAME
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.df_validator import validate_prod_sup_similarweb_df


"""
This script combines all the data from compressed source scrapes + individual scrapes + similarweb scrapes

And performs final round of filtering if necessary
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    args = parser.parse_args()

    in_filepath: str = args.in_filepath
    out_folderpath: str = args.out_folderpath

    if not in_filepath or not out_folderpath:
        print("Invalid inputs")
        return

    start_time = log_start()

    sup_similarweb_df = read_csv_as_df(in_filepath)

    # SimilarWeb
    sup_similarweb_df = sup_similarweb_df.rename(
        columns={
            "domain": "source_domain",
        }
    )
    similarweb_cols = ["source_domain", "total_visits_last_month", "data_created_at"]
    sup_similarweb_df = sup_similarweb_df[similarweb_cols]

    validate_prod_sup_similarweb_df(sup_similarweb_df)

    # Save
    sup_similarweb_df_savepath = join(out_folderpath, "sup_similarweb.csv")
    save_df_as_csv(sup_similarweb_df, sup_similarweb_df_savepath)

    log_end(start_time)


if __name__ == "__main__":
    main()
