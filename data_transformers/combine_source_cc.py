from os.path import join
from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
import argparse


"""
This script combines all the data from compressed source scrapes + individual scrapes + similarweb scrapes

And performs final round of filtering if necessary
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-folderpath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    in_folderpath = args.in_folderpath
    out_filepath = args.out_filepath

    if not in_folderpath or not out_filepath:
        print("Invalid inputs")
        return

    # TODO: Accomodate for different sources when needed

    aift_file = join(in_folderpath, "cc_source_aift_scrape.csv")
    ph_file = join(in_folderpath, "cc_source_ph_scrape.csv")

    aift_df = read_csv_as_df(aift_file)
    ph_df = read_csv_as_df(ph_file)

    aift_df = aift_df.add_prefix("aift_")
    ph_df = ph_df.add_prefix("ph_")

    # Merge
    master_df = aift_df.merge(
        ph_df,
        left_on="aift_clean_product_url",
        right_on="ph_clean_product_url",
        how="outer",
    )

    # Merge some columns
    master_df["clean_product_url"] = master_df["aift_clean_product_url"].combine_first(
        master_df["ph_clean_product_url"]
    )

    # Drop some columns
    cols_to_drop = [
        "ph_clean_product_url",
        "aift_clean_product_url",
    ]
    master_df = master_df.drop(columns=cols_to_drop)

    # Set types
    master_df["ph_count_follower"] = master_df["ph_count_follower"].astype("Int64")
    master_df["aift_count_save"] = master_df["aift_count_save"].astype("Int64")

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
