from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse


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

    master_df = concat_folder_into_df(in_folderpath, ends_with_filter=None)
    master_df = master_df.dropna(subset="url")
    master_df = master_df.drop_duplicates(subset="url", keep="last")
    master_df = master_df.sort_values(by="url")

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
