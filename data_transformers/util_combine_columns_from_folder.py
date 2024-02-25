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
    parser.add_argument("-c", "--col-name", type=str)
    args = parser.parse_args()

    in_folderpath = args.in_folderpath
    out_filepath = args.out_filepath
    col_name = args.col_name

    if not in_folderpath or not out_filepath or not col_name:
        print("Invalid inputs")
        exit(code=1)

    master_df = concat_folder_into_df(
        in_folderpath, ends_with_filter=None, enforce_mono_column=col_name
    )
    master_df = master_df.dropna(subset=col_name)
    master_df = master_df.drop_duplicates(subset=col_name, keep="last")
    master_df = master_df.sort_values(by=col_name)

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
