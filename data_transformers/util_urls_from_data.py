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
    parser.add_argument("-c", "--col-name", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    master_df = read_csv_as_df(in_filepath)

    # Handle col_name arg
    col_name = args.col_name or "url"
    if args.col_name:
        master_df["url"] = master_df[col_name]
        master_df = master_df[["url"]]

    urls_df = master_df[col_name].to_frame("url")
    urls_df = urls_df.dropna(subset="url")
    urls_df = urls_df.drop_duplicates(subset="url", keep="last")
    urls_df = urls_df.sort_values(by="url")

    save_df_as_csv(urls_df, out_filepath)


if __name__ == "__main__":
    main()
