from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.url_formatters import get_domain_from_url


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    parser.add_argument("-c", "--url-col-name", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    master_df = read_csv_as_df(in_filepath)

    # Handle col_name arg
    col_name = args.url_col_name or "url"

    master_df["url"] = master_df[col_name]
    master_df = master_df[["url"]]
    master_df["domain"] = master_df["url"].apply(get_domain_from_url)
    master_df = master_df.drop(columns=["url"])
    master_df = master_df.dropna(subset="domain")
    master_df = master_df.drop_duplicates(subset="domain", keep="last")
    master_df = master_df.sort_values(by="domain")

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
