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
    parser.add_argument("--in-col", type=str)
    parser.add_argument("--out-col", type=str)
    parser.add_argument("--dont-clean", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath
    in_col = args.in_col
    out_col = args.out_col
    is_dont_clean = args.dont_clean

    if not in_filepath or not out_filepath or not in_col or not out_col:
        print("Invalid inputs")
        exit(code=1)

    master_df = read_csv_as_df(in_filepath)

    master_df["url"] = master_df[in_col]
    master_df = master_df[["url"]]
    urls_df = master_df["url"].to_frame("url")

    if not is_dont_clean:
        urls_df = urls_df.dropna(subset="url")
        urls_df = urls_df.drop_duplicates(subset="url", keep="last")
        urls_df = urls_df.sort_values(by="url")

    urls_df = urls_df.rename(columns={"url": out_col})

    save_df_as_csv(urls_df, out_filepath)


if __name__ == "__main__":
    main()
