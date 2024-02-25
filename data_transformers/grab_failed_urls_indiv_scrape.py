from custom_helpers_py.string_formatters import clean_text
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse
import pandas as pd


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

    failed_log_df = concat_folder_into_df(
        in_folderpath,
        drop_subset="url",
        use_python_engine=True,
        ends_with_filter="-failed.csv",
        allow_empty_return=True,
    )

    if failed_log_df == None:
        to_save_df = pd.DataFrame([{"url": ""}])
        save_df_as_csv(to_save_df, out_filepath)
        return

    failed_log_list = failed_log_df.to_dict(orient="records")
    urls_list = []
    for log_obj in failed_log_list:
        key_str: str = log_obj["key"]
        val_str: str = log_obj["val"]

        if key_str == "url" and val_str != "":
            to_add = {"url": val_str.strip()}
            urls_list.append(to_add)

    new_df = pd.DataFrame(urls_list)

    save_df_as_csv(new_df, out_filepath)


if __name__ == "__main__":
    main()
