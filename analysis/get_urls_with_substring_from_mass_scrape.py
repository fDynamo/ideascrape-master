import argparse
from custom_helpers_py.pandas_helpers import save_df_as_json, save_df_as_csv
import pandas as pd
from custom_helpers_py.pandas_helpers import (
    save_df_as_json,
    concat_folder_into_df,
)
from custom_helpers_py.url_formatters import clean_url


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--substring", type=str, required=True, dest="substring")
    parser.add_argument(
        "-o", "--out-file-path", type=str, dest="out_file_path", required=True
    )
    parser.add_argument("--src", "--source", type=str, required=False, dest="source")
    args = parser.parse_args()

    substring: str = args.substring
    out_file_path: str = args.out_file_path
    source: str = args.source

    df_list = []
    is_aift = not source or source == "aift"
    is_ph = not source or source == "ph"

    if is_aift:
        MASS_SCRAPE_AIFT_FOLDER_PATH = "./z_tmp/mass_scrape/aift"
        aift_df = concat_folder_into_df(MASS_SCRAPE_AIFT_FOLDER_PATH)
        aift_df = aift_df[["product_url"]]
        df_list.append(aift_df)

    if is_ph:
        MASS_SCRAPE_PH_FOLDER_PATH = "./z_tmp/mass_scrape/ph"
        ph_df = concat_folder_into_df(
            MASS_SCRAPE_PH_FOLDER_PATH, use_python_engine=True
        )
        ph_df = ph_df[["product_url"]]
        df_list.append(ph_df)

    master_df = pd.concat(df_list)

    master_df["product_url"] = master_df["product_url"].apply(lambda x: clean_url(x))

    mask = master_df["product_url"].str.contains(substring)
    master_df = master_df[mask]

    master_df.rename(columns={"product_url": "url"})

    if out_file_path.endswith("json"):
        save_df_as_json(master_df, out_file_path)
    else:
        save_df_as_csv(master_df, out_file_path)


if __name__ == "__main__":
    main()
