from custom_helpers_py.string_formatters import clean_text
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    individual_scrape_df = concat_folder_into_df(
        in_filepath, drop_subset="url", use_python_engine=True
    )

    individual_scrape_df["title"] = individual_scrape_df["title"].apply(clean_text)
    individual_scrape_df["description"] = individual_scrape_df["description"].apply(
        clean_text
    )

    # Delete malformed rows
    individual_scrape_df = individual_scrape_df.dropna(subset=["title", "description"])

    individual_scrape_df = individual_scrape_df.sort_values(by="url").reset_index(
        drop=True
    )

    individual_scrape_df = individual_scrape_df.rename(
        columns={"title": "page_title", "description": "page_description"}
    )

    individual_scrape_df = individual_scrape_df[
        ["url", "page_title", "page_description", "favicon_url"]
    ]

    save_df_as_csv(individual_scrape_df, out_filepath)


if __name__ == "__main__":
    main()
