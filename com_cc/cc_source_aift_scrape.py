import pandas as pd
from os.path import join
from datetime import datetime, timedelta
from custom_helpers_py.url_formatters import clean_url
from custom_helpers_py.pandas_helpers import concat_folder_into_df
from custom_helpers_py.pandas_helpers import (
    save_df_as_csv,
    concat_folder_into_df,
)
import argparse

"""
TODO:
- Note data timestamp before removing duplicates
- Compress logs as well for analysis later
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

    LISTS_FOLDER = join(in_folderpath, "lists")
    lists_df = concat_folder_into_df(LISTS_FOLDER, drop_subset="source_url")
    POSTS_FOLDER = join(in_folderpath, "posts")
    posts_df = concat_folder_into_df(POSTS_FOLDER, drop_subset="source_url")

    def clean_numbers(in_num):
        if not in_num:
            return 0
        return int(str(in_num).replace(",", ""))

    """
    TODO: Resolve conflicts better. Right now, we just take lists_df as source of truth, but there are times where the posts_df will have more up to date data. This means that count_save might be off.
    """

    lists_df["count_save"] = lists_df["count_save"].apply(clean_numbers).astype(int)

    posts_df = posts_df[["listed_at", "updated_at", "source_url"]]

    # Merge the two
    master_df = pd.merge(lists_df, posts_df, on="source_url")

    # Convert listed at to datetime
    def fix_listed_at(launch_date: str):
        if "added" in launch_date.lower():
            # Get the value in between
            time_added_str = (
                launch_date.lower().replace("added", "").replace("ago", "").strip()
            )

            if "h" in time_added_str:
                today = datetime.today().date()
                return today.strftime("%Y-%m-%d")

            if "d" in time_added_str:
                days_ago = time_added_str.removesuffix("d")
                date_days_ago = datetime.today() - timedelta(days=int(days_ago))
                return date_days_ago.strftime("%Y-%m-%d")

            if "m" in time_added_str:
                minutes_ago = time_added_str.removesuffix("m")
                date_minutes_ago = datetime.today() - timedelta(
                    minutes=int(minutes_ago)
                )
                return date_minutes_ago.strftime("%Y-%m-%d")

        if len(launch_date) > 10:
            non_year_date = launch_date[4:]
            non_year_date = non_year_date.replace("0", "")
            launch_date = launch_date[0:4] + non_year_date
        return launch_date

    master_df["listed_at"] = pd.to_datetime(
        master_df["listed_at"].apply(fix_listed_at), utc=True, format="%Y-%m-%d"
    )

    # Conver updated at to datetime
    def fix_updated_at(first_featured_text: str):
        date_string = first_featured_text

        date_string = (
            date_string.replace("st", "")
            .replace("nd", "")
            .replace("rd", "")
            .replace("th", "")
        )

        # Weird fix for data
        if "Augu " in date_string:
            date_string = date_string.replace("Augu", "August")

        # Define the format of the input string
        input_format = "%B %d %Y"

        # Parse the input string to a datetime object
        try:
            date_object = datetime.strptime(date_string, input_format)
        except:
            return "BAD " + date_string

        # Format the datetime object to the desired output format
        output_format = "%Y-%m-%d"
        formatted_date = date_object.strftime(output_format)
        return formatted_date

    master_df["updated_at"] = pd.to_datetime(
        master_df["updated_at"].apply(fix_updated_at), utc=True
    )

    master_df["clean_product_url"] = master_df["product_url"].apply(clean_url)
    master_df = master_df.drop_duplicates(subset="clean_product_url", keep="last")

    save_df_as_csv(master_df, out_filepath)


if __name__ == "__main__":
    main()
