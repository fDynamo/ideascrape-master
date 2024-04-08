from os.path import join
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData
from custom_helpers_py.pandas_helpers import save_df_as_json, grab_and_rename_columns
import pandas as pd


"""
This script combines all the data gathered to create prod tables for uploads

Algo:
- When we are cre

"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    parser.add_argument(
        "-o", "--out-folder-path", type=str, dest="out_folder_path", required=True
    )
    parser.add_argument(
        "-r", "--rejected-file-path", type=str, dest="rejected_file_path"
    )

    # some options
    parser.add_argument(
        "--ignore-missing-search-vector",
        type=bool,
        dest="ignore_missing_search_vector",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--skip-missing-search-vector",
        type=bool,
        dest="skip_missing_search_vector",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--delete-rejected",
        type=bool,
        dest="delete_rejected",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    args = parser.parse_args()

    tp_folder_path: str = args.tp_folder_path
    out_folder_path: str = args.out_folder_path
    rejected_file_path: str = args.rejected_file_path
    ignore_missing_search_vector: bool = args.ignore_missing_search_vector
    skip_missing_search_vector: bool = args.skip_missing_search_vector
    delete_rejected: bool = args.delete_rejected

    tpd = TPData(folder_path=tp_folder_path)
    master_df = tpd.get_combined_parts_df(filter_rejected=False)

    # Filter rejected
    not_rejected_mask = master_df["rejected"].isna()
    rejected_df = master_df[~not_rejected_mask]
    master_df = master_df[not_rejected_mask]

    # Format df
    # TODO: Add aift and ph support
    grab_dict = {
        "product_url": "",
        "title": "product_name",
        "description": "product_description",
        "local_image_file_name": "product_image_filename",
        "search_vector": "",
        "aift_url": "",
        "aift_count_save": "",
        "aift_listed_at": "",
        "aift_updated_at": "",
        "ph_url": "",
        "ph_count_follower": "",
        "ph_listed_at": "",
        "ph_updated_at": "",
        "sw_url": "",
        "sw_visits_last_month": "",
        "sw_created_at": "",
        "from_googleps": "",
        "googleps_count_download": "",
        "googleps_updated_at": "",
    }
    master_df = grab_and_rename_columns(master_df, grab_dict)

    if not ignore_missing_search_vector:

        def check_missing_search_vector(in_row: dict):
            sv = in_row["search_vector"]

            if not isinstance(sv, str):
                if skip_missing_search_vector:
                    return False

                raise Exception("Missing search vector found", in_row["product_url"])
            return True

        master_df["tmp"] = master_df.apply(check_missing_search_vector, axis=1)
        master_df = master_df[master_df["tmp"]]
        master_df = master_df.drop(columns=["tmp"])

    # Add upsync action
    master_df["upsync_action"] = "upsert"

    # Add updated at
    master_df["info_updated_at"] = pd.Timestamp.now()

    if rejected_file_path:
        rejected_df = rejected_df[["product_url", "rejected"]]
        save_df_as_json(rejected_df, rejected_file_path)

    if delete_rejected:
        rejected_df = rejected_df[["product_url"]]
        rejected_df["upsync_action"] = "delete"
        master_df = pd.concat([master_df, rejected_df])

    # Save
    save_path = join(out_folder_path, "zero.json")
    save_df_as_json(master_df, save_path)


if __name__ == "__main__":
    main()
