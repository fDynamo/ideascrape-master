from os.path import join
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData
from custom_helpers_py.pandas_helpers import save_df_as_json


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

    args = parser.parse_args()

    tp_folder_path: str = args.tp_folder_path
    out_folder_path: str = args.out_folder_path
    rejected_file_path: str = args.rejected_file_path
    ignore_missing_search_vector: bool = args.ignore_missing_search_vector

    tpd = TPData(folder_path=tp_folder_path)
    master_df = tpd.get_combined_parts_df(filter_rejected=False)

    # Filter rejected
    not_rejected_mask = master_df["rejected"].isna()
    rejected_df = master_df[~not_rejected_mask]
    master_df = master_df[not_rejected_mask]

    # Format df
    # TODO: Add aift and ph support
    GRAB_DICT = {
        "product_url": "",
        "title": "product_name",
        "description": "product_description",
        "local_image_file_name": "product_image_file_name",
        "search_vector": "",
        "sw_url": "",
        "sw_visits_last_month": "",
        "sw_created_at": "",
    }
    grab_col_list = list(GRAB_DICT.keys())
    master_df = master_df[grab_col_list]
    rename_dict = {}
    for col_name in grab_col_list:
        new_name = GRAB_DICT[col_name]
        if not new_name:
            continue
        rename_dict[col_name] = new_name
    master_df = master_df.rename(columns=rename_dict)

    if not ignore_missing_search_vector:

        def check_missing_search_vector(in_row: dict):
            sv = in_row["search_vector"]
            if not isinstance(sv, str):
                raise Exception("Missing search vector found", in_row["product_url"])

        master_df.apply(check_missing_search_vector, axis=1)

    master_df["upsync_action"] = "upsert"

    save_path = join(out_folder_path, "zero.json")
    save_df_as_json(master_df, save_path)

    if rejected_file_path:
        rejected_df = rejected_df[["product_url", "rejected"]]
        save_df_as_json(rejected_df, rejected_file_path)


if __name__ == "__main__":
    main()
