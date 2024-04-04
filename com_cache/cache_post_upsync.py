import argparse
import pandas as pd
from custom_helpers_py.custom_classes.index_cache import IndexCache
from os import listdir
from os.path import join
import json


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--upsync-records-folder-path",
        type=str,
        required=True,
        dest="upsync_records_folder_path",
    )
    parser.add_argument("--run-name", type=str, required=True, dest="run_name")
    parser.add_argument(
        "--prod", action=argparse.BooleanOptionalAction, default=False, dest="prod"
    )

    args = parser.parse_args()

    upsync_records_folder_path = args.upsync_records_folder_path
    run_name = args.run_name
    is_prod = args.prod

    ic = IndexCache(prod=is_prod)

    file_name_list: list[str] = listdir(upsync_records_folder_path)
    test_str = "upsync_upsert-"
    if is_prod:
        test_str += "prod"
    else:
        test_str += "local"

    for file_name in file_name_list:
        if file_name.startswith(test_str):
            file_path = join(upsync_records_folder_path, file_name)
            with open(file_path, "r", encoding="utf-8") as in_file:
                file_contents = in_file.read()
            file_obj = json.loads(file_contents)
            uploaded_list = file_obj["uploaded_list"]
            curr_df = pd.DataFrame(uploaded_list)
            curr_df = curr_df[["product_url"]]

            curr_df["comments"] = pd.NA
            if is_prod:
                curr_df["status"] = "IN_PROD"
            else:
                curr_df["status"] = "IN_LOCAL"

            curr_df["last_run_name"] = run_name

            ic.add_data(in_df=curr_df)


if __name__ == "__main__":
    main()
