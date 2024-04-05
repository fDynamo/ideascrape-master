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

    if is_prod:
        env_string = "prod"
    else:
        env_string = "local"

    upsert_test_str = "upsync_upsert-" + env_string
    delete_test_str = "upsync_delete-" + env_string

    for file_name in file_name_list:
        is_upsert = file_name.startswith(upsert_test_str)
        is_delete = file_name.startswith(delete_test_str)

        if not is_upsert and not is_delete:
            continue

        file_path = join(upsync_records_folder_path, file_name)
        with open(file_path, "r", encoding="utf-8") as in_file:
            file_contents = in_file.read()
        file_obj = json.loads(file_contents)
        uploaded_list = file_obj["uploaded_list"]
        curr_df = pd.DataFrame(uploaded_list)
        curr_df = curr_df[["product_url"]]

        curr_df["comments"] = pd.NA

        if is_upsert:
            curr_df["status"] = "UPLOADED"
        if is_delete:
            curr_df["status"] = "DELETED"

        curr_df["last_run_name"] = run_name

        ic.add_data(in_df=curr_df)


if __name__ == "__main__":
    main()
