import argparse
from custom_helpers_py.custom_classes.index_cache import IndexCache
from os import listdir
from os.path import join
import json


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--in-folder-path", type=str, required=True, dest="in_folder_path"
    )
    parser.add_argument(
        "--prod", action=argparse.BooleanOptionalAction, default=False, dest="prod"
    )

    args = parser.parse_args()

    in_folder_path = args.in_folder_path

    is_prod = args.prod

    ic = IndexCache(prod=is_prod)

    file_name_list: list[str] = listdir(in_folder_path)

    for file_name in file_name_list:
        file_path = join(in_folder_path, file_name)
        with open(file_path, "r", encoding="utf-8") as in_file:
            file_contents = in_file.read()
        obj_list: list[dict] = json.loads(file_contents)
        to_save_list = []
        for obj in obj_list:
            to_add = {
                "product_url": obj["product_url"],
                "comments": None,
                "last_run_name": "cache",
                "status": "IN_PROD",
            }
            to_save_list.append(to_add)

        ic.add_data(in_list=to_save_list)


if __name__ == "__main__":
    main()
