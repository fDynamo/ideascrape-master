from os.path import join, abspath
import argparse
from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from custom_helpers_py.pipeline_components_helpers import SCRIPT_RUN_STOPPER
from custom_helpers_py.pipeline_preset_args_helpers import (
    add_args_for_out_folder_preset,
    parse_args_for_out_folder_preset,
)
from custom_helpers_py.custom_classes.script_component import ScriptComponent

BLINK_REMAP_SEARCH_MAIN_IDS_SCRIPT_FILENAME = "_blink_remap_search_main_ids_list.txt"
BLINK_REMAP_SEARCH_MAIN_IDS_FOLDER_PREFIX = "blink_remap_search_main_ids_"


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # Required
    parser.add_argument("-o", "--out-folderpath", type=str)

    # Optional
    add_args_for_out_folder_preset(parser)

    parser.add_argument(
        "--prod-env", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--upload", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--skip-cache", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--dont-run-upload", action=argparse.BooleanOptionalAction, default=False
    )

    args, _ = parser.parse_known_args()

    out_folderpath: str = args.out_folderpath

    is_prod_env: bool = args.prod_env
    is_upload: bool = args.upload
    is_skip_cache: bool = args.skip_cache
    is_dont_run_upload: bool = args.dont_run_upload

    if not out_folderpath:
        out_folderpath = parse_args_for_out_folder_preset(
            args, folder_prefix=BLINK_REMAP_SEARCH_MAIN_IDS_FOLDER_PREFIX
        )
        if not out_folderpath:
            print("Invalid inputs")
            exit(1)

    out_folderpath = abspath(out_folderpath)
    upload_records_folder = join(out_folderpath, "upload_records")

    mkdir_if_not_exists([out_folderpath, upload_records_folder])

    # Call cache search main records
    com_cache_search_main_records = ScriptComponent(
        body="npm run pi_cache_search_main_records",
        args=[["reset"], ["prod", is_prod_env]],
    )

    if is_skip_cache:
        com_cache_search_main_records.erase_component()

    # Call cache search main records
    com_cache_sup_similarweb_records = ScriptComponent(
        body="npm run pi_cache_sup_similarweb_records",
        args=[["reset"], ["prod", is_prod_env]],
    )

    if is_skip_cache:
        com_cache_sup_similarweb_records.erase_component()

    # Call remap search main
    remap_filepath = join(out_folderpath, "remapped_data.csv")
    com_remap_search_main_ids = ScriptComponent(
        body="python com_special/remap_search_main_ids.py",
        args=[["o", remap_filepath], ["prod-env", is_prod_env]],
    )

    # Call update
    upload_com_list = []

    if is_upload:
        com_update_search_main_ids = ScriptComponent(
            body="npm run pi_update_search_main_ids",
            args=[
                ["inputFilePath", remap_filepath],
                ["prod", is_prod_env],
                ["recordsFolder", upload_records_folder],
            ],
        )

        if is_dont_run_upload:
            upload_com_list = [SCRIPT_RUN_STOPPER]

        upload_com_list += ["[Update]", com_update_search_main_ids]

    # Write
    to_write = [
        "[search_main cache]",
        com_cache_search_main_records,
        com_cache_sup_similarweb_records,
        com_remap_search_main_ids,
        *upload_com_list,
    ]

    to_write = [str(val) for val in to_write]

    script_list_outfile = join(
        out_folderpath, BLINK_REMAP_SEARCH_MAIN_IDS_SCRIPT_FILENAME
    )
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))

    return script_list_outfile


if __name__ == "__main__":
    script_filepath = main()
    print(script_filepath)
