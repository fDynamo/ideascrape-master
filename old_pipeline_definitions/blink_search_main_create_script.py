from os.path import join, abspath
import argparse
from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from custom_helpers_py.pipeline_components_helpers import SCRIPT_RUN_STOPPER
from custom_helpers_py.pipeline_preset_args_helpers import (
    add_args_for_out_folder_preset,
    parse_args_for_out_folder_preset,
)
from custom_helpers_py.get_paths import (
    get_search_main_records_filepath,
)
from old_pipeline_definitions.carthago_create_script import DRY_RUN_CARTHAGO_FOLDERPATH
from custom_helpers_py.custom_classes.script_component import ScriptComponent

BLINK_SEARCH_MAIN_SCRIPT_FILENAME = "_blink_search_main_list.txt"
BLINK_SEARCH_MAIN_FOLDER_PREFIX = "blink_search_main_"
DRY_RUN_BLINK_SEARCH_MAIN_FOLDERPATH = DRY_RUN_CARTHAGO_FOLDERPATH


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # Required
    parser.add_argument("-o", "--out-folderpath", type=str)

    # Optional
    add_args_for_out_folder_preset(parser)

    parser.add_argument(
        "--dry-run", action=argparse.BooleanOptionalAction, default=False
    )
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

    is_dry_run: bool = args.dry_run
    is_prod_env: bool = args.prod_env
    is_upload: bool = args.upload
    is_skip_cache: bool = args.skip_cache
    is_dont_run_upload: bool = args.dont_run_upload

    if not out_folderpath:
        out_folderpath = parse_args_for_out_folder_preset(
            args, folder_prefix=BLINK_SEARCH_MAIN_FOLDER_PREFIX
        )
        if not out_folderpath:
            print("Invalid inputs")
            exit(1)

    out_folderpath = abspath(out_folderpath)

    # Make necessary folders
    rejected_urls_folder = join(out_folderpath, "rejected_urls")
    indiv_scrape_folder = join(out_folderpath, "indiv_scrape")
    product_images_folder = join(out_folderpath, "product_images")
    to_upload_folder = join(out_folderpath, "to_upload")
    upload_records_folder = join(out_folderpath, "upload_records")

    mkdir_if_not_exists(
        [
            out_folderpath,
            rejected_urls_folder,
            indiv_scrape_folder,
            product_images_folder,
            to_upload_folder,
            upload_records_folder,
        ]
    )

    # Call cache search main records
    com_cache_search_main_records = ScriptComponent(
        body="npm run pi_cache_search_main_records",
        args=[["reset"], ["prod", is_prod_env]],
    )

    if is_skip_cache:
        com_cache_search_main_records.erase_component()

    cached_search_main_records_file_path = get_search_main_records_filepath(is_prod_env)

    # Filter urls
    urls_to_scrape_filepath = join(out_folderpath, "urls_4_indiv_scrape.csv")
    rejected_urls_from_filter_urls_filepath = join(
        rejected_urls_folder, "from_filter_urls.csv"
    )
    com_filter_urls_indiv = ScriptComponent(
        body="python com_filters/filter_urls_indiv.py",
        args=[
            ["i", cached_search_main_records_file_path],
            ["o", urls_to_scrape_filepath],
            ["r", rejected_urls_from_filter_urls_filepath],
            ["c", "product_url"],
            ["prod-env", is_prod_env],
            ["ignore-cache"],
        ],
    )

    # Scrape
    com_scrape_indiv = ScriptComponent(
        body="npm run indiv_scrape",
        args=[
            ["urlListFilepath", urls_to_scrape_filepath],
            ["outFolder", indiv_scrape_folder],
        ],
    )
    if is_dry_run:
        com_scrape_indiv.erase_component()
        indiv_scrape_folder = join(DRY_RUN_BLINK_SEARCH_MAIN_FOLDERPATH, "indiv_scrape")

    # Get failed urls
    rejected_urls_from_failed_scrape_filepath = join(
        rejected_urls_folder, "from_failed_scrape.csv"
    )
    com_grab_failed_urls = ScriptComponent(
        body="python com_special/grab_failed_urls_indiv_scrape.py",
        args=[
            ["i", indiv_scrape_folder],
            ["o", rejected_urls_from_failed_scrape_filepath],
        ],
    )

    # Clean compress
    cc_indiv_scrape_filepath = join(out_folderpath, "cc_indiv_scrape.csv")
    com_cc_indiv_scrape = ScriptComponent(
        body="python com_cc/cc_indiv_scrape.py",
        args=[["i", indiv_scrape_folder], ["o", cc_indiv_scrape_filepath]],
    )

    # Filter indiv scrape
    filtered_indiv_scrape_filepath = join(out_folderpath, "filtered_indiv_scrape.csv")
    rejected_urls_from_filter_indiv_scrape_filepath = join(
        rejected_urls_folder, "from_filter_indiv_scrape.csv"
    )
    com_filter_indiv_scrape = ScriptComponent(
        body="python com_filters/filter_indiv_scrape.py",
        args=[
            ["i", cc_indiv_scrape_filepath],
            ["o", filtered_indiv_scrape_filepath],
            ["r", rejected_urls_from_filter_indiv_scrape_filepath],
        ],
    )

    # Combine failed urls
    full_rejected_urls_filepath = join(out_folderpath, "full_rejected_urls.csv")
    com_combine_rejected_urls = ScriptComponent(
        body="python com_utils/util_combine_columns_from_folder.py",
        args=[
            ["i", rejected_urls_folder],
            ["o", full_rejected_urls_filepath],
            ["c", "url"],
        ],
    )

    # Generate pre extract
    pre_extract_filepath = join(out_folderpath, "pre_extract.csv")
    com_pre_extract = ScriptComponent(
        body="python com_special/gen_pre_extract.py",
        args=[
            ["cc-indiv-scrape-filepath", cc_indiv_scrape_filepath],
            ["o", pre_extract_filepath],
        ],
    )

    # Extract embeddings
    desc_embeddings_filepath = join(out_folderpath, "extract_embed_description.csv")
    com_extract_embed_description = ScriptComponent(
        body="python com_search_extract/extract_embed_description.py",
        args=[
            ["i", pre_extract_filepath],
            ["o", desc_embeddings_filepath],
        ],
    )
    if is_dry_run:
        com_extract_embed_description.erase_component()
        desc_embeddings_filepath = join(
            DRY_RUN_BLINK_SEARCH_MAIN_FOLDERPATH, "extract_embed_description.csv"
        )

    # Download product images
    com_download_product_images = ScriptComponent(
        body="python com_search_extract/download_product_images.py",
        args=[
            ["i", pre_extract_filepath],
            ["o", product_images_folder],
        ],
    )
    if is_dry_run:
        com_download_product_images.erase_component()
        product_images_folder = join(
            DRY_RUN_BLINK_SEARCH_MAIN_FOLDERPATH, "product_images"
        )

    # Prodify
    com_prodify = ScriptComponent(
        body="python com_special/prodify.py",
        args=[
            ["i", pre_extract_filepath],
            ["o", to_upload_folder],
            ["embedding-description-filepath", desc_embeddings_filepath],
            ["product-images-folderpath", product_images_folder],
        ],
    )

    upload_components_list = []
    if is_upload:
        # Upsert product images
        com_upsert_product_images = ScriptComponent(
            body="npm run pi_upsert_images",
            args=[
                ["imagesFolderPath", product_images_folder],
                ["recordsFolder", upload_records_folder],
                ["prod", is_prod_env],
            ],
        )

        # Upload new urls
        com_upsert_records = ScriptComponent(
            body="npm run pi_upsert_records",
            args=[
                ["toUploadFolderPath", to_upload_folder],
                ["fileName", "search_main"],
                ["recordsFolder", upload_records_folder],
                ["searchMainOnly"],
                ["prod", is_prod_env],
            ],
        )

        # Delete rejected urls

        # deleteListFilePath, prod, recordsFolder
        com_delete_rejected = ScriptComponent(
            body="npm run pi_delete_search_main",
            args=[
                ["deleteListFilePath", full_rejected_urls_filepath],
                ["recordsFolder", upload_records_folder],
                ["prod", is_prod_env],
            ],
        )

        upload_components_list = [
            "[Upload]",
            com_upsert_product_images,
            com_upsert_records,
            com_delete_rejected,
        ]

        if is_dont_run_upload:
            upload_components_list = [SCRIPT_RUN_STOPPER] + upload_components_list

    if is_dry_run and is_prod_env:
        upload_components_list = []

    # Write
    to_write = [
        "[search_main cache]",
        com_cache_search_main_records,
        "",
        "[Filter urls cache]",
        com_filter_urls_indiv,
        "",
        "[Scrape]",
        com_scrape_indiv,
        com_cc_indiv_scrape,
        "",
        "[More filtering]",
        com_grab_failed_urls,
        com_filter_indiv_scrape,
        com_combine_rejected_urls,
        "",
        "[Extract]",
        com_pre_extract,
        com_extract_embed_description,
        com_download_product_images,
        "",
        "[Prodify]",
        com_prodify,
        "",
        *upload_components_list,
    ]

    to_write = [str(val) for val in to_write]

    script_list_outfile = join(out_folderpath, BLINK_SEARCH_MAIN_SCRIPT_FILENAME)
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))

    return script_list_outfile


if __name__ == "__main__":
    script_filepath = main()
    print(script_filepath)
