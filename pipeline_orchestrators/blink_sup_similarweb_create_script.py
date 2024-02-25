from os.path import join, abspath
import argparse
from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from custom_helpers_py.pipeline_preset_args_helpers import (
    add_args_for_out_folder_preset,
    parse_args_for_out_folder_preset,
)
from custom_helpers_py.get_paths import get_sup_similarweb_records_filepath
from pipeline_orchestrators.carthago_create_script import DRY_RUN_CARTHAGO_FOLDERPATH

BLINK_SUP_SIMILARWEB_SCRIPT_FILENAME = "_blink_sup_similarweb_list.txt"
BLINK_SUP_SIMILARWEB_FOLDER_PREFIX = "blink_sup_similarweb_"
DRY_RUN_BLINK_SUP_SIMILARWEB_FOLDERPATH = DRY_RUN_CARTHAGO_FOLDERPATH


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # Required
    parser.add_argument("-o", "--out-folderpath", type=str)

    # Optional
    add_args_for_out_folder_preset(parser)

    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--prod-env", action=argparse.BooleanOptionalAction)
    parser.add_argument("--upload", action=argparse.BooleanOptionalAction)

    args, _ = parser.parse_known_args()

    out_folderpath: str = args.out_folderpath

    is_dry_run: bool = args.dry_run
    is_prod_env: bool = args.prod_env
    is_upload: bool = args.upload

    if not out_folderpath:
        out_folderpath = parse_args_for_out_folder_preset(
            args, folder_prefix=BLINK_SUP_SIMILARWEB_FOLDER_PREFIX
        )
        if not out_folderpath:
            print("Invalid inputs")
            exit(1)

    out_folderpath = abspath(out_folderpath)

    # Make necessary folders
    sup_similarweb_scrape_folder = join(out_folderpath, "sup_similarweb_scrape")
    to_upload_folder = join(out_folderpath, "to_upload")
    upload_records_folder = join(out_folderpath, "upload_records")

    mkdir_if_not_exists(
        [
            out_folderpath,
            sup_similarweb_scrape_folder,
            to_upload_folder,
            upload_records_folder,
        ]
    )

    # Call cache sup similarweb records
    com_cache_records = "npm run pi_cache_sup_similarweb_records -- --reset"
    if is_prod_env:
        com_cache_records += "--prod"

    cached_similarweb_records_file_path = get_sup_similarweb_records_filepath(
        is_prod_env
    )

    # Filter domains
    domains_to_scrape_file_path = join(out_folderpath, "domains_to_scrape.csv")
    rejected_domains_file_path = join(out_folderpath, "rejected_domains.csv")

    com_filter_domains_sup_similarweb = 'python data_transformers/filter_domains_sup_similarweb.py -i "{}" --col-name "source_domain" -o "{}" -r "{}"'.format(
        cached_similarweb_records_file_path,
        domains_to_scrape_file_path,
        rejected_domains_file_path,
    )

    # Scrape similarweb
    com_scrape = 'npm run sup_similarweb_scrape -- --domainListFilepath "{}" --outFolder "{}"'.format(
        domains_to_scrape_file_path, sup_similarweb_scrape_folder
    )
    if is_dry_run:
        com_scrape = ""
        sup_similarweb_scrape_folder = join(
            DRY_RUN_BLINK_SUP_SIMILARWEB_FOLDERPATH, "sup_similarweb_scrape"
        )

    # Clean compress
    cc_file_path = join(out_folderpath, "cc_sup_similarweb_scrape.csv")
    com_cc = (
        'python data_transformers/cc_sup_similarweb_scrape.py -i "{}" -o "{}"'.format(
            sup_similarweb_scrape_folder, cc_file_path
        )
    )

    # Prodify
    com_prodify = (
        'python data_transformers/prodify_sup_similarweb.py -i "{}" -o "{}"'.format(
            cc_file_path, to_upload_folder
        )
    )

    upload_components_list = []
    if is_upload:
        prod_upload_flag = ""
        if is_prod_env:
            prod_upload_flag = " --prod"

        com_upsert_files = 'npm run pi_upsert_records -- --toUploadFolderPath "{}" --fileName "sup_similarweb" --recordsFolder "{}"{}'.format(
            to_upload_folder, upload_records_folder, prod_upload_flag
        )

        com_delete_files = (
            'npm run pi_delete_sup_similarweb -- --rejectedFilepath "{}"{}'.format(
                rejected_domains_file_path, prod_upload_flag
            )
        )

        upload_components_list = [
            "[Upsert and delete]",
            com_upsert_files,
            com_delete_files,
        ]

    if is_dry_run and is_prod_env:
        upload_components_list = []

    # Write
    to_write = [
        "[Cache]",
        com_cache_records,
        "",
        "[Filter urls]",
        com_filter_domains_sup_similarweb,
        "",
        "[Scrape similarweb]",
        com_scrape,
        "",
        "[Scrape individual formatting]",
        com_cc,
        "",
        "[Prodify]",
        com_prodify,
        "",
        *upload_components_list,
    ]

    script_list_outfile = join(out_folderpath, BLINK_SUP_SIMILARWEB_SCRIPT_FILENAME)
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))

    return script_list_outfile


if __name__ == "__main__":
    script_filepath = main()
    print(script_filepath)
