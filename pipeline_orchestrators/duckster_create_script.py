from os.path import join, abspath
import argparse
from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from custom_helpers_py.pipeline_preset_args_helpers import (
    add_args_for_out_folder_preset,
    parse_args_for_out_folder_preset,
)
from pipeline_orchestrators.carthago_create_script import DRY_RUN_CARTHAGO_FOLDERPATH
from shutil import copy

DUCKSTER_SCRIPT_FILENAME = "_duckster_list.txt"
DUCKSTER_FOLDER_PREFIX = "duckster_"
DRY_RUN_DUCKSTER_FOLDERPATH = DRY_RUN_CARTHAGO_FOLDERPATH


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # Required
    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)

    # Optional
    add_args_for_out_folder_preset(parser)
    parser.add_argument("--combined-source-filepath", type=str)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--copy-in-filepath", action=argparse.BooleanOptionalAction)
    parser.add_argument("--prod-env", action=argparse.BooleanOptionalAction)
    parser.add_argument("--upload", action=argparse.BooleanOptionalAction)

    args, _ = parser.parse_known_args()

    in_filepath: str = args.in_filepath
    out_folderpath: str = args.out_folderpath

    combined_source_filepath: str = args.combined_source_filepath
    is_dry_run: bool = args.dry_run
    is_copy_in_filepath: bool = args.copy_in_filepath
    is_prod_env: bool = args.prod_env
    is_upload: bool = args.upload

    if not in_filepath:
        print("Invalid inputs")
        exit(1)

    if not out_folderpath:
        out_folderpath = parse_args_for_out_folder_preset(
            args, folder_prefix=DUCKSTER_FOLDER_PREFIX
        )
        if not out_folderpath:
            print("Invalid inputs")
            exit(1)

    out_folderpath = abspath(out_folderpath)

    # Make necessary folders
    indiv_scrape_folder = join(out_folderpath, "indiv_scrape")
    sup_similarweb_scrape_folder = join(out_folderpath, "sup_similarweb_scrape")
    product_images_folder = join(out_folderpath, "product_images")
    prod_folder = join(out_folderpath, "to_upload")
    upload_records_folder = join(out_folderpath, "upload_records")

    mkdir_if_not_exists(
        [
            out_folderpath,
            indiv_scrape_folder,
            sup_similarweb_scrape_folder,
            product_images_folder,
            prod_folder,
            upload_records_folder,
        ]
    )

    # Copy in filepath to folder
    if is_copy_in_filepath:
        in_urls_file_copy_path = join(out_folderpath, "in_urls_file.csv")
        copy(in_filepath, in_urls_file_copy_path)

    filter_urls_indiv_outfile = join(out_folderpath, "urls_4_indiv_scrape.csv")
    com_filter_urls_indiv = (
        'python data_transformers/filter_urls_indiv.py -i "{}" -o "{}"'.format(
            in_filepath, filter_urls_indiv_outfile
        )
    )
    if is_prod_env:
        com_filter_urls_indiv += " --prod-env"

    com_indiv_scrape = (
        'npm run indiv_scrape -- --urlListFilepath "{}" --outFolder "{}"'.format(
            filter_urls_indiv_outfile, indiv_scrape_folder
        )
    )
    if is_dry_run:
        com_indiv_scrape = ""
        indiv_scrape_folder = join(DRY_RUN_DUCKSTER_FOLDERPATH, "indiv_scrape")

    cc_indiv_scrape_outfile = join(out_folderpath, "cc_indiv_scrape.csv")
    com_cc_indiv_scrape = (
        'python data_transformers/cc_indiv_scrape.py -i "{}" -o "{}"'.format(
            indiv_scrape_folder, cc_indiv_scrape_outfile
        )
    )

    filter_indiv_scrape_outfile = join(out_folderpath, "filter_indiv_scrape.csv")
    com_filter_indiv_scrape = (
        'python data_transformers/filter_indiv_scrape.py -i "{}" -o "{}"'.format(
            cc_indiv_scrape_outfile, filter_indiv_scrape_outfile
        )
    )

    filter_indiv_scrape_urls_outfile = join(
        out_folderpath, "filter_indiv_scrape_urls.csv"
    )
    com_filter_indiv_scrape_urls = (
        'python data_transformers/util_urls_from_data.py -i "{}" -o "{}" -c url'.format(
            filter_indiv_scrape_outfile, filter_indiv_scrape_urls_outfile
        )
    )

    filter_indiv_scrape_domains_outfile = join(
        out_folderpath, "filter_indiv_scrape_domains.csv"
    )
    com_filter_indiv_scrape_domains = (
        'python data_transformers/util_domains_from_urls.py -i "{}" -o "{}"'.format(
            filter_indiv_scrape_urls_outfile, filter_indiv_scrape_domains_outfile
        )
    )

    # Similarweb
    similarweb_domains_file = join(out_folderpath, "filter_sup_similarweb_domains.csv")
    com_filter_domains_sup_similarweb = 'python data_transformers/filter_domains_sup_similarweb.py -i "{}" -o "{}"'.format(
        filter_indiv_scrape_domains_outfile, similarweb_domains_file
    )

    com_sup_similarweb_scrape = 'npm run sup_similarweb_scrape -- --domainListFilepath "{}" --outFolder "{}"'.format(
        similarweb_domains_file, sup_similarweb_scrape_folder
    )
    if is_dry_run:
        com_sup_similarweb_scrape = ""
        sup_similarweb_scrape_folder = join(
            DRY_RUN_DUCKSTER_FOLDERPATH, "sup_similarweb_scrape"
        )

    cc_sup_similarweb_scrape_outfile = join(
        out_folderpath, "cc_sup_similarweb_scrape.csv"
    )
    com_cc_sup_similarweb_scrape = (
        'python data_transformers/cc_sup_similarweb_scrape.py -i "{}" -o "{}"'.format(
            sup_similarweb_scrape_folder, cc_sup_similarweb_scrape_outfile
        )
    )

    # Pre extract
    gen_pre_extract_outfile = join(out_folderpath, "pre_extract.csv")
    com_gen_pre_extract = 'python data_transformers/gen_pre_extract.py --cc-indiv-scrape-filepath "{}" --cc-sup-similarweb-scrape-filepath "{}" -o "{}"'.format(
        filter_indiv_scrape_outfile,
        cc_sup_similarweb_scrape_outfile,
        gen_pre_extract_outfile,
    )

    if combined_source_filepath:
        com_gen_pre_extract = 'python data_transformers/gen_pre_extract.py --cc-indiv-scrape-filepath "{}" --cc-sup-similarweb-scrape-filepath "{}" --combined-source-filepath "{}" -o "{}"'.format(
            filter_indiv_scrape_outfile,
            cc_sup_similarweb_scrape_outfile,
            combined_source_filepath,
            gen_pre_extract_outfile,
        )

    # Extract features
    extract_embed_description_outfile = join(
        out_folderpath, "extract_embed_description.csv"
    )
    com_extract_embed_description = (
        'python data_transformers/extract_embed_description.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, extract_embed_description_outfile
        )
    )
    if is_dry_run:
        com_extract_embed_description = ""
        extract_embed_description_outfile = join(
            DRY_RUN_DUCKSTER_FOLDERPATH, "extract_embed_description.csv"
        )

    com_download_product_images = (
        'python data_transformers/download_product_images.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, product_images_folder
        )
    )
    if is_dry_run:
        com_download_product_images = ""
        product_images_folder = join(DRY_RUN_DUCKSTER_FOLDERPATH, "product_images")

    # Prodify
    com_prodify = 'python data_transformers/prodify.py -i "{}" --embedding-description-filepath "{}" --product-images-folderpath "{}" -o "{}"'.format(
        gen_pre_extract_outfile,
        extract_embed_description_outfile,
        product_images_folder,
        prod_folder,
    )

    # Upload scripts
    # TODO: Make npm scripts here to follow convention
    upload_script_com_list = []
    if is_upload:
        upload_script_com_list.append("[Upload]")

        prod_upload_flag = ""
        if is_prod_env:
            prod_upload_flag = " --prod"

        # Generate scripts for upload
        upload_script_filename_list = ["sup_similarweb"]
        if combined_source_filepath:
            # TODO: Make extensible with more sources
            upload_script_filename_list += ["source_aift", "source_ph"]
        upload_script_filename_list += ["search_main"]

        for filename in upload_script_filename_list:
            to_add = 'npm run pi_upsert_records -- --toUploadFolderPath "{}" --fileName "{}" --recordsFolder "{}"{}'.format(
                prod_folder, filename, upload_records_folder, prod_upload_flag
            )
            upload_script_com_list.append(to_add)

        upload_images = 'npm run pi_upsert_images -- --imagesFolderPath "{}" --errorFile "{}"{}'.format(
            product_images_folder,
            join(upload_records_folder, "image_upload_errors.txt"),
            prod_upload_flag,
        )

        upload_script_com_list.append(upload_images)

    if is_dry_run and is_prod_env:
        upload_script_com_list = []

    # Write
    to_write = [
        "[Filter urls]",
        com_filter_urls_indiv,
        "",
        "[Scrape individual formatting]",
        com_indiv_scrape,
        com_cc_indiv_scrape,
        com_filter_indiv_scrape,
        com_filter_indiv_scrape_urls,
        com_filter_indiv_scrape_domains,
        "",
        "[Scrape similarweb]",
        com_filter_domains_sup_similarweb,
        com_sup_similarweb_scrape,
        com_cc_sup_similarweb_scrape,
        "",
        "[Pre extract]",
        com_gen_pre_extract,
        "",
        "[Description and images]",
        com_extract_embed_description,
        com_download_product_images,
        "",
        "[Prodify]",
        com_prodify,
        "",
        *upload_script_com_list,
    ]

    script_list_outfile = join(out_folderpath, DUCKSTER_SCRIPT_FILENAME)
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))

    return script_list_outfile


if __name__ == "__main__":
    script_filepath = main()
    print(script_filepath)
