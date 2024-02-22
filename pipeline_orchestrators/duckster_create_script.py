from os.path import join, abspath
import argparse
from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from custom_helpers_py.date_helpers import get_current_date_filename
from custom_helpers_py.get_paths import get_artifacts_folder_path

DUCKSTER_SCRIPT_FILENAME = "_duckster_list.txt"


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("--combined-source-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    parser.add_argument("-n", "--new-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--prod-upload", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    in_filepath: str = args.in_filepath
    out_folderpath: str = args.out_folderpath
    combined_source_filepath: str = args.combined_source_filepath
    is_prod_upload: bool = args.prod_upload
    is_new_run: bool = args.new_run

    if not in_filepath:
        print("Invalid inputs")
        return

    if not out_folderpath:
        if is_new_run:
            folder_name = get_current_date_filename()
            artifacts_folder_path = get_artifacts_folder_path()
            out_folderpath = join(artifacts_folder_path, folder_name)
        else:
            print("Invalid inputs")
            return

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

    filter_urls_indiv_outfile = join(out_folderpath, "urls_4_indiv_scrape.csv")
    component_filter_urls_indiv = (
        'python data_transformers/filter_urls_indiv.py -i "{}" -o "{}"'.format(
            in_filepath, filter_urls_indiv_outfile
        )
    )

    component_indiv_scrape = (
        'npm run indiv_scrape -- --urlListFilepath "{}" --outFolder "{}"'.format(
            filter_urls_indiv_outfile, indiv_scrape_folder
        )
    )

    cc_indiv_scrape_outfile = join(out_folderpath, "cc_indiv_scrape.csv")
    component_cc_indiv_scrape = (
        'python data_transformers/cc_indiv_scrape.py -i "{}" -o "{}"'.format(
            indiv_scrape_folder, cc_indiv_scrape_outfile
        )
    )

    filter_indiv_scrape_outfile = join(out_folderpath, "filter_indiv_scrape.csv")
    component_filter_indiv_scrape = (
        'python data_transformers/filter_indiv_scrape.py -i "{}" -o "{}"'.format(
            cc_indiv_scrape_outfile, filter_indiv_scrape_outfile
        )
    )

    filter_indiv_scrape_urls_outfile = join(
        out_folderpath, "filter_indiv_scrape_urls.csv"
    )
    component_filter_indiv_scrape_urls = (
        'python data_transformers/util_urls_from_data.py -i "{}" -o "{}" -c url'.format(
            filter_indiv_scrape_outfile, filter_indiv_scrape_urls_outfile
        )
    )

    filter_indiv_scrape_domains_outfile = join(
        out_folderpath, "filter_indiv_scrape_domains.csv"
    )
    component_filter_indiv_scrape_domains = (
        'python data_transformers/util_domains_from_urls.py -i "{}" -o "{}"'.format(
            filter_indiv_scrape_urls_outfile, filter_indiv_scrape_domains_outfile
        )
    )

    # Similarweb
    similarweb_domains_file = join(out_folderpath, "filter_sup_similarweb_domains.csv")
    component_filter_domains_sup_similarweb = 'python data_transformers/filter_domains_sup_similarweb.py -i "{}" -o "{}"'.format(
        filter_indiv_scrape_domains_outfile, similarweb_domains_file
    )

    component_sup_similarweb_scrape = 'npm run sup_similarweb_scrape -- --domainListFilepath "{}" --outFolder "{}"'.format(
        similarweb_domains_file, sup_similarweb_scrape_folder
    )

    cc_sup_similarweb_scrape_outfile = join(
        out_folderpath, "cc_sup_similarweb_scrape.csv"
    )
    component_cc_sup_similarweb_scrape = (
        'python data_transformers/cc_sup_similarweb_scrape.py -i "{}" -o "{}"'.format(
            sup_similarweb_scrape_folder, cc_sup_similarweb_scrape_outfile
        )
    )

    # Pre extract
    gen_pre_extract_outfile = join(out_folderpath, "pre_extract.csv")
    component_gen_pre_extract = 'python data_transformers/gen_pre_extract.py --cc-indiv-scrape-filepath "{}" --cc-sup-similarweb-scrape-filepath "{}" -o "{}"'.format(
        filter_indiv_scrape_outfile,
        cc_sup_similarweb_scrape_outfile,
        gen_pre_extract_outfile,
    )

    if combined_source_filepath:
        component_gen_pre_extract = 'python data_transformers/gen_pre_extract.py --cc-indiv-scrape-filepath "{}" --cc-sup-similarweb-scrape-filepath "{}" --combined-source-filepath "{}" -o "{}"'.format(
            filter_indiv_scrape_outfile,
            cc_sup_similarweb_scrape_outfile,
            combined_source_filepath,
            gen_pre_extract_outfile,
        )

    extract_embed_description_outfile = join(
        out_folderpath, "extract_embed_description.csv"
    )
    component_extract_embed_description = (
        'python data_transformers/extract_embed_description.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, extract_embed_description_outfile
        )
    )

    component_download_product_images = (
        'python data_transformers/download_product_images.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, product_images_folder
        )
    )

    component_prodify = 'python data_transformers/prodify.py -i "{}" --embedding-description-filepath "{}" --product-images-folderpath "{}" -o "{}"'.format(
        gen_pre_extract_outfile,
        extract_embed_description_outfile,
        product_images_folder,
        prod_folder,
    )

    # Upload scripts
    # TODO: Make npm scripts here to follow convention
    prod_upload_flag = ""
    if is_prod_upload:
        prod_upload_flag = " --prod"

    upload_script_filename_list = ["sup_similarweb"]
    if combined_source_filepath:
        # TODO: Make extensible with more sources
        upload_script_filename_list += ["source_aift", "source_ph"]
    upload_script_filename_list += ["search_main"]

    upload_script_component_list = []
    for filename in upload_script_filename_list:
        to_add = 'node uploaders/upload_records.mjs --toUploadFolderPath "{}" --fileName "{}" --recordsFolder "{}"{}'.format(
            prod_folder, filename, upload_records_folder, prod_upload_flag
        )
        upload_script_component_list.append(to_add)

    upload_images = 'node uploaders/upload_images.mjs --imagesFolderPath "{}" --errorFile "{}"{}'.format(
        product_images_folder,
        join(upload_records_folder, "image_upload_errors.txt"),
        prod_upload_flag,
    )

    # Write
    to_write = [
        "[Filter urls]",
        component_filter_urls_indiv,
        "",
        "[Scrape individual formatting]",
        component_indiv_scrape,
        component_cc_indiv_scrape,
        component_filter_indiv_scrape,
        component_filter_indiv_scrape_urls,
        component_filter_indiv_scrape_domains,
        "",
        "[Scrape similarweb]",
        component_filter_domains_sup_similarweb,
        component_sup_similarweb_scrape,
        component_cc_sup_similarweb_scrape,
        "",
        "[Pre extract]",
        component_gen_pre_extract,
        "",
        "[Description and images]",
        component_extract_embed_description,
        component_download_product_images,
        "",
        "[Prodify]",
        component_prodify,
        "",
        "[Upload]",
        *upload_script_component_list,
        upload_images,
    ]

    script_list_outfile = join(out_folderpath, DUCKSTER_SCRIPT_FILENAME)
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))


if __name__ == "__main__":
    main()
