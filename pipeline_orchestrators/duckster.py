from os import mkdir
from os.path import isdir, join, abspath
import argparse


def mkdir_if_not_exists(dirpath: str):
    if not isdir(dirpath):
        mkdir(dirpath)


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    args = parser.parse_args()

    in_filepath: str = args.in_filepath
    out_folderpath: str = args.out_folderpath
    out_folderpath = abspath(out_folderpath)

    # Make necessary folders
    mkdir_if_not_exists(out_folderpath)

    indiv_scrape_folder = join(out_folderpath, "indiv_scrape")
    mkdir_if_not_exists(indiv_scrape_folder)
    sup_similarweb_scrape_folder = join(out_folderpath, "sup_similarweb_scrape")
    mkdir_if_not_exists(sup_similarweb_scrape_folder)
    product_images_folder = join(out_folderpath, "product_images")
    mkdir_if_not_exists(product_images_folder)
    prod_folder = join(out_folderpath, "to_upload")
    mkdir_if_not_exists(prod_folder)

    filter_urls_indiv_outfile = join(out_folderpath, "urls-4-indiv-scrape.csv")
    component_filter_urls_indiv = 'python filter_urls_indiv.py -i "{}" -o "{}"'.format(
        in_filepath, filter_urls_indiv_outfile
    )

    component_indiv_scrape = (
        'npm run indiv_scrape -- --urlListFilepath "{}" --outFolder "{}"'.format(
            filter_urls_indiv_outfile, indiv_scrape_folder
        )
    )

    cc_indiv_scrape_outfile = join(out_folderpath, "cc-indiv-scrape.csv")
    component_cc_indiv_scrape = 'python cc_indiv_scrape.py -i "{}" -o "{}"'.format(
        indiv_scrape_folder, cc_indiv_scrape_outfile
    )

    filter_indiv_scrape_outfile = join(out_folderpath, "filter-indiv-scrape.csv")
    component_filter_indiv_scrape = (
        'python filter_indiv_scrape.py -i "{}" -o "{}"'.format(
            cc_indiv_scrape_outfile, filter_indiv_scrape_outfile
        )
    )

    filter_indiv_scrape_urls_outfile = join(
        out_folderpath, "filter-indiv-scrape-urls.csv"
    )
    component_filter_indiv_scrape_urls = (
        'python util_urls_from_data.py -i "{}" -o "{}" -c url'.format(
            filter_indiv_scrape_outfile, filter_indiv_scrape_urls_outfile
        )
    )

    filter_indiv_scrape_domains_outfile = join(
        out_folderpath, "filter-indiv-scrape-domains.csv"
    )
    component_filter_indiv_scrape_domains = (
        'python util_domains_from_urls.py -i "{}" -o "{}"'.format(
            filter_indiv_scrape_urls_outfile, filter_indiv_scrape_domains_outfile
        )
    )

    component_sup_similarweb_scrape = 'npm run sup_similarweb_scrape -- --domainListFilepath "{}" --outFolder "{}"'.format(
        filter_indiv_scrape_domains_outfile, sup_similarweb_scrape_folder
    )

    cc_sup_similarweb_scrape_outfile = join(
        out_folderpath, "cc-sup-similarweb-scrape.csv"
    )
    component_cc_sup_similarweb_scrape = (
        'python cc_sup_similarweb_scrape.py -i "{}" -o "{}"'.format(
            sup_similarweb_scrape_folder, cc_sup_similarweb_scrape_outfile
        )
    )

    gen_pre_extract_outfile = join(out_folderpath, "pre-extract.csv")
    component_gen_pre_extract = 'python gen_pre_extract.py --cc-indiv-scrape-filepath "{}" --cc-sup-similarweb-scrape-filepath "{}" -o "{}"'.format(
        filter_indiv_scrape_outfile,
        cc_sup_similarweb_scrape_outfile,
        gen_pre_extract_outfile,
    )

    extract_embed_description_outfile = join(
        out_folderpath, "extract-embed-description.csv"
    )
    component_extract_embed_description = (
        'python extract_embed_description.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, extract_embed_description_outfile
        )
    )

    component_download_product_images = (
        'python download_product_images.py -i "{}" -o "{}"'.format(
            gen_pre_extract_outfile, product_images_folder
        )
    )

    component_prodify = 'python prodify.py -i "{}" --embedding-description-filepath "{}" --product-images-folderpath "{}" -o "{}"'.format(
        gen_pre_extract_outfile,
        extract_embed_description_outfile,
        product_images_folder,
        prod_folder,
    )

    upload_script_sup_similarweb = 'node add_records.mjs --toUploadPath "{}" --fileName "{}" --recordsFolder "{}" --prod'.format(
        prod_folder, "sup_similarweb", "./records/"
    )

    upload_script_search_main = 'node add_records.mjs --toUploadPath "{}" --fileName "{}" --recordsFolder "{}" --prod'.format(
        prod_folder, "search_main", "./records/"
    )

    upload_images = (
        'node upload_images.mjs --imagesFolderPath "{}" --errorFile "{}" --prod'.format(
            product_images_folder, "./records/image_upload_errors.txt"
        )
    )

    # Write
    to_write = [
        "[Filter urls]",
        component_filter_urls_indiv,
        "[Scrape individual]",
        component_indiv_scrape,
        component_cc_indiv_scrape,
        component_filter_indiv_scrape,
        component_filter_indiv_scrape_urls,
        component_filter_indiv_scrape_domains,
        "[Scrape similarweb]",
        component_sup_similarweb_scrape,
        component_cc_sup_similarweb_scrape,
        "[Pre extract]",
        component_gen_pre_extract,
        "[Description and images]",
        component_extract_embed_description,
        component_download_product_images,
        "[Prodify]",
        component_prodify,
        "[Upload]",
        upload_script_sup_similarweb,
        upload_script_search_main,
        upload_images,
    ]

    script_list_outfile = join(out_folderpath, "_script_list.txt")
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write))


if __name__ == "__main__":
    main()
