from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from os.path import abspath, join
import argparse
from custom_helpers_py.get_paths import get_artifacts_folder_path
from custom_helpers_py.pipeline_preset_args_helpers import (
    add_args_for_out_folder_preset,
    parse_args_for_out_folder_preset,
)

CARTHAGO_SCRIPT_FILENAME = "_carthago_list.txt"
DRY_RUN_CARTHAGO_FOLDERPATH = join(get_artifacts_folder_path(), "_dry_run_carthago")


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # Required
    parser.add_argument("-o", "--out-folderpath", type=str)

    # Optional
    add_args_for_out_folder_preset(parser)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--prod", action=argparse.BooleanOptionalAction)
    parser.add_argument("--upload", action=argparse.BooleanOptionalAction)

    args, _ = parser.parse_known_args()

    out_folderpath: str = args.out_folderpath

    is_dry_run: bool = args.dry_run
    is_prod_env: bool = args.prod
    is_upload: bool = args.upload

    if not out_folderpath:
        out_folderpath = parse_args_for_out_folder_preset(
            args, folder_prefix="carthago"
        )
        if not out_folderpath:
            print("Invalid inputs")
            exit(1)

    out_folderpath = abspath(out_folderpath)

    # Declare source acronyms
    SOURCE_ACRONYMS = ["ph", "aift"]

    # Make necessary folders
    folder_source_scrapes = join(out_folderpath, "source_scrapes")
    if is_dry_run:
        folder_source_scrapes = join(DRY_RUN_CARTHAGO_FOLDERPATH, "source_scrapes")

    source_scrape_folders_list = [
        join(folder_source_scrapes, "source_{}_scrape".format(acr))
        for acr in SOURCE_ACRONYMS
    ]
    folder_cc_source_scrapes = join(out_folderpath, "cc_source_scrapes")
    folder_source_scrape_urls = join(out_folderpath, "source_scrape_urls")

    mkdir_if_not_exists(
        [
            out_folderpath,
            folder_source_scrapes,
            folder_cc_source_scrapes,
            folder_source_scrape_urls,
        ]
        + source_scrape_folders_list
    )

    to_write_list = []

    for i, acr in enumerate(SOURCE_ACRONYMS):
        folder_out_scrape = source_scrape_folders_list[i]

        scrape_component = 'npm run source_{}_scrape -- --outFolder "{}"'.format(
            acr, folder_out_scrape
        )
        if is_dry_run:
            scrape_component = ""

        cc_out_filename = "cc_source_{}_scrape.csv".format(acr)
        cc_out_filepath = join(folder_cc_source_scrapes, cc_out_filename)
        cc_component = 'python com_cc/cc_source_{}_scrape.py -i "{}" -o "{}"'.format(
            acr, folder_out_scrape, cc_out_filepath
        )

        urls_out_filename = "source_{}_urls.csv".format(acr)
        urls_out_filepath = join(folder_source_scrape_urls, urls_out_filename)
        urls_component = (
            'python com_utils/util_urls_from_data.py -i "{}" -o "{}"'.format(
                cc_out_filepath, urls_out_filepath
            )
        )

        to_write_list += [
            "[{} scrape and CC]".format(acr),
            scrape_component,
            cc_component,
            urls_component,
            "",
        ]

    # Combine data
    combined_source_filepath = join(out_folderpath, "combined_source.csv")
    combine_source_component = (
        'python com_cc/combine_source_cc.py -i "{}" -o "{}"'.format(
            folder_cc_source_scrapes, combined_source_filepath
        )
    )

    # Combine urls
    combined_urls_filepath = join(out_folderpath, "scraped_urls_list.csv")
    combine_urls_component = (
        'python com_utils/util_combine_urls.py -i "{}" -o "{}"'.format(
            folder_source_scrape_urls, combined_urls_filepath
        )
    )

    ending_flags = ""
    if is_dry_run:
        ending_flags += " --dry-run"
    if is_prod_env:
        ending_flags += " --prod-env"
    if is_upload:
        ending_flags += " --upload"

    # Call duckster
    duckster_call = 'python old_pipeline_definitions/duckster_create_script.py -i "{}" --combined-source-filepath "{}" -o "{}"{}'.format(
        combined_urls_filepath,
        combined_source_filepath,
        out_folderpath,
        ending_flags,
    )

    # Write
    to_write_list += [
        "[Combine data and urls]",
        combine_source_component,
        combine_urls_component,
        "",
        "[Duckster call]",
        duckster_call,
        "",
    ]

    script_list_outfile = join(out_folderpath, CARTHAGO_SCRIPT_FILENAME)
    with open(script_list_outfile, "w", encoding="utf-8") as file:
        file.write("\n\n".join(to_write_list))

    return script_list_outfile


if __name__ == "__main__":
    script_filepath = main()
    print(script_filepath)
