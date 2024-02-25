from custom_helpers_py.folder_helpers import mkdir_if_not_exists
from os.path import abspath, join
from os import listdir
import argparse
from custom_helpers_py.date_helpers import get_current_date_filename
from custom_helpers_py.get_paths import get_artifacts_folder_path

CARTHAGO_SCRIPT_FILENAME = "_carthago_list.txt"
DRY_RUN_CARTHAGO_FOLDERPATH = join(get_artifacts_folder_path(), "_dry_run_carthago")


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--out-folderpath", type=str)
    parser.add_argument("-n", "--new-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--recent-run", action=argparse.BooleanOptionalAction)
    parser.add_argument("--local-upload", action=argparse.BooleanOptionalAction)
    parser.add_argument("--prod-upload", action=argparse.BooleanOptionalAction)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction)
    args, _ = parser.parse_known_args()

    out_folderpath: str = args.out_folderpath
    is_prod_upload: bool = args.prod_upload
    is_new_run: bool = args.new_run
    is_dry_run: bool = args.dry_run
    is_local_upload: bool = args.local_upload
    is_recent_run: bool = args.recent_run

    if not out_folderpath:
        if is_recent_run:
            artifacts_folder_path = get_artifacts_folder_path()
            artifacts_contents = listdir(artifacts_folder_path)
            most_recent = ""
            for folderpath in artifacts_contents:
                if folderpath.startswith("2"):
                    if not most_recent:
                        most_recent = folderpath
                    elif folderpath > most_recent:
                        most_recent = folderpath
            out_folderpath = join(artifacts_folder_path, most_recent)
        elif is_new_run:
            folder_name = get_current_date_filename()
            artifacts_folder_path = get_artifacts_folder_path()
            out_folderpath = join(artifacts_folder_path, folder_name)
        else:
            print("Invalid inputs")
            return None

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
        cc_component = (
            'python data_transformers/cc_source_{}_scrape.py -i "{}" -o "{}"'.format(
                acr, folder_out_scrape, cc_out_filepath
            )
        )

        urls_out_filename = "source_{}_urls.csv".format(acr)
        urls_out_filepath = join(folder_source_scrape_urls, urls_out_filename)
        urls_component = (
            'python data_transformers/util_urls_from_data.py -i "{}" -o "{}"'.format(
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
        'python data_transformers/gen_combined_source.py -i "{}" -o "{}"'.format(
            folder_cc_source_scrapes, combined_source_filepath
        )
    )

    # Combine urls
    combined_urls_filepath = join(out_folderpath, "scraped_urls_list.csv")
    combine_urls_component = (
        'python data_transformers/util_combine_urls.py -i "{}" -o "{}"'.format(
            folder_source_scrape_urls, combined_urls_filepath
        )
    )

    ending_flags = ""
    if is_prod_upload:
        ending_flags += " --prod-upload"
    if is_dry_run:
        ending_flags += " --dry-run"
    if is_local_upload:
        ending_flags += " --local-upload"

    # Call duckster
    duckster_call = 'python pipeline_orchestrators/duckster_create_script.py -i "{}" --combined-source-filepath "{}" -o "{}"{}'.format(
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
