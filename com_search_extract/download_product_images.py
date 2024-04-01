from custom_helpers_py.string_formatters import (
    convert_url_to_file_name,
    format_count_percentage,
)
from os.path import join, basename
import requests
from time import sleep
from custom_helpers_py.url_formatters import get_domain_from_url
import re
import argparse
from wand.image import Image
from custom_helpers_py.custom_classes.tp_data import TPData


LOCAL_IMAGE_FILE_NAME_COL_NAME = "local_image_file_name"
LOCAL_IMAGE_ERROR_COL_NAME = "local_image_error"


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    parser.add_argument(
        "-o", "--out-folder-path", type=str, dest="out_folder_path", required=True
    )
    parser.add_argument(
        "--start-index", type=int, dest="start_index", required=False, default=0
    )
    args = parser.parse_args()

    tp_folder_path = args.tp_folder_path
    out_folder_path = args.out_folder_path
    start_index = args.start_index

    RUN_DELAY = 0

    tpd = TPData(tp_folder_path)

    to_download_df = tpd.as_df()
    to_download_df = to_download_df[["product_url", "page_image_url"]]
    to_download_df["page_image_url"] = to_download_df["page_image_url"].apply(
        fix_favicon_url
    )

    # Download
    allowed_extensions = [".svg", ".png", ".gif", ".ico", ".jpg", ".jpeg", ".webp"]
    to_download_list: list[str] = to_download_df.to_dict(orient="records")

    count_to_download = len(to_download_list)
    if count_to_download == 0:
        print("Nothing to download")
        exit(1)

    atleast_one_success = False
    for i, entry in enumerate(to_download_list):
        if i < start_index:
            continue

        image_url = entry["page_image_url"]

        if not image_url or not isinstance(image_url, str) or image_url == "":
            continue

        print("downloading", i, entry["product_url"])

        # Convert url to image name
        save_image_name = convert_url_to_file_name(entry["product_url"])

        # Get extension name
        to_save_ext = ""
        for ext in allowed_extensions:
            if ext in image_url.lower():
                to_save_ext = ext
                break

        if to_save_ext == "":
            entry[LOCAL_IMAGE_ERROR_COL_NAME] = "Invalid extension"
            continue

        save_image_name = save_image_name + to_save_ext
        save_file_path = join(out_folder_path, save_image_name)

        try:
            download_image(image_url, save_file_path)
            print("downloaded")
            transformed_file_path = transform_image(save_file_path, save_file_path)
            print("transformed")
            transformed_file_name = basename(transformed_file_path)
            atleast_one_success = True
        except Exception as error:
            print(error)
            entry[LOCAL_IMAGE_ERROR_COL_NAME] = str(error)
            continue

        # Save file_path in entry
        entry[LOCAL_IMAGE_FILE_NAME_COL_NAME] = transformed_file_name

        tpd.add_data(to_add_dict=entry)

        pct = format_count_percentage(i + 1, count_to_download)
        print("progress", pct)
        if RUN_DELAY > 0:
            sleep(RUN_DELAY)

    if not atleast_one_success:
        print("All downloads failed!")
        exit(1)


def fix_favicon_url(favicon_url: str, end_url: str):
    if favicon_url.startswith("http"):
        return favicon_url

    if favicon_url.startswith("//"):
        return "https:" + favicon_url

    if not end_url or not isinstance(end_url, str):
        raise Exception("Invalid end url!")

    domain = get_domain_from_url(end_url)
    favicon_url.removeprefix("/")

    to_return = domain + "/" + favicon_url
    to_return = re.sub("[/]+", "/", to_return)
    to_return = "https://" + to_return
    return to_return


# Download function
def download_image(to_download_url: str, save_file_path: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }
    img_data = requests.get(to_download_url, headers=headers).content
    with open(save_file_path, "wb") as handler:
        handler.write(img_data)


def transform_image(in_image_path: str, save_file_path: str):
    with Image(filename=in_image_path) as img:
        if in_image_path.endswith(".svg"):
            save_file_path = save_file_path.replace("svg", "png")
        img.resize(width=32, height=32)
        img.save(filename=save_file_path)

    return save_file_path


if __name__ == "__main__":
    main()
