from custom_helpers_py.string_formatters import (
    convert_url_to_file_name,
    format_count_percentage,
)
import pandas as pd
from os.path import join, basename
import requests
from time import sleep
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from wand.image import Image


RECORD_FILE_NAME = "_record.csv"
ERROR_FILE_NAME = "ER"


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


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--inFilePath", type=str, dest="in_file_path", required=True
    )
    parser.add_argument(
        "-o", "--outFolderPath", type=str, dest="out_folder_path", required=True
    )
    parser.add_argument(
        "--startIndex", type=int, dest="start_index", required=False, default=0
    )
    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_folder_path = args.out_folder_path
    start_index = args.start_index

    RECORD_FILE_PATH = join(out_folder_path, RECORD_FILE_NAME)
    RUN_DELAY = 0

    to_download_df = read_csv_as_df(in_file_path)

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

        image_url = entry["image_url"]

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
            entry["image_file_name"] = ERROR_FILE_NAME
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
            entry["image_file_name"] = ERROR_FILE_NAME
            continue

        # Save file_path in entry
        entry["image_file_name"] = transformed_file_name

        pct = format_count_percentage(i + 1, count_to_download)
        print("progress", pct)
        if RUN_DELAY > 0:
            sleep(RUN_DELAY)

    if not atleast_one_success:
        print("All downloads failed!")
        exit(1)

    downloaded_df = pd.DataFrame(to_download_list)
    downloaded_df = downloaded_df.dropna(subset="image_file_name")
    downloaded_df = downloaded_df[["product_url", "image_file_name"]]

    save_df_as_csv(downloaded_df, RECORD_FILE_PATH)
    print(downloaded_df)


if __name__ == "__main__":
    main()
