from custom_helpers_py.string_formatters import (
    convert_url_to_filename,
    format_count_percentage,
)
import pandas as pd
from os.path import join, basename
import requests
from time import sleep
from custom_helpers_py.basic_time_logger import log_start, log_end
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from wand.image import Image


RECORD_FILENAME = "_record.csv"
ERROR_FILENAME = "ER"


# Download function
def download_image(to_download_url: str, save_filepath: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }
    img_data = requests.get(to_download_url, headers=headers).content
    with open(save_filepath, "wb") as handler:
        handler.write(img_data)


def transform_image(in_image_path: str, save_filepath: str):
    with Image(filename=in_image_path) as img:
        if in_image_path.endswith(".svg"):
            save_filepath = save_filepath.replace("svg", "png")
        img.resize(width=32, height=32)
        img.save(filename=save_filepath)

    return save_filepath


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    args = parser.parse_args()

    in_filepath: str = args.in_filepath
    out_folderpath: str = args.out_folderpath

    if not in_filepath or not out_folderpath:
        print("Invalid inputs")
        return

    start_time = log_start("download_product_images")

    RECORD_FILEPATH = join(out_folderpath, RECORD_FILENAME)
    RUN_DELAY = 0

    pre_extraction_df = read_csv_as_df(in_filepath)

    images_df = pre_extraction_df[["url", "product_image_url"]]
    images_df = images_df.rename(columns={"product_image_url": "image_url"})
    images_df = images_df.dropna(subset="image_url")

    # TODO: Filter on existing images
    # TODO: Filter on error files
    # TODO: Write to records csv as we go not at the end

    # Download
    allowed_extensions = [".svg", ".png", ".gif", ".ico", ".jpg", ".jpeg", ".webp"]
    to_download_list: list[str] = images_df.to_dict(orient="records")
    i = 0
    count_to_download = len(to_download_list)

    if count_to_download > 0:
        for entry in to_download_list:
            image_url = entry["image_url"]

            if not image_url or not isinstance(image_url, str) or image_url == "":
                continue

            print("downloading", i, image_url)

            # Convert url to image name
            save_image_name = convert_url_to_filename(entry["url"])

            # Get extension name
            to_save_ext = ""
            for ext in allowed_extensions:
                if ext in image_url.lower():
                    to_save_ext = ext
                    break

            if to_save_ext == "":
                entry["image_filename"] = ERROR_FILENAME
                continue

            save_image_name = save_image_name + to_save_ext
            save_filepath = join(out_folderpath, save_image_name)

            try:
                download_image(image_url, save_filepath)
                transformed_filepath = transform_image(save_filepath, save_filepath)
                transformed_filename = basename(transformed_filepath)
            except:
                entry["image_filename"] = ERROR_FILENAME
                continue

            # Save filepath in entry
            entry["image_filename"] = transformed_filename

            i += 1

            pct = format_count_percentage(i + 1, count_to_download)
            print("progress", pct)
            if RUN_DELAY > 0:
                sleep(RUN_DELAY)

        downloaded_df = pd.DataFrame(to_download_list)
        downloaded_df = downloaded_df.dropna(subset="image_filename")
        downloaded_df = downloaded_df[["url", "image_url", "image_filename"]]

        save_df_as_csv(downloaded_df, RECORD_FILEPATH)
        print(downloaded_df)
    else:
        print("Nothing to download")

    log_end(start_time)


if __name__ == "__main__":
    main()
