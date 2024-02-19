from custom_helpers_py.string_formatters import (
    convert_url_to_filename,
    format_count_percentage,
)
import pandas as pd
from os.path import join
import requests
from time import sleep
from custom_helpers_py.basic_time_logger import log_start, log_end
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from PIL import Image
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM


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


def transform_image(in_image_path: str, in_folderpath: str, in_image_name: str):
    src = in_image_path
    dst = in_image_path

    new_image_name = in_image_name
    if in_image_name.endswith(".svg"):
        new_image_name = in_image_name[:-3] + "png"
        tmp_filename = "tmp_" + new_image_name
        tmp_png_dst = join(in_folderpath, tmp_filename)

        rlg_file = svg2rlg(in_image_path)
        renderPM.drawToFile(rlg_file, tmp_png_dst, fmt="PNG")

        src = tmp_png_dst
        dst = join(in_folderpath, new_image_name)

    image = Image.open(src)
    new_image = image.resize((32, 32))
    new_image.save(dst)

    return [dst, new_image_name]


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

    RECORD_FILEPATH = join(out_folderpath, "record.csv")
    RUN_DELAY = 0

    pre_extraction_df = read_csv_as_df(in_filepath)

    images_df = pre_extraction_df[["url", "product_image_url"]]
    images_df = images_df.rename(columns={"product_image_url": "image_url"})
    images_df = images_df.dropna(subset="image_url")

    # TODO: Filter on existing images
    # TODO: Filter on error files

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
            save_image_name = convert_url_to_filename(image_url)

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
                [_, transformed_image_filename] = transform_image(
                    save_filepath, out_folderpath, save_image_name
                )

                # Save filepath in entry
                entry["image_filename"] = transformed_image_filename
            except:
                entry["image_filename"] = ERROR_FILENAME

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
