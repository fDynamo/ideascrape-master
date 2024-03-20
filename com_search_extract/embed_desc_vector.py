from openai import OpenAI
from custom_helpers_py.string_formatters import (
    convert_url_to_file_name,
    clean_text,
    format_count_percentage,
)
from dotenv import load_dotenv
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
)
import argparse
from os.path import join


"""
TODO:
- Don't embed if embedding already exists
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("i", "--inFilePath", type=str, dest="in_file_path")
    parser.add_argument("-o", "--outFolderPath", type=str, dest="out_folder_path")
    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_folder_path = args.out_folder_path

    if not in_file_path or not out_folder_path:
        print("Invalid inputs")
        exit(1)

    load_dotenv()

    # Get text to embed
    in_df = read_csv_as_df(in_file_path)

    # Start embedding
    client = OpenAI()

    in_list = in_df.to_dict("records")
    num_records = len(in_list)
    if num_records == 0:
        print("Nothing to embed")
        exit(1)

    for i, in_record in enumerate(in_list):
        print("embedding", i)
        product_url = in_record["product_url"]
        text_to_embed = in_record["desc"]
        text_to_embed = clean_text(text_to_embed)
        if not text_to_embed or text_to_embed == "":
            print("empty skipping")
            continue

        embedding = (
            client.embeddings.create(
                model="text-embedding-3-small",
                input=text_to_embed,
                encoding_format="float",
            )
            .data[0]
            .embedding
        )

        embedding = str(embedding)
        save_file_path = join(
            out_folder_path, convert_url_to_file_name(product_url) + ".txt"
        )

        with open(save_file_path, "w") as out_file:
            out_file.write(embedding)

        pct = format_count_percentage(i, num_records)
        print("done", i, pct)

    print("Embeddings done!")


if __name__ == "__main__":
    main()
