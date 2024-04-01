from openai import OpenAI
from custom_helpers_py.string_formatters import (
    clean_text,
    format_count_percentage,
)
from dotenv import load_dotenv
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData


"""
TODO:
- Don't embed if embedding already exists
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    args = parser.parse_args()

    tp_folder_path = args.tp_folder_path

    load_dotenv()

    tpd = TPData(folder_path=tp_folder_path)
    in_df = tpd.as_df()

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
        text_to_embed = in_record["analyzed_description"]
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

        to_add = {"product_url": product_url, "search_vector": embedding}

        tpd.add_data(to_add_list=[to_add], part_name="search_vector_embeddings")

        pct = format_count_percentage(i, num_records)
        print("done", i, pct)

    print("Embeddings done!")


if __name__ == "__main__":
    main()
