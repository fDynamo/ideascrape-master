from openai import OpenAI
from custom_helpers_py.basic_time_logger import log_start, log_end
from custom_helpers_py.string_formatters import clean_text, format_count_percentage
from dotenv import load_dotenv
import pandas as pd
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse


"""
TODO:
- Don't embed if embedding already exists
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    in_filepath = args.in_filepath
    out_filepath = args.out_filepath

    if not in_filepath or not out_filepath:
        print("Invalid inputs")
        return

    start_time = log_start("embed_descriptions")

    load_dotenv()

    # Get text to embed
    pre_extraction_df = read_csv_as_df(in_filepath)
    text_df = pre_extraction_df.loc[:, ("description")].to_frame("text")
    text_df["text_to_embed"] = text_df["text"].apply(
        lambda x: clean_text(x, True, True)
    )

    # TODO: Filter with already embedded
    # already_embedded_df = # concat_out_folder_data_files( Here we get df with embedded data
    #     "text_embeddings", ends_with_filter="-embedding.csv"
    # )
    # merged_df = text_df.merge(
    #     already_embedded_df,
    #     on="text_to_embed",
    #     how="left",
    #     indicator=True,
    #     suffixes=("", "_dupe"),
    # )
    # text_df = merged_df[merged_df["_merge"] == "left_only"]
    # text_df = text_df.drop(
    #     columns=["_merge"] + [col for col in text_df.columns if col.endswith("_dupe")]
    # )

    # Start embedding
    client = OpenAI()

    text_records = text_df.to_dict("records")
    num_records = len(text_records)

    if num_records > 0:
        for i, in_record in enumerate(text_records):
            print("embedding", i)
            text_to_embed = in_record["text_to_embed"]
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

            in_record["embedding"] = str(embedding)

            pct = format_count_percentage(i, num_records)
            print("done", i, pct)

        final_text_df = pd.DataFrame(text_records)
        save_df_as_csv(final_text_df, out_filepath)
    else:
        print("Nothing to embed")

    log_end(start_time)


if __name__ == "__main__":
    main()
