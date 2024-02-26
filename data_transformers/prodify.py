from os.path import join
from custom_helpers_py.string_formatters import clean_text
from custom_helpers_py.basic_time_logger import log_start, log_end
from download_product_images import ERROR_FILENAME, RECORD_FILENAME
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.df_validator import validate_prod_sup_similarweb_df


"""
This script combines all the data from compressed source scrapes + individual scrapes + similarweb scrapes

And performs final round of filtering if necessary
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--embedding-description-filepath", type=str)
    parser.add_argument("--product-images-folderpath", type=str)
    parser.add_argument("-i", "--in-pre-extraction-filepath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    args = parser.parse_args()

    embedding_description_filepath: str = args.embedding_description_filepath
    product_images_folderpath: str = args.product_images_folderpath
    in_pre_extraction_filepath: str = args.in_pre_extraction_filepath
    out_folderpath: str = args.out_folderpath

    if (
        not embedding_description_filepath
        or not product_images_folderpath
        or not in_pre_extraction_filepath
        or not out_folderpath
    ):
        print("Invalid inputs")
        return

    start_time = log_start()

    pre_extraction_df = read_csv_as_df(in_pre_extraction_filepath)

    print("start embeddings")
    """
    Embeddings
    """
    embeddings_df = read_csv_as_df(embedding_description_filepath)
    embeddings_df = embeddings_df.rename(
        columns={
            "embedding": "description_embedding",
            "text": "tmp_text_to_embed",
        }
    )
    embeddings_df = embeddings_df.drop(columns="text_to_embed")
    embeddings_df["tmp_text_to_embed"] = embeddings_df["tmp_text_to_embed"].apply(
        lambda x: clean_text(
            x, remove_html=True, remove_non_alpha=True, remove_commas=True
        )
    )

    pre_extraction_df["tmp_clean_description"] = pre_extraction_df["description"].apply(
        lambda x: clean_text(
            x, remove_html=True, remove_non_alpha=True, remove_commas=True
        )
    )

    master_df = pre_extraction_df.merge(
        embeddings_df,
        left_on="tmp_clean_description",
        right_on="tmp_text_to_embed",
        how="left",
    )

    master_df = master_df[~master_df["description_embedding"].isna()]

    # Drop columns
    cols_to_remove = [col for col in master_df.columns if col.startswith("tmp_")]
    master_df = master_df.drop(columns=cols_to_remove)

    print("start images")
    """
    Product images
    """
    IMAGE_RECORDS_FILEPATH = join(product_images_folderpath, RECORD_FILENAME)
    image_records_df = read_csv_as_df(IMAGE_RECORDS_FILEPATH)

    master_df = master_df.merge(image_records_df, on="url", how="left")
    # master_df now has image_filename

    # Filter error filenames
    def filter_filenames(in_filename: str):
        if in_filename == ERROR_FILENAME:
            return None
        else:
            return in_filename

    master_df["image_filename"] = master_df["image_filename"].apply(filter_filenames)

    print("start sources")
    """
    Get source dfs
    """
    cols_list = list(master_df.columns)

    has_source_aift = "aift_source_url" in cols_list
    has_source_ph = "ph_source_url" in cols_list
    has_sup_similarweb = "similarweb_data_created_at" in cols_list

    # AIFT
    source_aift_df = None
    if has_source_aift:
        aift_cols = [col for col in master_df.columns if col.startswith("aift_")]
        source_aift_df = master_df[aift_cols]
        aift_cols = [col.removeprefix("aift_") for col in aift_cols]
        source_aift_df.columns = aift_cols
        source_aift_df = source_aift_df.dropna(subset="source_url")
        source_aift_df = source_aift_df.drop_duplicates(subset="source_url")
        source_aift_df = source_aift_df.reset_index(drop=True)
        source_aift_df["count_save"] = source_aift_df["count_save"].astype(int)
        source_aift_df = source_aift_df.drop(columns="product_url")
        aift_cols = ["source_url", "count_save", "listed_at", "updated_at"]
        source_aift_df = source_aift_df[aift_cols]
        source_aift_df = source_aift_df.rename(
            columns={
                "listed_at": "product_listed_at",
                "updated_at": "product_updated_at",
            }
        )

    # PH
    source_ph_df = None
    if has_source_ph:
        ph_cols = [col for col in master_df.columns if col.startswith("ph_")]
        source_ph_df = master_df[ph_cols].reset_index(drop=True)
        ph_cols = [col.removeprefix("ph_") for col in ph_cols]
        source_ph_df.columns = ph_cols
        source_ph_df = source_ph_df.dropna(subset="source_url")
        source_ph_df = source_ph_df.drop_duplicates(subset="source_url")
        source_ph_df = source_ph_df.reset_index(drop=True)
        source_ph_df["count_follower"] = source_ph_df["count_follower"].astype(int)
        source_ph_df = source_ph_df.drop(columns="product_url")
        ph_cols = ["source_url", "count_follower", "listed_at", "updated_at"]
        source_ph_df = source_ph_df[ph_cols]
        source_ph_df = source_ph_df.rename(
            columns={
                "listed_at": "product_listed_at",
                "updated_at": "product_updated_at",
            }
        )

    """
    Get sup dfs
    """
    # SimilarWeb
    sup_similarweb_df = None
    if has_sup_similarweb:
        similarweb_cols = [
            "product_domain",
            "similarweb_total_visits_last_month",
            "similarweb_data_created_at",
        ]
        sup_similarweb_df = master_df[similarweb_cols].reset_index(drop=True)
        similarweb_cols = [col.removeprefix("similarweb_") for col in similarweb_cols]
        sup_similarweb_df.columns = similarweb_cols
        sup_similarweb_df = sup_similarweb_df.rename(
            columns={"product_domain": "source_domain"}
        )
        sup_similarweb_df = sup_similarweb_df.dropna(
            subset=["source_domain", "data_created_at"]
        )
        sup_similarweb_df = sup_similarweb_df.drop_duplicates(subset="source_domain")
        sup_similarweb_df = sup_similarweb_df.reset_index(drop=True)
        sup_similarweb_df["total_visits_last_month"] = sup_similarweb_df[
            "total_visits_last_month"
        ].astype(int)
        similarweb_cols = [
            "source_domain",
            "total_visits_last_month",
            "data_created_at",
        ]
        sup_similarweb_df = sup_similarweb_df[similarweb_cols]

        validate_prod_sup_similarweb_df(sup_similarweb_df)

    print("start search main")

    """
    Get search main
    """
    """
    Get ids
    """
    # AIFT
    if has_source_aift:
        aift_id_df = source_aift_df.rename_axis("aift_id").reset_index()
        aift_id_df["aift_id"] = aift_id_df["aift_id"].apply(lambda x: x + 1)
        aift_id_df = aift_id_df[["aift_id", "source_url"]]
        master_df = master_df.merge(
            aift_id_df,
            left_on="aift_source_url",
            right_on="source_url",
            how="left",
            suffixes=("", "_aift"),
        )
    else:
        master_df["aift_id"] = None

    # PH
    if has_source_ph:
        ph_id_df = source_ph_df.rename_axis("ph_id").reset_index()
        ph_id_df["ph_id"] = ph_id_df["ph_id"].apply(lambda x: x + 1)
        ph_id_df = ph_id_df[["ph_id", "source_url"]]
        master_df = master_df.merge(
            ph_id_df,
            left_on="ph_source_url",
            right_on="source_url",
            how="left",
            suffixes=("", "_ph"),
        )
    else:
        master_df["ph_id"] = None

    # SimilarWeb
    if has_sup_similarweb:
        similarweb_id_df = sup_similarweb_df.rename_axis("similarweb_id").reset_index()
        similarweb_id_df["similarweb_id"] = similarweb_id_df["similarweb_id"].apply(
            lambda x: x + 1
        )
        similarweb_id_df = similarweb_id_df[["similarweb_id", "source_domain"]]
        master_df = master_df.merge(
            similarweb_id_df,
            left_on="product_domain",
            right_on="source_domain",
            how="left",
            suffixes=("", "_similarweb"),
        )
    else:
        master_df["similarweb_id"] = None

    # Drop duplicates
    master_df = master_df.drop_duplicates(subset="url")

    # Get actual df
    search_main_cols = [
        "url",
        "title",
        "description",
        "image_filename",
        "description_embedding",
        "aift_id",
        "ph_id",
        "similarweb_id",
    ]
    search_main_df = master_df[search_main_cols]
    search_main_cols = [
        "product_url",
        "product_name",
        "product_description",
        "product_image_filename",
        "product_description_embedding",
        "aift_id",
        "ph_id",
        "similarweb_id",
    ]
    search_main_df.columns = search_main_cols

    # Set types
    if has_source_aift:
        search_main_df["aift_id"] = search_main_df["aift_id"].astype("Int64")
    if has_source_ph:
        search_main_df["ph_id"] = search_main_df["ph_id"].astype("Int64")
    if has_sup_similarweb:
        search_main_df["similarweb_id"] = search_main_df["similarweb_id"].astype(
            "Int64"
        )

    print("start saving")
    """
    Save all dfs
    """
    search_main_df_savepath = join(out_folderpath, "search_main.csv")
    save_df_as_csv(search_main_df, search_main_df_savepath)

    search_main_no_embedding_df = search_main_df.drop(
        columns="product_description_embedding"
    )
    search_main_no_embedding_df_savepath = join(
        out_folderpath, "search_main_no_embedding.csv"
    )
    save_df_as_csv(search_main_no_embedding_df, search_main_no_embedding_df_savepath)

    if has_source_aift:
        source_aift_df_savepath = join(out_folderpath, "source_aift.csv")
        save_df_as_csv(source_aift_df, source_aift_df_savepath)

    if has_source_ph:
        source_ph_df_savepath = join(out_folderpath, "source_ph.csv")
        save_df_as_csv(source_ph_df, source_ph_df_savepath)

    if has_sup_similarweb:
        sup_similarweb_df_savepath = join(out_folderpath, "sup_similarweb.csv")
        save_df_as_csv(sup_similarweb_df, sup_similarweb_df_savepath)

    log_end(start_time)


if __name__ == "__main__":
    main()
