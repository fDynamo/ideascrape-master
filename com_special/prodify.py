from os.path import join
from custom_helpers_py.string_formatters import convert_url_to_file_name
from custom_helpers_py.url_formatters import get_domain_from_url
from com_search_extract.download_product_images import ERROR_FILE_NAME, RECORD_FILE_NAME
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.df_validator import validate_prod_sup_similarweb_df


"""
This script combines all the data gathered to create prod tables for uploads

Algo:
- When we are cre

"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    # search_main
    parser.add_argument(
        "--ccIndivScrapeFilePath",
        type=str,
        dest="cc_indiv_scrape_file_path",
    )
    parser.add_argument(
        "--descEmbeddingsFolderPath",
        type=str,
        dest="desc_embeddings_folder_path",
    )
    parser.add_argument(
        "--productImagesFolderPath",
        type=str,
        dest="product_images_folder_path",
    )

    # sup_similarweb
    parser.add_argument(
        "--ccSupSimilarwebScrapeFilePath",
        type=str,
        dest="cc_sup_similarweb_scrape_file_path",
    )

    # sources
    parser.add_argument(
        "--combinedSourceFilePath", type=str, dest="combined_source_file_path"
    )

    parser.add_argument(
        "-o", "--outFolderPath", type=str, dest="out_folder_path", required=True
    )
    args = parser.parse_args()

    cc_indiv_scrape_file_path: str = args.cc_indiv_scrape_file_path
    desc_embeddings_folder_path: str = args.desc_embeddings_folder_path
    product_images_folder_path: str = args.product_images_folder_path

    cc_sup_similarweb_scrape_file_path: str = args.cc_sup_similarweb_scrape_file_path

    combined_source_file_path: str = args.combined_source_file_path

    out_folder_path: str = args.out_folder_path

    """
    Initial search_main
    """
    has_search_main = not not cc_indiv_scrape_file_path
    if cc_indiv_scrape_file_path:
        print("start search_main")
        search_main_df = read_csv_as_df(cc_indiv_scrape_file_path)
        search_main_df = search_main_df.drop(columns=["end_url", "page_image_url"])
        search_main_df = search_main_df.rename(
            columns={
                "init_url": "product_url",
                "title": "product_name",
                "description": "product_description",
            }
        )

        # tmp values for help later
        search_main_df["tmp_url_file_name"] = search_main_df["product_url"].apply(
            convert_url_to_file_name
        )
        search_main_df["tmp_product_domain"] = search_main_df["product_url"].apply(
            get_domain_from_url
        )

        if desc_embeddings_folder_path:
            print("start embeddings")

            def get_desc_embedding(url_file_name: str):
                desc_embedding_file_path = join(
                    desc_embeddings_folder_path, url_file_name + ".txt"
                )
                with open(desc_embedding_file_path, "r") as em_file:
                    embedding = em_file.read()
                return embedding

            search_main_df["product_description_embedding"] = search_main_df[
                "tmp_url_file_name"
            ].apply(get_desc_embedding)
        else:
            search_main_df["product_description_embedding"] = ""

        if product_images_folder_path:
            print("start images")
            IMAGE_RECORDS_FILE_PATH = join(product_images_folder_path, RECORD_FILE_NAME)
            image_records_df = read_csv_as_df(IMAGE_RECORDS_FILE_PATH)
            search_main_df = search_main_df.merge(image_records_df, on="product_url")
            search_main_df = search_main_df.rename(
                columns={"image_file_name": "product_image_filename"}
            )

            def fix_image_file_name(in_name: str):
                if in_name == ERROR_FILE_NAME:
                    return ""
                return in_name

            search_main_df["product_image_filename"] = search_main_df[
                "product_image_filename"
            ].apply(fix_image_file_name)
        else:
            search_main_df["product_image_filename"] = ""

    """
    sup_similarweb
    """
    has_sup_similarweb = not not cc_sup_similarweb_scrape_file_path
    if cc_sup_similarweb_scrape_file_path:
        print("start sup_similarweb")
        sup_similarweb_df = read_csv_as_df(cc_sup_similarweb_scrape_file_path)
        sup_similarweb_df = sup_similarweb_df.rename(
            columns={
                "domain": "source_domain",
            }
        )
        sup_similarweb_df = sup_similarweb_df[
            ["source_domain", "total_visits_last_month", "data_created_at"]
        ]

    """
    sources
    """
    has_source_aift = not not combined_source_file_path
    has_source_ph = has_source_aift
    if combined_source_file_path:
        combined_source_df = read_csv_as_df(combined_source_file_path)

        aift_cols = [
            col for col in combined_source_df.columns if col.startswith("aift_")
        ] + ["clean_product_url"]
        if len(aift_cols) > 1:
            print("started source_aift")
            source_aift_df = combined_source_df[aift_cols]
            aift_cols = [col.removeprefix("aift_") for col in aift_cols]
            source_aift_df.columns = aift_cols

            # Clean
            source_aift_df = source_aift_df.dropna(subset="source_url")
            source_aift_df = source_aift_df.drop_duplicates(subset="source_url")
            source_aift_df = source_aift_df.reset_index(drop=True)

            # Prepare
            source_aift_df["count_save"] = source_aift_df["count_save"].astype(int)
            aift_cols = [
                "clean_product_url",  # NOTE: This is to be removed after handling search_main ids
                "source_url",
                "count_save",
                "listed_at",
                "updated_at",
            ]
            source_aift_df = source_aift_df[aift_cols]
            source_aift_df = source_aift_df.rename(
                columns={
                    "clean_product_url": "product_url",
                    "listed_at": "product_listed_at",
                    "updated_at": "product_updated_at",
                }
            )

        ph_cols = [
            col for col in combined_source_df.columns if col.startswith("ph_")
        ] + ["clean_product_url"]
        if len(ph_cols) > 1:
            print("started source_ph")
            source_ph_df = combined_source_df[ph_cols]
            ph_cols = [col.removeprefix("ph_") for col in ph_cols]
            source_ph_df.columns = ph_cols

            # Clean
            source_ph_df = source_ph_df.dropna(subset="source_url")
            source_ph_df = source_ph_df.drop_duplicates(subset="source_url")
            source_ph_df = source_ph_df.reset_index(drop=True)

            # Prepare
            source_ph_df["count_follower"] = source_ph_df["count_follower"].astype(int)
            ph_cols = [
                "clean_product_url",
                "source_url",
                "count_follower",
                "listed_at",
                "updated_at",
            ]
            source_ph_df = source_ph_df[ph_cols]
            source_ph_df = source_ph_df.rename(
                columns={
                    "clean_product_url": "product_url",
                    "listed_at": "product_listed_at",
                    "updated_at": "product_updated_at",
                }
            )

    """
    Combine to get ids for search_main
    """
    if has_search_main:
        print("Started id combinations for search_main")
        # similarweb
        if has_sup_similarweb:
            print("started sup_similarweb_id")
            # Remove in source if not in search_main
            sup_similarweb_df = sup_similarweb_df[
                sup_similarweb_df["source_domain"].isin(
                    search_main_df["tmp_product_domain"]
                )
            ].reset_index(drop=True)

            sup_similarweb_id_df = sup_similarweb_df.rename_axis(
                "sup_similarweb_id"
            ).reset_index()
            sup_similarweb_id_df["sup_similarweb_id"] = sup_similarweb_id_df[
                "sup_similarweb_id"
            ].apply(lambda x: x + 1)
            sup_similarweb_id_df = sup_similarweb_id_df[
                ["sup_similarweb_id", "source_domain"]
            ]
            sup_similarweb_id_df = sup_similarweb_id_df.rename(
                columns={"source_domain": "tmp_product_domain"}
            )
            search_main_df = search_main_df.merge(
                sup_similarweb_id_df,
                on="tmp_product_domain",
                how="left",
            )

        else:
            search_main_df["sup_similarweb_id"] = None

        # aift
        if has_source_aift:
            print("started source_aift_id")
            # Remove in source if not in search_main
            source_aift_df = source_aift_df[
                source_aift_df["product_url"].isin(search_main_df["product_url"])
            ].reset_index(drop=True)

            source_aift_id_df = source_aift_df.rename_axis(
                "source_aift_id"
            ).reset_index()
            source_aift_id_df["source_aift_id"] = source_aift_id_df[
                "source_aift_id"
            ].apply(lambda x: x + 1)
            source_aift_id_df = source_aift_id_df[["source_aift_id", "product_url"]]
            search_main_df = search_main_df.merge(
                source_aift_id_df,
                left_on="product_url",
                right_on="product_url",
                how="left",
            )

        else:
            search_main_df["source_aift_id"] = None

        # ph
        if has_source_ph:
            print("started source_ph_id")
            # Remove in source if not in search_main
            source_ph_df = source_ph_df[
                source_ph_df["product_url"].isin(search_main_df["product_url"])
            ].reset_index(drop=True)

            source_ph_id_df = source_ph_df.rename_axis("source_ph_id").reset_index()
            source_ph_id_df["source_ph_id"] = source_ph_id_df["source_ph_id"].apply(
                lambda x: x + 1
            )
            source_ph_id_df = source_ph_id_df[["source_ph_id", "product_url"]]
            search_main_df = search_main_df.merge(
                source_ph_id_df,
                left_on="product_url",
                right_on="product_url",
                how="left",
            )

        else:
            search_main_df["source_ph_id"] = None

    """
    Cleanup
    """
    print("final cleanup")
    if has_source_aift:
        source_aift_df = source_aift_df.drop(columns="product_url")
    if has_source_ph:
        source_ph_df = source_ph_df.drop(columns="product_url")

    if has_search_main:
        search_main_cols = [
            "product_url",
            "product_name",
            "product_description",
            "product_image_filename",
            "product_description_embedding",
            "source_aift_id",
            "source_ph_id",
            "sup_similarweb_id",
        ]
        search_main_df = search_main_df[search_main_cols]

        # Set types
        if has_source_aift:
            search_main_df["source_aift_id"] = search_main_df["source_aift_id"].astype(
                "Int64"
            )
        if has_source_ph:
            search_main_df["source_ph_id"] = search_main_df["source_ph_id"].astype(
                "Int64"
            )

        if has_sup_similarweb:
            search_main_df["sup_similarweb_id"] = search_main_df[
                "sup_similarweb_id"
            ].astype("Int64")

    print("start validations")
    if has_sup_similarweb:
        validate_prod_sup_similarweb_df(sup_similarweb_df)

    print("start saving")
    """
    Save all dfs
    """
    search_main_df_savepath = join(out_folder_path, "search_main.csv")
    save_df_as_csv(search_main_df, search_main_df_savepath)

    search_main_no_embedding_df = search_main_df.drop(
        columns="product_description_embedding"
    )
    search_main_no_embedding_df_savepath = join(
        out_folder_path, "search_main_no_embedding.csv"
    )
    save_df_as_csv(search_main_no_embedding_df, search_main_no_embedding_df_savepath)

    if has_source_aift:
        source_aift_df_savepath = join(out_folder_path, "source_aift.csv")
        save_df_as_csv(source_aift_df, source_aift_df_savepath)

    if has_source_ph:
        source_ph_df_savepath = join(out_folder_path, "source_ph.csv")
        save_df_as_csv(source_ph_df, source_ph_df_savepath)

    if has_sup_similarweb:
        sup_similarweb_df_savepath = join(out_folder_path, "sup_similarweb.csv")
        save_df_as_csv(sup_similarweb_df, sup_similarweb_df_savepath)


if __name__ == "__main__":
    main()
