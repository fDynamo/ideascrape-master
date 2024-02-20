from custom_helpers_py.url_formatters import get_domain_from_url
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse


"""
This script combines all the data from compressed source scrapes + individual scrapes + similarweb scrapes

And performs final round of filtering if necessary
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--cc-indiv-scrape-filepath", type=str)
    parser.add_argument("--cc-sup-similarweb-scrape-filepath", type=str)
    parser.add_argument("--cc-source-scrape-folderpath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    cc_indiv_scrape_filepath = args.cc_indiv_scrape_filepath
    cc_sup_similarweb_scrape_filepath = args.cc_sup_similarweb_scrape_filepath
    cc_source_scrape_folderpath = args.cc_source_scrape_folderpath
    out_filepath = args.out_filepath

    if (
        not cc_indiv_scrape_filepath
        or not cc_sup_similarweb_scrape_filepath
        or not out_filepath
    ):
        print("Invalid inputs")
        return

    master_df = read_csv_as_df(cc_indiv_scrape_filepath)

    if cc_source_scrape_folderpath:
        # TODO:
        # Merge master df with source scrape
        # Also need to handle better descriptions, titles, and image urls
        # Also need to handle better image urls here
        # compressed_source_df = get_compressed_sources_df()

        # master_df = filtered_individual_df.merge(
        #     compressed_source_df, left_on="url", right_on="clean_product_url", how="left"
        # )
        # master_df = master_df.drop(columns="clean_product_url")
        pass

    # Decide on product image url
    # Use these substrings for source images and filter out these default images
    banned_substring = [
        "media.theresanaiforthat.com/assets/favicon-large",
        "media.theresanaiforthat.com/assets/favicon",
        "ph-files.imgix.net/ecca7485-4473-4479-84ea-1702b0fe04e5",
    ]

    # Get domain
    master_df["product_domain"] = master_df["url"].apply(get_domain_from_url)

    # Fix image urls
    def fix_image_urls(row):
        image_url = row["product_image_url"]
        if not isinstance(image_url, str) or image_url == "":
            return None

        if image_url.startswith("http"):
            return image_url

        if image_url.startswith("//"):
            return "https:" + image_url

        domain = row["product_domain"]
        try:
            to_return = "https://" + domain + image_url
            return to_return
        except:
            print(row)

    master_df["product_image_url"] = master_df["favicon_url"]
    master_df["product_image_url"] = master_df.apply(fix_image_urls, axis=1)

    print("lol", master_df)

    # Decide on description
    def get_product_description(row):
        page_desc: str = row["page_description"]
        if not page_desc or not isinstance(page_desc, str):
            return None

        words = page_desc.split(" ")
        num_words = len(words)
        if num_words < 10:
            return None
        return page_desc

    master_df["description"] = master_df.apply(get_product_description, axis=1)
    master_df = master_df.dropna(subset="description")

    # Decide on title
    master_df["title"] = master_df["page_title"]
    master_df = master_df.dropna(subset="title")

    # Merge with seo
    similarweb_df = read_csv_as_df(cc_sup_similarweb_scrape_filepath)
    similarweb_df = similarweb_df.add_prefix("similarweb_")
    master_df = master_df.merge(
        similarweb_df,
        left_on="product_domain",
        right_on="similarweb_domain",
        how="left",
    )

    # Handle duplicates with different urls
    # TODO: Make something more complex here
    master_df["title+description"] = master_df.apply(
        lambda row: row["title"] + row["description"], axis=1
    )
    master_df = master_df.sort_values("similarweb_total_visits_last_month")
    master_df = master_df.drop_duplicates("title+description", keep="last")

    # Drop columns
    master_df = master_df.drop(columns=["title+description", "similarweb_domain"])

    master_df = master_df.sort_values("url")

    save_df_as_csv(master_df, out_filepath)

    print(master_df)
    print(master_df.columns)


if __name__ == "__main__":
    main()
