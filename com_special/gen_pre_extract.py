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
    parser.add_argument("--combined-source-filepath", type=str)
    parser.add_argument("-o", "--out-filepath", type=str)
    args = parser.parse_args()

    cc_indiv_scrape_filepath = args.cc_indiv_scrape_filepath
    cc_sup_similarweb_scrape_filepath = args.cc_sup_similarweb_scrape_filepath
    combined_source_filepath = args.combined_source_filepath
    out_filepath = args.out_filepath

    if not cc_indiv_scrape_filepath or not out_filepath:
        print("Invalid inputs")
        return

    master_df = read_csv_as_df(cc_indiv_scrape_filepath)

    if combined_source_filepath:
        combined_source_df = read_csv_as_df(combined_source_filepath)

        master_df = master_df.merge(
            combined_source_df,
            left_on="url",
            right_on="clean_product_url",
            how="left",
        )
        master_df = master_df.drop(columns="clean_product_url")

    # Get domain
    master_df["product_domain"] = master_df["url"].apply(get_domain_from_url)

    # Decide on product image url
    # Fix image urls
    def fix_favicon_url(row):
        image_url: str = row["favicon_url"]

        if image_url.startswith("http"):
            return image_url

        if image_url.startswith("//"):
            return "https:" + image_url

        domain = row["product_domain"]
        if not domain or not isinstance(domain, str):
            print(row)
            raise "Invalid domain!"

        to_return = "https://" + domain + image_url
        return to_return

    def choose_image_url(row) -> str | None:
        favicon_url = row["favicon_url"]
        if not isinstance(favicon_url, str) or favicon_url == "":
            return None
        favicon_url = fix_favicon_url(row)

        return favicon_url

    master_df["product_image_url"] = master_df.apply(choose_image_url, axis=1)

    # Decide on description
    def choose_product_description(row):
        page_desc: str = row["page_description"]
        if not page_desc or not isinstance(page_desc, str):
            return None

        words = page_desc.split(" ")
        num_words = len(words)
        if num_words < 10:
            return None
        return page_desc

    master_df["description"] = master_df.apply(choose_product_description, axis=1)
    master_df = master_df.dropna(subset="description")

    # Decide on title
    master_df["title"] = master_df["page_title"]
    master_df = master_df.dropna(subset="title")

    # Merge with seo
    if cc_sup_similarweb_scrape_filepath:
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

    if cc_sup_similarweb_scrape_filepath:
        master_df = master_df.sort_values("similarweb_total_visits_last_month")
        master_df = master_df.drop_duplicates("title+description", keep="last")
    else:
        master_df = master_df.drop_duplicates("title+description", keep="last")

    # Drop columns
    cols_to_drop = ["title+description"]
    if cc_sup_similarweb_scrape_filepath:
        cols_to_drop.append("similarweb_domain")

    master_df = master_df.drop(columns=cols_to_drop)

    master_df = master_df.sort_values("url")

    save_df_as_csv(master_df, out_filepath)

    print(master_df)
    print(master_df.columns)


if __name__ == "__main__":
    main()
