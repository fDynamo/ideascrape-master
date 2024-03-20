from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
from custom_helpers_py.string_formatters import convert_url_to_file_name
from os import rename
from os.path import join, exists

search_main_file = "./test/to_upload/search_main.csv"
search_main_df = read_csv_as_df(search_main_file)

search_main_no_embedding_file = "./test/to_upload/search_main_no_embedding.csv"
search_main_no_embedding_df = read_csv_as_df(search_main_no_embedding_file)

product_images_folder = "./test/product_images/"


def fix_image_filename(row):
    in_url = row["product_url"]
    in_image_filename: str = row["product_image_filename"]
    if not isinstance(in_image_filename, str) or not in_image_filename:
        return None

    dot_idx = in_image_filename.rfind(".")
    extension = in_image_filename[dot_idx:]
    if "svg" in extension:
        extension = extension.replace("svg", "png")

    new_name = convert_url_to_file_name(in_url) + extension
    return new_name


search_main_df = search_main_df.drop(columns="old_image_filename")

search_main_no_embedding_df["old_image_filename"] = search_main_no_embedding_df[
    "product_image_filename"
]
search_main_no_embedding_df["product_image_filename"] = (
    search_main_no_embedding_df.apply(fix_image_filename, axis=1)
)

info_list = search_main_no_embedding_df.to_dict(orient="records")
for info in info_list:
    old_name = info["old_image_filename"]
    new_name = info["product_image_filename"]

    if isinstance(old_name, str):
        old_path = join(product_images_folder, old_name)
        if exists(old_path):
            new_path = join(product_images_folder, new_name)
            rename(old_path, new_path)

save_df_as_csv(search_main_df, search_main_file)
save_df_as_csv(search_main_no_embedding_df, search_main_no_embedding_file)

search_main_no_embedding_df[
    ["product_url", "old_image_filename", "product_image_filename"]
]
save_df_as_csv(search_main_no_embedding_df, "product_image_fix.csv")
