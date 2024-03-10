from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
from custom_helpers_py.string_formatters import convert_url_to_filename
from os import rename
from os.path import join, exists
import pandas as pd

fix_map_file = "./tmp/images_fix_map.csv"
fix_map_df = read_csv_as_df(fix_map_file)

img_record_file = "./test/product_images/_record.csv"
img_record_df = read_csv_as_df(img_record_file)

img_record_df = img_record_df.merge(
    fix_map_df, left_on="image_filename", right_on="old_filename", how="left"
)
img_record_df = img_record_df.drop(
    columns=["image_filename", "old_filename", "product_url"]
)
img_record_df = img_record_df.rename(columns={"new_filename": "image_filename"})
save_df_as_csv(img_record_df, "./test/product_images/_record_fix.csv")
