from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
import pandas as pd

filepath = "./tmp/source_domain_dupes.csv"
dupes_df = read_csv_as_df(filepath)
dupes_df = dupes_df.sort_values(by="id")

keep_df = dupes_df.drop_duplicates(subset="source_domain", keep="first")
rejected_df = dupes_df[~dupes_df["id"].isin(keep_df["id"])]

keep_df = keep_df.rename(columns={"id": "to_keep_id"})
rejected_df = rejected_df.rename(columns={"id": "to_remove_id"})

keep_df = keep_df.merge(rejected_df, how="left", on="source_domain")
keep_df = keep_df[["to_keep_id", "to_remove_id"]]

save_df_as_csv(keep_df, "source_domain_id_map.csv")
