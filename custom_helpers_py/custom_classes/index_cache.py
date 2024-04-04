from dotenv import load_dotenv
from os import environ
from os.path import join, exists
import json
import pandas as pd
from custom_helpers_py.pandas_helpers import save_df_as_json, read_json_as_df


class IndexCache:
    def __init__(self) -> None:
        load_dotenv(override=True)
        self.cache_folder_path = environ.get("CACHE_FOLDER")
        if not self.cache_folder_path or not exists(self.cache_folder_path):
            raise Exception("CACHE_FOLDER env variable not set!")
        self.index_cache_file_path = join(self.cache_folder_path, "index_cache.json")

    """
    in_data needs to be a list with dicts that look like:
    {product_url, status, comments, last_run_name}
    """

    def add_data(self, in_data: list[dict] = None, in_df: pd.DataFrame = None):
        if in_df is None and in_data is None:
            raise Exception("No data to cache")

        if in_df is None:
            in_df = pd.DataFrame(in_data)

        # if log attempt, need new last attempt
        in_df["updated_at"] = pd.Timestamp.now()

        # First time we add
        if not exists(self.index_cache_file_path):
            # Populate the rest
            in_df["added_at"] = in_df["updated_at"]
            in_df["num_updates"] = 1

            save_df_as_json(in_df, self.index_cache_file_path)
        else:
            in_df = in_df.rename(
                columns={
                    "status": "new_status",
                    "comments": "new_comments",
                    "last_run_name": "new_last_run_name",
                    "updated_at": "new_updated_at",
                }
            )

            old_df = read_json_as_df(self.index_cache_file_path)
            new_df = old_df.merge(in_df, how="outer", on="product_url")

            # Increment updated
            def increment_updated(in_row: dict):
                old_num = in_row["num_updates"]
                is_updated = not pd.isna(in_row["new_updated_at"])

                no_old = pd.isna(old_num)

                if no_old:
                    return 1
                if is_updated:
                    return old_num + 1
                return old_num

            new_df["num_updates"] = new_df.apply(increment_updated, axis=1).astype(int)

            # Merge values
            new_df["status"] = new_df["new_status"].fillna(new_df["status"])
            new_df["comments"] = new_df["new_comments"].fillna(new_df["comments"])
            new_df["last_run_name"] = new_df["new_last_run_name"].fillna(
                new_df["last_run_name"]
            )
            new_df["updated_at"] = new_df["new_updated_at"].fillna(new_df["updated_at"])
            new_df["added_at"] = new_df["added_at"].fillna(new_df["updated_at"])

            new_df = new_df.drop(
                columns=[col for col in new_df.columns if col.startswith("new_")]
            )

            save_df_as_json(new_df, self.index_cache_file_path)
