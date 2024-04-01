from os.path import exists
from custom_helpers_py.pandas_helpers import read_json_as_df
from custom_helpers_py.url_formatters import get_domain_from_url, clean_url
import pandas as pd


class TPFile:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.file_exists = exists(file_path)

    def add_data(
        self,
        to_add_list: list[dict] = None,
        clean_product_url=False,
        to_add_df: pd.DataFrame = None,
    ):
        if to_add_df is None:
            if to_add_list is None:
                raise Exception("Nothing to add")

            # Read as df
            to_add_df = pd.DataFrame(to_add_list)

        original_df = to_add_df

        if TPFile.__validate_tp_df(to_add_df):
            raise Exception("Malformed df")

        if clean_product_url:
            to_add_df["product_url"] = to_add_df["product_url"].apply(clean_url)
            to_add_df = to_add_df.drop_duplicates(subset="product_url")

        if self.file_exists:
            try:
                curr_df = read_json_as_df(self.file_path)
                dupe_suffix = "_old"
                to_add_df = curr_df.merge(
                    to_add_df, on="product_url", how="outer", suffixes=(dupe_suffix, "")
                )
                cols_to_remove = [
                    col for col in to_add_df.columns if col.endswith(dupe_suffix)
                ]
                if len(cols_to_remove) > 0:
                    for old_col in cols_to_remove:
                        new_col = old_col.removesuffix(dupe_suffix)
                        to_add_df[new_col] = to_add_df[new_col].fillna(
                            to_add_df[old_col]
                        )

                    to_add_df = to_add_df.drop(columns=cols_to_remove)
            except:
                to_add_df = original_df

        if (
            "product_domain" not in to_add_df.columns
            or to_add_df["product_domain"].isna().any()
        ):
            to_add_df["product_domain"] = to_add_df["product_url"].apply(
                get_domain_from_url
            )

        self.save_df(to_add_df, skip_validation=True)

    def as_df(self, filter_rejected=True) -> pd.DataFrame | None:
        if self.file_exists:
            master_df = read_json_as_df(self.file_path)
            if filter_rejected:
                master_df = master_df[~master_df["rejected"].isna()]

            return master_df
        return None

    def save_df(self, df_to_save: pd.DataFrame, skip_validation=False):
        if not skip_validation and TPFile.__validate_tp_df(df_to_save):
            raise Exception("Malformed df")

        df_to_save.to_json(self.file_path, orient="records", indent=4)
        if not self.file_exists:
            self.file_exists = True

    def get_urls(self, domains: bool = False, filter_rejected=True):
        df = self.as_df(filter_rejected=filter_rejected)
        if df is None:
            return None

        if domains:
            return df["product_domain"].drop_duplicates(keep="last")
        return df["product_url"]

    @staticmethod
    def __validate_tp_df(in_df: pd.DataFrame) -> bool:
        return not "product_url" in in_df.columns or in_df["product_url"].isna().any()


if __name__ == "__main__":
    pass
    # test_tp = TPFile("./test.json")
    # test_tp.add_data([{"product_url": "test", "cool": "one"}])
    # test_tp.add_data(
    #     [
    #         {"product_url": "test", "cool": "two"},
    #         {"product_url": "test2", "cool": "four"},
    #     ]
    # )
