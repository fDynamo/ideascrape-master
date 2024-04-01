from os.path import exists
from custom_helpers_py.pandas_helpers import read_json_as_df
from custom_helpers_py.url_formatters import get_domain_from_url, clean_url
import pandas as pd
from os.path import join
from os import mkdir, listdir

MASTER_FILE_NAME = "master.json"
PART_PREFIX = "part_"


class TPData:
    def __init__(self, folder_path: str) -> None:
        self.folder_path = folder_path
        self.folder_exists = exists(folder_path)

    def add_data(
        self,
        to_add_df: pd.DataFrame = None,
        to_add_list: list[dict] = None,
        to_add_dict: dict = None,
        part_name: str = None,
        domain_pk: bool = False,
        reset: bool = False,
    ):
        if to_add_df is not None:
            pass
        elif to_add_list is not None:
            to_add_df = pd.DataFrame(to_add_list)
        elif to_add_dict is not None:
            to_add_df = pd.DataFrame([to_add_dict])
        else:
            raise Exception("Nothing to add")

        # Infer domain pk
        if not domain_pk:
            domain_pk = self.__infer_domain_pk(to_add_df)

        if not TPData.__validate_tp_df(to_add_df, domain_pk=domain_pk):
            raise Exception("Malformed df")

        old_file_path = self.__get_data_file_path(part_name=part_name)
        is_old_exists = exists(old_file_path)

        if is_old_exists and not reset:
            curr_df = read_json_as_df(old_file_path)
            dupe_suffix = "_old"
            on_col = "product_url" if not domain_pk else "product_domain"
            to_add_df = curr_df.merge(
                to_add_df, on=on_col, how="outer", suffixes=(dupe_suffix, "")
            )
            cols_to_remove = [
                col for col in to_add_df.columns if col.endswith(dupe_suffix)
            ]
            if len(cols_to_remove) > 0:
                for old_col in cols_to_remove:
                    new_col = old_col.removesuffix(dupe_suffix)
                    to_add_df[new_col] = to_add_df[new_col].fillna(to_add_df[old_col])

                to_add_df = to_add_df.drop(columns=cols_to_remove)

        if "product_url" in to_add_df.columns and (
            "product_domain" not in to_add_df.columns
            or to_add_df["product_domain"].isna().any()
        ):
            to_add_df["product_domain"] = to_add_df["product_url"].apply(
                get_domain_from_url
            )

        self.save_df(to_add_df, skip_validation=True, part_name=part_name)

    """
    Combines all parts to master
    IMPORTANT:
    - Any urls or domains not in master will still get appended
    """

    def get_combined_parts_df(self, filter_rejected=True):
        master_df = self.as_df(filter_rejected=filter_rejected)
        if master_df is None:
            return None

        file_list = listdir(self.folder_path)
        for file_name in file_list:
            if not file_name.startswith(PART_PREFIX):
                continue

            part_name = file_name.removeprefix(PART_PREFIX).removesuffix(".json")
            part_df = self.as_df(part_name=part_name, filter_rejected=filter_rejected)
            domain_pk = self.__infer_domain_pk(part_df)

            dupe_suffix = "_old"
            on_col = "product_url" if not domain_pk else "product_domain"
            master_df = master_df.merge(
                part_df, on=on_col, how="outer", suffixes=(dupe_suffix, "")
            )
            cols_to_remove = [
                col for col in master_df.columns if col.endswith(dupe_suffix)
            ]
            if len(cols_to_remove) > 0:
                for old_col in cols_to_remove:
                    new_col = old_col.removesuffix(dupe_suffix)
                    master_df[new_col] = master_df[new_col].fillna(master_df[old_col])

                master_df = master_df.drop(columns=cols_to_remove)

        return master_df

    def as_df(self, filter_rejected=True, part_name: str = None) -> pd.DataFrame | None:
        data_file_path = self.__get_data_file_path(part_name=part_name)
        is_data_exists = exists(data_file_path)

        if is_data_exists:
            master_df = read_json_as_df(data_file_path)
            if filter_rejected:
                master_df = master_df[master_df["rejected"].isna()]

            return master_df
        return None

    def save_df(
        self, df_to_save: pd.DataFrame, skip_validation=False, part_name: str = None
    ):
        if not skip_validation and not TPData.__validate_tp_df(df_to_save):
            raise Exception("Malformed df")

        if not self.folder_exists:
            mkdir(self.folder_path)
            self.folder_exists = True

        save_file_path = self.__get_data_file_path(part_name=part_name)

        df_to_save.to_json(save_file_path, orient="records", indent=4)

    def get_urls(
        self, domains: bool = False, filter_rejected=True, part_name: str = None
    ):
        df = self.as_df(filter_rejected=filter_rejected, part_name=part_name)
        if df is None:
            return None

        if domains:
            return df["product_domain"].drop_duplicates(keep="last")
        return df["product_url"]

    def __get_data_file_path(self, part_name: str = None):
        old_file_name = MASTER_FILE_NAME
        if part_name is not None:
            old_file_name = PART_PREFIX + part_name + ".json"
        return join(self.folder_path, old_file_name)

    def __infer_domain_pk(self, in_df: pd.DataFrame):
        return "product_domain" in in_df.columns and "product_url" not in in_df.columns

    @staticmethod
    # Returns false if invalid
    def __validate_tp_df(in_df: pd.DataFrame, domain_pk=False) -> bool:
        if not domain_pk:
            return not (
                "product_url" not in in_df.columns or in_df["product_url"].isna().any()
            )
        else:
            return not (
                "product_domain" not in in_df.columns
                or in_df["product_domain"].isna().any()
            )


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
