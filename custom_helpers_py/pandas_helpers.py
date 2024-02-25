import pandas as pd
from os.path import join
from os import listdir


def read_csv_as_df(filepath: str, use_python_engine: bool = False) -> pd.DataFrame:
    if use_python_engine:
        return pd.read_csv(filepath, encoding="utf-8", engine="python")
    return pd.read_csv(filepath, encoding="utf-8", low_memory=False)


def save_df_as_csv(df: pd.DataFrame, filepath: str):
    df.to_csv(filepath, encoding="utf-8", index=False)


def concat_folder_into_df(
    folderpath: str,
    drop_subset=None,
    ends_with_filter="-data.csv",
    use_python_engine=False,
    print_filename=False,
    enforce_mono_column: (
        None | str
    ) = None,  # If a string, extracts only this col from all files and raises error if this does not exist
):
    file_list: list[str] = listdir(folderpath)
    file_list.sort()

    df_list = []
    for filename in file_list:
        if ends_with_filter and not filename.endswith(ends_with_filter):
            continue
        filepath = join(folderpath, filename)
        if print_filename:
            print(filepath)
        if use_python_engine:
            df = pd.read_csv(filepath, encoding="utf-8", engine="python")
        else:
            df = pd.read_csv(filepath, encoding="utf-8", low_memory=False)

        if enforce_mono_column:
            df = df[[enforce_mono_column]]
        df_list.append(df)

    master_df = pd.concat(df_list)

    if drop_subset:
        master_df = master_df.drop_duplicates(subset=drop_subset, keep="last")

    master_df = master_df.reset_index(drop=True)

    return master_df
