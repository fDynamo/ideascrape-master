import argparse
from custom_helpers_py.custom_classes.index_cache import IndexCache
from os.path import join
import json
import math
from custom_helpers_py.pandas_helpers import save_df_as_csv
import pandas as pd


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--out-folder-path",
        type=str,
        required=True,
        dest="out_folder_path",
    )
    parser.add_argument("--batch-size", type=int, dest="batch_size", default=-1)
    parser.add_argument(
        "--prod", action=argparse.BooleanOptionalAction, default=False, dest="prod"
    )
    parser.add_argument(
        "--only-uploaded",
        action=argparse.BooleanOptionalAction,
        default=False,
        dest="only_uploaded",
    )

    args = parser.parse_args()

    out_folder_path = args.out_folder_path
    batch_size = args.batch_size
    is_prod = args.prod
    is_only_uploaded = args.only_uploaded

    ic = IndexCache(prod=is_prod)
    url_list = ic.get_urls(only_uploaded=is_only_uploaded)

    to_add_list = []
    if batch_size > -1:
        NUM_BATCHES = math.ceil(len(url_list) / batch_size)
        for i in range(NUM_BATCHES):
            start_idx = i * batch_size
            if i + 1 == NUM_BATCHES:
                end_idx = len(url_list)
            else:
                end_idx = (i + 1) * batch_size
            curr_slice = url_list[start_idx:end_idx]
            to_add_list.append(curr_slice)
    else:
        to_add_list.append(url_list)

    for i, obj in enumerate(to_add_list):
        save_name = "batch_" + str(i) + ".csv"
        save_path = join(out_folder_path, save_name)
        df = pd.Series(obj).to_frame("url")
        save_df_as_csv(df, save_path)


if __name__ == "__main__":
    main()
