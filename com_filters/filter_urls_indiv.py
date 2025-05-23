import pandas as pd
from custom_helpers_py.url_formatters import clean_url, get_domain_from_url
from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
from custom_helpers_py.custom_classes.tp_data import TPData
from custom_helpers_py.custom_classes.index_cache import IndexCache
from com_filters.helpers import is_url_valid
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--in-file-path", type=str, required=False, dest="in_file_path"
    )
    parser.add_argument(
        "--tp", "--tp-file-path", type=str, required=True, dest="tp_folder_path"
    )
    parser.add_argument(
        "-o", "--out-file-path", type=str, required=True, dest="out_file_path"
    )
    parser.add_argument(
        "-r",
        "--rejected-file-path",
        type=str,
        required=False,
        dest="rejected_file_path",
    )
    parser.add_argument("-c", "--col-name", type=str, default="url", dest="col_name")

    parser.add_argument(
        "--use-tp-as-input",
        action=argparse.BooleanOptionalAction,
        default=False,
        dest="use_tp_as_input",
    )

    parser.add_argument("--prod", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--disable-filter", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--reset-tp", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--use-cache-filter", action=argparse.BooleanOptionalAction, default=False
    )
    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_file_path = args.out_file_path
    rejected_file_path = args.rejected_file_path
    tp_folder_path = args.tp_folder_path
    col_name = args.col_name
    is_use_tp_as_input = args.use_tp_as_input

    is_disable_filter = args.disable_filter
    is_reset_tp = args.reset_tp

    is_prod = args.prod
    is_use_cache_filter = args.use_cache_filter

    if not is_use_tp_as_input and not in_file_path:
        print("No input supplied")
        exit(1)

    tpd = TPData(folder_path=tp_folder_path)
    if is_use_tp_as_input:
        urls_series: pd.Series = tpd.get_urls(combined=True)
        if urls_series is None:
            print("TPD cannot be used as input")
            exit(1)
    else:
        in_df = read_csv_as_df(in_file_path)
        urls_series: pd.Series = in_df[col_name]

    urls_series = urls_series.apply(clean_url)
    domains_series = urls_series.apply(get_domain_from_url)

    master_df = pd.concat([urls_series, domains_series]).to_frame("url")
    master_df = master_df.drop_duplicates(subset="url", keep="last")
    master_df = master_df.sort_values(by="url")

    # Filter
    if is_use_cache_filter:
        ic = IndexCache(prod=is_prod)
        recent_url_set = set(ic.get_recent_urls(recent_days=30, recent_type="updated"))

    def is_url_rejected(in_url):
        if is_disable_filter:
            return None

        if is_use_cache_filter and in_url in recent_url_set:
            return "filter_urls_indiv: Recently updated"

        is_valid_res = is_url_valid(in_url)
        if is_valid_res[0]:
            return None
        return "filter_urls_indiv: " + is_valid_res[1]

    master_df["rejected"] = master_df["url"].apply(is_url_rejected)

    filtered_df = master_df[master_df["rejected"].isna()][["url"]]
    rejected_df = master_df[~master_df["rejected"].isna()][["url", "rejected"]]

    # Save
    save_df_as_csv(filtered_df, out_file_path)
    if rejected_file_path:
        save_df_as_csv(rejected_df, rejected_file_path)

    master_df = master_df.rename(columns={"url": "product_url"})
    tpd.add_data(to_add_df=master_df, reset=is_reset_tp)


if __name__ == "__main__":
    main()
