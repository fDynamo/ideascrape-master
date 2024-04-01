import pandas as pd
from custom_helpers_py.filter_results import is_url_valid
from custom_helpers_py.url_formatters import clean_url, get_domain_from_url
from custom_helpers_py.pandas_helpers import read_csv_as_df, save_df_as_csv
from custom_helpers_py.custom_classes.tp_file import TPFile
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "--in-file-path", type=str, required=False, dest="in_file_path"
    )
    parser.add_argument(
        "--tp", "--tp-file-path", type=str, required=True, dest="tp_file_path"
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
        dest="is_use_tp_as_input",
    )

    parser.add_argument("--prod", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--disable-filter", action=argparse.BooleanOptionalAction, default=False
    )
    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_file_path = args.out_file_path
    rejected_file_path = args.rejected_file_path
    tp_file_path = args.tp_file_path
    col_name = args.col_name
    is_use_tp_as_input = args.is_use_tp_as_input

    is_prod = args.prod
    is_disable_filter = args.disable_filter

    if not is_use_tp_as_input and not in_file_path:
        print("No input supplied")
        exit(1)

    tp_file = TPFile(file_path=tp_file_path)
    if is_use_tp_as_input:
        urls_series: pd.Series = tp_file.get_urls()
        if urls_series is None:
            print("TP File cannot be used as input")
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
    def is_url_rejected(in_url):
        if is_disable_filter:
            return None

        valid = is_url_valid(in_url)
        if valid == "y":
            return None
        return "filter_urls_indiv: " + valid

    master_df["rejected"] = master_df["url"].apply(is_url_rejected)

    filtered_df = master_df[master_df["rejected"].isna()][["url"]]
    rejected_df = master_df[~master_df["rejected"].isna()][["url", "rejected"]]

    # Save
    save_df_as_csv(filtered_df, out_file_path)
    if rejected_file_path:
        save_df_as_csv(rejected_df, rejected_file_path)

    master_df = master_df.rename(columns={"url": "product_url"})
    tp_file.add_data(to_add_df=master_df)


if __name__ == "__main__":
    main()
