from custom_helpers_py.filter_results import (
    is_domain_similarweb_scrapable,
)
from custom_helpers_py.pandas_helpers import (
    read_csv_as_df,
    save_df_as_csv,
)
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData
import pandas as pd
from custom_helpers_py.url_formatters import get_domain_from_url


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
    tp_folder_path = args.tp_folder_path
    col_name = args.col_name
    is_use_tp_as_input = args.is_use_tp_as_input

    if not is_use_tp_as_input and not in_file_path:
        print("No input supplied")
        exit(1)

    tpd = TPData(folder_path=tp_folder_path)
    if is_use_tp_as_input:
        domains_series: pd.Series = tpd.get_urls(domains=True)
        if domains_series is None:
            print("TPD cannot be used as input")
            exit(1)
    else:
        in_df = read_csv_as_df(in_file_path)
        domains_series: pd.Series = in_df[col_name]
        domains_series = domains_series.apply(get_domain_from_url)
        domains_series = domains_series.dropna()
        domains_series = domains_series.drop_duplicates(keep="last")

    # Handle col_name arg
    master_df = domains_series.to_frame("domain")

    # Filter by title and description
    # TODO: More complex yet efficient logic to truly filter trash out
    def filter_domains(in_domain: str):
        check_res = is_domain_similarweb_scrapable(in_domain)
        if check_res == "y":
            return None
        return check_res

    master_df["rejected_similarweb_scrape"] = master_df["domain"].apply(filter_domains)
    master_df

    invalid_mask = ~master_df["rejected_similarweb_scrape"].isna()
    rejected_df = master_df[invalid_mask]["domain"]
    filtered_df = master_df[~invalid_mask]["domain"]

    # Save
    save_df_as_csv(filtered_df, out_file_path)
    if isinstance(rejected_file_path, str):
        rejected_df = rejected_df.drop(columns=["is_valid"])
        save_df_as_csv(rejected_df, rejected_file_path)

    # Save in tpd
    master_df = master_df.rename(columns={"domain": "product_domain"})
    tpd.add_data(to_add_df=master_df, domain_pk=True)


if __name__ == "__main__":
    main()
