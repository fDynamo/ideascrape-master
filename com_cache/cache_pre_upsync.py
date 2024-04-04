import argparse
import pandas as pd
from custom_helpers_py.custom_classes.tp_data import TPData
from custom_helpers_py.custom_classes.index_cache import IndexCache


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--run-name", type=str, required=True, dest="run_name")
    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )

    args = parser.parse_args()

    tp_folder_path = args.tp_folder_path
    run_name = args.run_name

    ic = IndexCache()
    tpd = TPData(folder_path=tp_folder_path)

    master_df = tpd.as_df(filter_rejected=False)
    master_df = master_df[["product_url", "rejected"]]
    master_df = master_df.rename(columns={"rejected": "comments"})
    master_df["last_run_name"] = run_name

    def get_status(in_row: dict):
        comments = in_row["comments"]
        if pd.isna(comments):
            return "READY_FOR_UPLOAD"
        return "REJECTED"

    master_df["status"] = master_df.apply(get_status, axis=1)

    ic.add_data(in_df=master_df)


if __name__ == "__main__":
    main()
