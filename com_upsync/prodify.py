from os.path import join
import argparse
from custom_helpers_py.custom_classes.tp_data import TPData


"""
This script combines all the data gathered to create prod tables for uploads

Algo:
- When we are cre

"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp", "--tp-folder-path", type=str, required=True, dest="tp_folder_path"
    )
    parser.add_argument(
        "-o", "--out-folder-path", type=str, dest="out_folder_path", required=True
    )
    parser.add_argument(
        "--product-images-folder-path",
        type=str,
        dest="product_images_folder_path",
    )

    # some options
    parser.add_argument(
        "--ignore-missing-embeddings",
        type=bool,
        dest="ignore_missing_embeddings",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    args = parser.parse_args()

    exit(1)

    tp_folder_path: str = args.tp_folder_path
    out_folder_path: str = args.out_folder_path
    product_images_folder_path: str = args.product_images_folder_path
    ignore_missing_embeddings: bool = args.ignore_missing_embeddings

    tpd = TPData(folder_path=tp_folder_path)
    master_df = tpd.get_combined_parts_df(filter_rejected=False)

    col_to_take_list = []


if __name__ == "__main__":
    main()
