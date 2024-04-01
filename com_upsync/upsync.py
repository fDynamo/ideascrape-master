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
        "-i", "--in-folder-path", type=str, dest="in_folder_path", required=True
    )

    # some options
    parser.add_argument(
        "--prod",
        type=bool,
        dest="prod",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    args = parser.parse_args()

    in_folder_path: str = args.in_folder_path

    exit(1)


if __name__ == "__main__":
    main()
