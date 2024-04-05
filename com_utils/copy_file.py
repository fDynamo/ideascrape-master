import argparse
from shutil import copyfile


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i", "-c", "--in-file-path", type=str, dest="in_file_path", required=True
    )
    parser.add_argument(
        "-o", "-p", "--out-file-path", type=str, dest="out_file_path", required=True
    )

    args = parser.parse_args()

    in_file_path = args.in_file_path
    out_file_path = args.out_file_path
    copyfile(in_file_path, out_file_path)


if __name__ == "__main__":
    main()
