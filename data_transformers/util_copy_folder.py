from shutil import copy
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-folderpath", type=str)
    parser.add_argument("-o", "--out-folderpath", type=str)
    args = parser.parse_args()

    in_folderpath = args.in_folderpath
    out_folderpath = args.out_folderpath

    if not in_folderpath or not out_folderpath:
        print("Invalid inputs")
        return

    copy(in_folderpath, out_folderpath)


if __name__ == "__main__":
    main()
