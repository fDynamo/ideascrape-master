from os.path import exists
from os import mkdir
import argparse


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--in-folder-path", type=str, dest="folder_path")
    args = parser.parse_args()

    if not args["folder_path"]:
        print("Invalid inputs")
        exit(1)

    if not exists(args["folder_path"]):
        mkdir(args["folder_path"])


if __name__ == "__main__":
    main()
