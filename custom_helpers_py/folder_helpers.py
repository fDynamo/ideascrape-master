from os import mkdir
from os.path import isdir, dirname


def mkdir_if_not_exists(in_dir: str | list[str]):
    if isinstance(in_dir, str):
        if not isdir(in_dir):
            mkdir(in_dir)
    elif isinstance(in_dir, list):
        for dirstr in in_dir:
            if not isdir(dirstr):
                mkdir(dirstr)


def mkdir_to_ensure_path(some_path: str):
    ACCEPTABLE_EXTENSION_LIST = [".json", ".csv", ".txt"]

    dir_to_ensure = some_path

    has_extension = False
    for ext in ACCEPTABLE_EXTENSION_LIST:
        if dir_to_ensure.endswith(ext):
            has_extension = True
            break

    if has_extension:
        dir_to_ensure = dirname(dir_to_ensure)

    mkdir_if_not_exists(dir_to_ensure)
