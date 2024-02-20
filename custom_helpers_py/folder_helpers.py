from os import mkdir
from os.path import isdir


def mkdir_if_not_exists(in_dir: str | list[str]):
    if isinstance(in_dir, str):
        if not isdir(in_dir):
            mkdir(in_dir)
    elif isinstance(in_dir, list):
        for dirstr in in_dir:
            if not isdir(dirstr):
                mkdir(dirstr)
