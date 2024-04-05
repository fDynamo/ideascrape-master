from os import mkdir, listdir, remove
from os.path import isdir, dirname, join, isfile


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

    ensure_list = [dir_to_ensure]
    RECURSION_DEPTH = 2
    for _ in range(RECURSION_DEPTH):
        dir_to_ensure = dirname(dir_to_ensure)
        ensure_list.append(dir_to_ensure)

        if dir_to_ensure == "." or dir_to_ensure == "":
            break

    ensure_list.reverse()
    mkdir_if_not_exists(ensure_list)


def delete_folder_contents(folder_path):
    file_list = listdir(folder_path)
    for file in file_list:
        file_path = join(folder_path, file)
        if isfile(file_path):
            remove(file_path)
