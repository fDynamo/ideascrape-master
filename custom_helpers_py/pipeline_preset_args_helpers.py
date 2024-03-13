import argparse
from os.path import join
from custom_helpers_py.get_paths import get_artifacts_folder_path
from custom_helpers_py.date_helpers import get_current_date_filename
from os import listdir


def add_args_for_out_folder_preset(parser):
    parser.add_argument("-n", "--run-new", action=argparse.BooleanOptionalAction)
    parser.add_argument("-r", "--run-recent", action=argparse.BooleanOptionalAction)
    parser.add_argument("-t", "--run-test", action=argparse.BooleanOptionalAction)


# Returns new folderpath after parsing args
def parse_args_for_out_folder_preset(args, folder_prefix="") -> str | None:
    is_run_new: bool = args.run_new
    is_run_recent: bool = args.run_recent
    is_run_test: bool = args.run_test

    if is_run_test:
        folder_name = folder_prefix + "test"
        artifacts_folder_path = get_artifacts_folder_path()
        return join(artifacts_folder_path, folder_name)
    elif is_run_recent:
        raise Exception("Not implemented!")
    elif is_run_new:
        folder_name = get_current_date_filename()
        artifacts_folder_path = get_artifacts_folder_path()
        return join(artifacts_folder_path, folder_prefix, folder_name)
    else:
        return None
