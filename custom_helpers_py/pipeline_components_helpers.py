PREFIX_DATA_TRANSFORMERS_SCRIPT = "python data_transformers/"
PREFIX_SCRAPERS_SCRIPT = "npm run scrapers/"

SCRIPT_LOCATIONS_DATA = {
    "data_transformers": {"fmt": "python data_transformers/{script_path} {args_str}"},
    "scrapers": {"fmt": "npm run scrapers/{script_path} -- {args_str}"},
}

"""
Args format
[
    arg_key: str,
    arg_val: str
]
"""


def format_component_string(
    script_location: str, script_path: str, script_args_list: list
):
    if not SCRIPT_LOCATIONS_DATA.get(script_location):
        raise "Invalid script location"

    script_location_data = SCRIPT_LOCATIONS_DATA[script_location]

    # Handle script args
    script_args_str = ""
    for script_arg in script_args_list:
        arg_key = script_arg[0]
        raw_arg_val = script_arg[1]
        arg_val = '"{}"'.format(raw_arg_val)
        if isinstance(raw_arg_val, int) or isinstance(raw_arg_val, float):
            arg_val = raw_arg_val

        to_add = arg_key + " " + arg_val + " "
        script_args_str += to_add

    if len(script_args_str) > 0:
        script_args_str = script_args_str[:-1]

    script_fmt = script_location_data["fmt"]
    return script_fmt.format(script_path=script_path, args_str=script_args_str)


SCRIPT_RUN_STOPPER = "/STOP"


def get_components_from_script(in_filepath: str):
    com_list = []
    with open(in_filepath, "r") as file:
        file_contents = file.read()
        lines = file_contents.splitlines()
        for line in lines:
            if not line or line == "" or line.startswith("["):
                continue
            if line.startswith(SCRIPT_RUN_STOPPER):
                break
            com_list.append(line)
    return com_list
