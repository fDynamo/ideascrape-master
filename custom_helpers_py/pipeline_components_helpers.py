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
