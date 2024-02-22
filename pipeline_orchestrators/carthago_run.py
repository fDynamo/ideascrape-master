import subprocess
from pipeline_orchestrators.carthago_create_script import main as carthago_create_script
import argparse


def get_components_from_script(in_filepath: str):
    com_list = []
    with open(in_filepath, "r") as file:
        file_contents = file.read()
        lines = file_contents.splitlines()
        for line in lines:
            if line and not line.startswith("["):
                com_list.append(line)
    return com_list


"""
TODO:
- Create args to skip creating script
- Find a way to find last script operation and run
    - Maybe add a record file for all things ran?
- Replace the current start index system
"""


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--start-index", type=int)
    args, _ = parser.parse_known_args()

    start_index = args.start_index

    # Generate carthago script
    carthago_script_filepath = carthago_create_script()
    if not carthago_script_filepath:
        return

    # Get components from script
    com_list = get_components_from_script(carthago_script_filepath)

    # Run all scripts
    com_list_len = len(com_list)
    duckster_script_filepath = None
    for i, com in enumerate(com_list):
        if start_index and i < start_index:
            continue

        is_duckster_call = i + 1 == com_list_len

        print("[ORCHESTRATOR] START script: ", i, com)

        if is_duckster_call:
            process = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE)
            output, _ = process.communicate()
            output = output.decode("utf-8")
            output = output.strip()
            duckster_script_filepath = output
        else:
            process = subprocess.Popen(com, shell=True)
            process.wait()

        print("[ORCHESTRATOR] END script: ", i, com)

    if not isinstance(duckster_script_filepath, str):
        print("No duckster script created")
        return

    com_list = get_components_from_script(duckster_script_filepath)
    for i, com in enumerate(com_list):
        process = subprocess.Popen(com, shell=True)
        process.wait()


if __name__ == "__main__":
    main()
