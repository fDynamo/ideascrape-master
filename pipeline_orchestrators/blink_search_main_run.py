import subprocess
from pipeline_orchestrators.blink_search_main_create_script import (
    main as blink_search_main_create_script,
)
import argparse
from custom_helpers_py.pipeline_components_helpers import get_components_from_script


def main():
    # Get arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--start-index", type=int)
    args, _ = parser.parse_known_args()

    start_index = args.start_index

    # Generate blink_search_main script
    blink_search_main_script_filepath = blink_search_main_create_script()
    if not blink_search_main_script_filepath:
        return

    # Get components from script
    com_list = get_components_from_script(blink_search_main_script_filepath)

    # Run all scripts
    for i, com in enumerate(com_list):
        if start_index and i < start_index:
            continue

        print("[ORCHESTRATOR] START script", i, com)

        process = subprocess.run(com, shell=True)
        returncode = process.returncode
        if returncode > 0:
            print("[ORCHESTRATOR] ERROR", i, com)
            break

        print("[ORCHESTRATOR] END script", i, com)


if __name__ == "__main__":
    main()
