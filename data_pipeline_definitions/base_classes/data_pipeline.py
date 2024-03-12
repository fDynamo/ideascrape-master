from abc import ABC, abstractmethod
from data_pipeline_definitions.base_classes.script_component import ScriptComponent
import argparse
import subprocess
from custom_helpers_py.get_paths import get_artifacts_folder_path
from os.path import join, exists
from custom_helpers_py.folder_helpers import mkdir_if_not_exists, mkdir_to_ensure_path
from custom_helpers_py.date_helpers import get_current_date_filename
import argparse


class DataPipeline(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.pipeline_run_folder_path: str = ""
        self.run_info_folder_path: str = ""

    @abstractmethod
    def get_pipeline_name(self) -> str:
        pass

    @abstractmethod
    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        pass

    def add_cli_args(self, parser):
        parser.add_argument("-o", "--outName", type=str, dest="out_name")
        parser.add_argument(
            "-n", "--newName", action=argparse.BooleanOptionalAction, dest="new_name"
        )
        parser.add_argument(
            "-t", "--testName", action=argparse.BooleanOptionalAction, dest="test_name"
        )
        parser.add_argument("-r", "--retryName", type=str, dest="retry_name")
        parser.add_argument("--startIndex", type=int, dest="start_index", default=0)
        parser.add_argument("--endIndex", type=int, dest="end_index", default=None)
        parser.add_argument(
            "--prod", action=argparse.BooleanOptionalAction, default=False
        )
        parser.add_argument(
            "--upsync", action=argparse.BooleanOptionalAction, default=False
        )
        parser.add_argument(
            "--useDevScrape",
            action=argparse.BooleanOptionalAction,
            default=False,
            dest="use_dev_scrape",
        )
        parser.add_argument(
            "--printSteps",
            action=argparse.BooleanOptionalAction,
            default=False,
            dest="print_steps",
        )

    def run_steps(self, steps: list[ScriptComponent], **kwargs):
        start_index, end_index = kwargs["start_index"], kwargs["end_index"]

        """
        TODO:
        - Find a way to save std out and std err from process to logs
        - Time start and end of step
        - Time start and end of entire run
        - Log inputs and full script
        """
        for i, com in enumerate(steps):
            if i < start_index:
                continue

            if end_index and i >= end_index:
                continue

            print("[ORCHESTRATOR] START script", i, com)

            try:
                # Make all directories as needed
                path_in_args = com.get_paths_in_args()
                for in_path in path_in_args:
                    mkdir_to_ensure_path(in_path)

                # Run
                script_to_run = str(com)
                process = subprocess.run(script_to_run, shell=True)
                returncode = process.returncode
                if returncode > 0:
                    raise Exception("Script failed")
            except Exception as error:
                print("[ORCHESTRATOR] ERROR", i, com)
                print(error)
                break

            print("[ORCHESTRATOR] END script", i, com)
            pass

    def print_steps(self, **kwargs):
        steps_to_run: list[ScriptComponent] = self.get_steps(**kwargs)
        for step in steps_to_run:
            print(step.get_debug_str())
            print()

    def run_from_cli(self):
        parser = argparse.ArgumentParser()
        self.add_cli_args(parser)

        cli_args, _ = parser.parse_known_args()

        if cli_args.print_steps:
            self.print_steps(**vars(cli_args))
            exit()

        pipeline_name = self.get_pipeline_name()
        root_pipeline_folder_path = join(get_artifacts_folder_path(), pipeline_name)
        mkdir_if_not_exists(root_pipeline_folder_path)

        # Get run folder name
        run_name = None
        is_retry = False

        out_name: str = cli_args.out_name
        retry_name: str = cli_args.retry_name
        if out_name:
            run_name = out_name
        elif retry_name:
            run_name = retry_name
            is_retry = True
        else:
            is_run_new: bool = cli_args.new_name
            is_run_test: bool = cli_args.test_name

            if is_run_test:
                run_name = "test"
            elif is_run_new:
                run_name = get_current_date_filename()

        if run_name is None:
            print("No name provided.")
            exit(1)

        pipeline_run_folder_path = join(root_pipeline_folder_path, run_name)
        run_info_folder_path = join(pipeline_run_folder_path, "run_info")

        self.pipeline_run_folder_path = pipeline_run_folder_path
        self.run_info_folder_path = run_info_folder_path
        steps_to_run = []
        if exists(run_info_folder_path) and is_retry:
            print("TODO: IMPLEMENT RETRIES")
            exit(1)

        mkdir_if_not_exists([pipeline_run_folder_path, run_info_folder_path])

        steps_to_run = self.get_steps(**vars(cli_args))

        # Run steps
        self.run_steps(steps_to_run, **vars(cli_args))
