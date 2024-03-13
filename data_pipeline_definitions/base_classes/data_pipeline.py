from abc import ABC, abstractmethod
from data_pipeline_definitions.base_classes.script_component import ScriptComponent
import argparse
import subprocess
from custom_helpers_py.get_paths import get_artifacts_folder_path
from os.path import join, exists
from custom_helpers_py.folder_helpers import mkdir_if_not_exists, mkdir_to_ensure_path
from custom_helpers_py.date_helpers import (
    get_current_date_filename,
    get_date_diff_string,
)
import argparse
from typing import Any
import json
import sys
from datetime import datetime


class Tee:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)

    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2


class RunInfoFolder:
    def __init__(self, folder_path="") -> None:
        self.set_folder_path(folder_path)

    def set_folder_path(self, new_path: str):
        self.folder_path = new_path
        if new_path:
            mkdir_if_not_exists(new_path)

    def initialize_run_info_folder(
        self, inputs: dict[str, Any], steps: list[ScriptComponent]
    ):
        # Create inputs
        full_command_str = " ".join(sys.argv)
        inputs_str = json.dumps(inputs)

        with open(join(self.folder_path, "inputs.txt"), "w") as outfile:
            outfile.write(full_command_str + "\n" + inputs_str)

        # Create full_script
        full_script = "\n\n".join([step.get_debug_str() for step in steps])
        with open(join(self.folder_path, "full_script.txt"), "w") as outfile:
            outfile.write(full_script)

    def add_step_to_progress(self, step_id: int, com: ScriptComponent):
        with open(join(self.folder_path, "progress.txt"), "a") as outfile:
            outfile.write(str(step_id) + " : " + str(com) + "\n")

    def open_pipeline_log(self):
        run_log_file = open(join(self.folder_path, "run_log.txt"), "a")
        error_log_file = open(join(self.folder_path, "error_log.txt"), "a")

        self.backup_stdout = sys.stdout
        self.backup_stderr = sys.stderr

        sys.stdout = Tee(run_log_file, sys.stdout)
        sys.stderr = Tee(error_log_file, sys.stderr)

        self.run_log_file = run_log_file
        self.error_log_file = error_log_file

    def close_pipleline_log(self):
        if not self.backup_stdout or not self.backup_stderr:
            raise Exception("No backup std out to revert to!")
        sys.stdout = self.backup_stdout
        sys.stderr = self.backup_stderr

        self.run_log_file.close()
        self.error_log_file.close()

        self.run_log_file = None
        self.error_log_file = None

    def add_to_timing(self, message: str, start_time: datetime, end_time: datetime):
        to_write = (
            "\n".join(
                [
                    message,
                    "start: " + start_time.isoformat(),
                    "end: " + end_time.isoformat(),
                    "duration: " + get_date_diff_string(end_time, start_time),
                ]
            )
            + "\n\n"
        )

        with open(join(self.folder_path, "timing.txt"), "a") as outfile:
            outfile.write(to_write)


class DataPipeline(ABC):
    def __init__(self, pipeline_run_folder_path="") -> None:
        super().__init__()
        self.run_info_folder = RunInfoFolder(pipeline_run_folder_path)
        self.set_pipeline_run_folder_path(pipeline_run_folder_path)

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
        self.run_info_folder.open_pipeline_log()

        print("[ORCHESTRATOR] Pipeline run started", self.get_pipeline_name())
        run_start_time = datetime.now()

        is_success_run = True
        for i, com in enumerate(steps):
            if i < start_index:
                continue

            if end_index and i >= end_index:
                continue

            print("\n[STEP START]", i, com)

            com_start_time = datetime.now()
            execution_error = None
            try:
                # Make all directories as needed
                path_in_args = com.get_paths_in_args()
                for in_path in path_in_args:
                    mkdir_to_ensure_path(in_path)

                # Run
                script_to_run = str(com)
                process = subprocess.Popen(
                    script_to_run, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                for line in iter(process.stdout.readline, b""):
                    print(">>> " + str(line.rstrip()))

                for line in iter(process.stderr.readline, b""):
                    print(">>> " + str(line.rstrip()), file=sys.stderr)

                process.wait()
                returncode = process.returncode
                if returncode > 0:
                    raise Exception("Step failed")

                self.run_info_folder.add_step_to_progress(i, com)
            except Exception as error:
                execution_error = error

            com_end_time = datetime.now()
            self.run_info_folder.add_to_timing(
                str(i) + " " + com.component_name, com_start_time, com_end_time
            )

            if execution_error:
                is_success_run = False
                print("[ERROR STEP]", i, com, "\n")
                print(execution_error, file=sys.stderr)
                break

            print("[STEP SUCCESS]", i, com, "\n")

        run_end_time = datetime.now()
        self.run_info_folder.add_to_timing("Pipeline run", run_start_time, run_end_time)

        if is_success_run:
            print("[ORCHESTRATOR] Pipeline run completed successfully")
        else:
            print("[ORCHESTRATOR] Pipeline Run FAILED!")

        self.run_info_folder.close_pipleline_log()

    def print_steps(self, **kwargs):
        steps_to_run: list[ScriptComponent] = self.get_steps(**kwargs)
        for step in steps_to_run:
            print(step.get_debug_str())
            print()

    def set_pipeline_run_folder_path(self, new_path: str):
        self.pipeline_run_folder_path = new_path
        if new_path:
            mkdir_if_not_exists(new_path)
            run_info_folder_path = join(new_path, "run_info")
        else:
            run_info_folder_path = ""

        self.run_info_folder.set_folder_path(run_info_folder_path)

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
        self.set_pipeline_run_folder_path(pipeline_run_folder_path)

        steps_to_run = []
        if is_retry:
            print("TODO: IMPLEMENT RETRIES")
            exit(1)

        steps_to_run = self.get_steps(**vars(cli_args))

        self.run_info_folder.initialize_run_info_folder(vars(cli_args), steps_to_run)

        # Run steps
        self.run_steps(steps_to_run, **vars(cli_args))
