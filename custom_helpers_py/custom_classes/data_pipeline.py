from abc import ABC, abstractmethod
from custom_helpers_py.custom_classes.script_component import ScriptComponent
import argparse
import subprocess


class DataPipeline(ABC):
    @abstractmethod
    def get_name() -> str:
        pass

    @abstractmethod
    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        pass

    def run_steps(self, **kwargs):
        steps = self.get_steps(kwargs)

        start_index, end_index = kwargs["start_index"], kwargs["end_index"]

        # Run all scripts

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

            process = subprocess.run(com, shell=True)
            returncode = process.returncode
            if returncode > 0:
                print("[ORCHESTRATOR] ERROR", i, com)
                break

            print("[ORCHESTRATOR] END script", i, com)
            pass

    def add_cli_args(parser):
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

    def run_from_cli():
        pass
