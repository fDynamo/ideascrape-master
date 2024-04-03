from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
import argparse


class BunTestPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "bun_test_pipeline"

    def add_cli_args(self, parser):
        parser.add_argument("--someInput", type=str, dest="some_input")
        parser.add_argument(
            "--error", type=bool, action=argparse.BooleanOptionalAction, default=False
        )
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        com_one = ScriptComponent(
            component_name="one",
            body="bun test.ts",
        )

        to_return = [com_one]

        return to_return


if __name__ == "__main__":
    BunTestPipeline().run_from_cli()
