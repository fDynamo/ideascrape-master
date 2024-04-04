from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
import argparse


class TestPipeline(DataPipeline):
    def get_base_pipeline_name(self) -> str:
        return "test_pipeline"

    def add_cli_args(self, parser):
        parser.add_argument("--someInput", type=str, dest="some_input")
        parser.add_argument(
            "--error", type=bool, action=argparse.BooleanOptionalAction, default=False
        )
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        com_one = ScriptComponent(
            component_name="one",
            body="echo ",
            args=[
                ComponentArg(arg_name="box", arg_val="boom"),
                ComponentArg(arg_name="first", arg_val="lol"),
                ComponentArg(arg_name="whaT", arg_val=False),
            ],
        )
        com_two = ScriptComponent(
            component_name="two",
            body="echo ",
            args=[
                ComponentArg(arg_name="second", arg_val="arg"),
            ],
        )

        to_return = [com_one, com_two]

        if kwargs.get("error", False):
            to_return.append(
                ScriptComponent(component_name="ERROR com", body="nigfuaoeh lol")
            )

        return to_return


if __name__ == "__main__":
    TestPipeline().run_from_cli()
