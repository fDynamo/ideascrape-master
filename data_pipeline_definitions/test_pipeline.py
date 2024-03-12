from data_pipeline_definitions.base_classes.data_pipeline import DataPipeline
from data_pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)


class TestPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "test_pipeline"

    def add_cli_args(self, parser):
        parser.add_argument("--someInput", type=str, dest="some_input")
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        com_cache_sup_similarweb_records = ScriptComponent(
            component_name="test",
            body="echo ",
            args=[
                ComponentArg(arg_name="box", arg_val="boom"),
                ComponentArg(arg_name="second", arg_val="lol"),
                ComponentArg(arg_name="whaT", arg_val=False),
            ],
        )
        return [com_cache_sup_similarweb_records]


if __name__ == "__main__":
    TestPipeline().run_from_cli()
