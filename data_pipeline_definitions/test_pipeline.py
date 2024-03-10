from data_pipeline_definitions.base_classes.data_pipeline import DataPipeline
from data_pipeline_definitions.base_classes.script_component import ScriptComponent


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
            args=[["prod", kwargs["some_input"]]],
        )
        return [com_cache_sup_similarweb_records]


if __name__ == "__main__":
    TestPipeline().run_from_cli()
