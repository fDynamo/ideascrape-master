from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join
from pipeline_definitions.duckster import DucksterPipeline
from custom_helpers_py.get_paths import get_dev_scrape_folder_path
import argparse


class CarthagoPipeline(DataPipeline):
    def get_base_pipeline_name(self) -> str:
        return "carthago"

    def add_cli_args(self, parser):
        parser.add_argument(
            "--skip-duckster",
            type=bool,
            dest="skip_duckster",
            action=argparse.BooleanOptionalAction,
        )

        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path

        folder_path_source_scrapes = join(out_folder_path, "source_scrapes")
        tp_folder_path = join(out_folder_path, ".tpd")

        SOURCE_ACRONYM_LIST = ["ph", "aift"]

        source_specific_com_list = []

        for acr in SOURCE_ACRONYM_LIST:
            folder_path_scrape = join(
                folder_path_source_scrapes, "source_{}_scrape".format(acr)
            )

            com_scrape = ScriptComponent(
                component_name="source " + acr + " scrape",
                body="npm run source_" + acr + "_scrape",
                args=[
                    ComponentArg(
                        arg_name="outFolder",
                        arg_val=folder_path_scrape,
                        is_path=True,
                    ),
                ],
            )

            if kwargs["use_dev_scrape"]:
                folder_path_scrape = join(
                    get_dev_scrape_folder_path(),
                    "source_scrapes",
                    "source_{}_scrape".format(acr),
                )
                com_scrape.erase()

            com_analyze = ScriptComponent(
                component_name="analyze source " + acr,
                body="python com_analyze/analyze_src_" + acr + "_scrape.py",
                args=[
                    ComponentArg(
                        arg_name="i",
                        arg_val=folder_path_scrape,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="tp",
                        arg_val=tp_folder_path,
                        is_path=True,
                    ),
                ],
            )

            source_specific_com_list += [
                com_scrape,
                com_analyze,
            ]

        # Call duckster
        to_return = [
            *source_specific_com_list,
        ]
        if not kwargs.get("skip_duckster"):
            duckster_args = {**kwargs, "in_tp_folder_path": tp_folder_path}
            duckster_steps = DucksterPipeline(
                pipeline_run_folder_path=self.pipeline_run_folder_path,
                run_name=self.run_name,
                parent_pipeline_name=self.get_pipeline_name(),
            ).get_steps(**duckster_args)
            to_return += duckster_steps

        return to_return


if __name__ == "__main__":
    CarthagoPipeline().run_from_cli()
