from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join
from pipeline_definitions.duckster import DucksterPipeline
from custom_helpers_py.get_paths import get_dev_scrape_folder_path


class CarthagoPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "carthago"

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path

        folder_path_source_scrapes = join(out_folder_path, "source_scrapes")
        folder_path_cc_source_scrapes = join(out_folder_path, "cc_source_scrapes")

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

            file_path_cc = join(
                folder_path_cc_source_scrapes, "cc_source_" + acr + "_scrape.csv"
            )
            com_cc = ScriptComponent(
                component_name="cc source " + acr + " scrape",
                body="python com_cc/cc_source_" + acr + "_scrape.py",
                args=[
                    ComponentArg(
                        arg_name="i",
                        arg_val=folder_path_scrape,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="o",
                        arg_val=file_path_cc,
                        is_path=True,
                    ),
                ],
            )

            source_specific_com_list += [
                com_scrape,
                com_cc,
            ]

        # Combine data
        file_path_combined_data = join(out_folder_path, "combined_data.csv")
        com_combine_data = ScriptComponent(
            component_name="combine source data",
            body="python com_cc/combine_source_cc.py",
            args=[
                ComponentArg(
                    arg_name="i",
                    arg_val=folder_path_cc_source_scrapes,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="o",
                    arg_val=file_path_combined_data,
                    is_path=True,
                ),
            ],
        )

        # Combine urls
        file_path_combined_urls = join(out_folder_path, "combined_urls.csv")
        com_get_urls = ScriptComponent(
            component_name="grab urls from combined source",
            body="python com_utils/util_extract_column_from_data.py",
            args=[
                ComponentArg(
                    arg_name="i",
                    arg_val=file_path_combined_data,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="o",
                    arg_val=file_path_combined_urls,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="in-col",
                    arg_val="clean_product_url",
                ),
                ComponentArg(
                    arg_name="out-col",
                    arg_val="url",
                ),
            ],
        )

        # Call duckster
        duckster_args = {
            **kwargs,
            "in_file_path": file_path_combined_urls,
            "combined_source_file_path": file_path_combined_data,
        }
        duckster_steps = DucksterPipeline(
            pipeline_run_folder_path=self.pipeline_run_folder_path
        ).get_steps(**duckster_args)

        to_return = [
            *source_specific_com_list,
            com_combine_data,
            com_get_urls,
            *duckster_steps,
        ]
        return to_return


if __name__ == "__main__":
    CarthagoPipeline().run_from_cli()
