from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join
from pipeline_definitions.duckster import DucksterPipeline
from custom_helpers_py.custom_classes.index_cache import IndexCache
import math


class BlinkSearchMainPipeline(DataPipeline):
    def get_base_pipeline_name(self) -> str:
        return "blink_search_main"

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        if not kwargs.get("upsync"):
            raise Exception("Blinks only support upsyncs at the moment")

        out_folder_path = self.pipeline_run_folder_path

        is_prod: bool = kwargs.get("prod", False) or False

        # Get number of batches
        # TODO: Better system to reduce logic duplication here

        ic = IndexCache(prod=is_prod)
        recent_urls_list = ic.get_urls(only_uploaded=True)
        BATCH_SIZE = 1000
        NUM_BATCHES = math.ceil(len(recent_urls_list) / BATCH_SIZE)

        to_return = []
        folder_path_in_urls = join(out_folder_path, "in_urls")
        com_get_urls = ScriptComponent(
            component_name="get cached urls",
            body="python com_cache/get_cached_urls.py",
            args=[
                ComponentArg(
                    arg_name="out-folder-path",
                    arg_val=folder_path_in_urls,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="batch-size",
                    arg_val=BATCH_SIZE,
                ),
                ComponentArg(
                    arg_name="only-uploaded",
                    arg_val=True,
                ),
                ComponentArg(
                    arg_name="prod",
                    arg_val=kwargs.get("prod"),
                ),
            ],
        )
        to_return.append(com_get_urls)

        folder_path_duckster_root = join(out_folder_path, "duckster_root")

        for batch_idx in range(NUM_BATCHES):
            run_folder_path = join(folder_path_duckster_root, "batch_" + str(batch_idx))
            url_file_path = join(
                folder_path_in_urls, "batch_" + str(batch_idx) + ".csv"
            )

            duckster_args = {
                **kwargs,
                "in_url_file_path": url_file_path,
                "delete_rejected": True,
                "upsync": True,
                "safe-indiv-scrape": True,
                "skip_cache_filter": True,
            }
            duckster_steps = DucksterPipeline(
                pipeline_run_folder_path=run_folder_path,
                run_name=self.run_name,
                parent_pipeline_name=self.get_pipeline_name(),
            ).get_steps(**duckster_args)
            to_return += duckster_steps

        return to_return


if __name__ == "__main__":
    BlinkSearchMainPipeline().run_from_cli()
