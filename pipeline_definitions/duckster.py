from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from pipeline_definitions.upsync import UpsyncPipeline
from os.path import join
import argparse
from custom_helpers_py.get_paths import get_dev_scrape_folder_path


class DucksterPipeline(DataPipeline):
    def get_base_pipeline_name(self) -> str:
        return "duckster"

    def add_cli_args(self, parser):
        parser.add_argument(
            "-i", "--in-url-file-path", type=str, dest="in_url_file_path", required=True
        )
        parser.add_argument(
            "--skip-url-filter",
            type=bool,
            dest="skip_url_filter",
            action=argparse.BooleanOptionalAction,
        )
        parser.add_argument(
            "--reset-tp",
            type=bool,
            dest="reset_tp",
            action=argparse.BooleanOptionalAction,
        )

        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path

        # Common folder paths
        tp_folder_path = kwargs.get("in_tp_folder_path", None)
        is_existing_tp = False
        if not tp_folder_path:
            tp_folder_path = join(out_folder_path, ".tpd")
        else:
            is_existing_tp = True

        folder_path_rejected = join(out_folder_path, "rejected")

        file_path_urls_for_indiv_scrape = join(
            out_folder_path, "urls_for_indiv_scrape.csv"
        )
        file_path_rejected_urls = join(folder_path_rejected, "rejected_urls.csv")
        com_filter_urls_indiv = ScriptComponent(
            component_name="filter urls",
            body="python com_filters/filter_urls_indiv.py",
            args=[
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
                ComponentArg(
                    arg_name="o", arg_val=file_path_urls_for_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_urls, is_path=True
                ),
                ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
                ComponentArg(
                    arg_name="reset-tp",
                    arg_val=kwargs.get("reset_tp", False),
                ),
                ComponentArg(
                    arg_name="disable-filter",
                    arg_val=kwargs.get("skip_url_filter", False),
                ),
                ComponentArg(
                    arg_name="use-cache-filter",
                    arg_val=True,
                ),
            ],
        )
        if is_existing_tp:
            com_filter_urls_indiv.add_arg(
                ComponentArg(
                    arg_name="use-tp-as-input",
                    arg_val=True,
                )
            )
        elif kwargs.get("in_url_file_path"):
            com_filter_urls_indiv.add_arg(
                ComponentArg(
                    arg_name="i", arg_val=kwargs["in_url_file_path"], is_path=True
                )
            )

        folder_path_indiv_scrape = join(out_folder_path, "indiv_scrape")
        com_indiv_scrape = ScriptComponent(
            component_name="indiv scrape",
            body="npm run indiv_scrape",
            args=[
                ["urlListFilePath", file_path_urls_for_indiv_scrape],
                ["outFolder", folder_path_indiv_scrape],
            ],
        )
        if kwargs["use_dev_scrape"]:
            folder_path_indiv_scrape = join(
                get_dev_scrape_folder_path(),
                "indiv_scrape",
            )
            com_indiv_scrape.erase()

        file_path_rejected_indiv_scrape = join(
            folder_path_rejected, "rejected_indiv_scrape.csv"
        )
        com_analyze_indiv_scrape = ScriptComponent(
            component_name="analyze indiv scrape",
            body="python com_analyze/analyze_indiv_scrape.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=folder_path_indiv_scrape, is_path=True
                ),
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_indiv_scrape, is_path=True
                ),
            ],
        )

        file_path_domains_for_sup_similarweb_scrape = join(
            out_folder_path, "domains_for_sup_similarweb_scrape.csv"
        )
        file_path_rejected_sw_domains = join(
            folder_path_rejected, "rejected_sw_domains.csv"
        )
        com_filter_domains_for_sup_similarweb = ScriptComponent(
            component_name="filter domains for sup similarweb",
            body="python com_filters/filter_domains_sup_similarweb.py",
            args=[
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
                ComponentArg(
                    arg_name="o",
                    arg_val=file_path_domains_for_sup_similarweb_scrape,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_sw_domains, is_path=True
                ),
                ComponentArg(arg_name="use-tp-as-input", arg_val=True),
            ],
        )

        folder_path_sup_similarweb_scrape = join(
            out_folder_path, "sup_similarweb_scrape"
        )
        com_scrape_sup_similarweb = ScriptComponent(
            component_name="scrape sup similarweb",
            body="npm run sup_similarweb_scrape",
            args=[
                ["domainListFilepath", file_path_domains_for_sup_similarweb_scrape],
                ["outFolder", folder_path_sup_similarweb_scrape],
            ],
        )
        if kwargs["use_dev_scrape"]:
            folder_path_sup_similarweb_scrape = join(
                get_dev_scrape_folder_path(),
                "sup_similarweb_scrape",
            )
            com_scrape_sup_similarweb.erase()

        com_analyze_similarweb = ScriptComponent(
            component_name="analyze sw",
            body="python com_analyze/analyze_similarweb_scrape.py",
            args=[
                ComponentArg(
                    arg_name="i",
                    arg_val=folder_path_sup_similarweb_scrape,
                    is_path=True,
                ),
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
            ],
        )

        com_embed_search_vector = ScriptComponent(
            component_name="embed search vectors",
            body="python com_search_extract/embed_search_vector.py",
            args=[
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
            ],
        )

        if kwargs["use_dev_scrape"]:
            com_embed_search_vector.body = "python com_utils/copy_file.py"
            com_embed_search_vector.args = [
                ComponentArg(
                    arg_name="i",
                    arg_val=join(
                        get_dev_scrape_folder_path(),
                        ".tpd",
                        "part_search_vector_embeddings.json",
                    ),
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="o",
                    arg_val=join(
                        tp_folder_path,
                        "part_search_vector_embeddings.json",
                    ),
                    is_path=True,
                ),
            ]

        folder_path_product_images = join(out_folder_path, "product_images")
        com_download_product_images = ScriptComponent(
            component_name="download product images",
            body="python com_search_extract/download_product_images.py",
            args=[
                ComponentArg(
                    arg_name="o", arg_val=folder_path_product_images, is_path=True
                ),
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
            ],
        )
        if kwargs["use_dev_scrape"]:
            folder_path_product_images = join(
                get_dev_scrape_folder_path(),
                "product_images",
            )
            com_download_product_images.erase()

        folder_path_upsync = join(out_folder_path, "upsync_files")
        file_path_rejected_prodify = join(folder_path_rejected, "prodify.json")
        com_prodify = ScriptComponent(
            component_name="prodify",
            body="python com_special/prodify.py",
            args=[
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
                ComponentArg(arg_name="o", arg_val=folder_path_upsync, is_path=True),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_prodify, is_path=True
                ),
            ],
        )

        if kwargs["use_dev_scrape"]:
            com_prodify.add_arg(
                ComponentArg(
                    arg_name="ignore-missing-search-vector",
                    arg_val=True,
                ),
            )

        cache_run_name = self.get_pipeline_name() + "_" + self.run_name
        com_cache_pre_upsync = ScriptComponent(
            component_name="cache pre upsync",
            body="python com_cache/cache_pre_upsync.py",
            args=[
                ComponentArg(arg_name="tp", arg_val=tp_folder_path, is_path=True),
                ComponentArg(arg_name="run-name", arg_val=cache_run_name),
                ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
            ],
        )

        to_return = [
            com_filter_urls_indiv,
            com_indiv_scrape,
            com_analyze_indiv_scrape,
            com_filter_domains_for_sup_similarweb,
            com_scrape_sup_similarweb,
            com_analyze_similarweb,
            com_embed_search_vector,
            com_download_product_images,
            com_prodify,
            com_cache_pre_upsync,
        ]

        if kwargs.get("upsync"):
            upsync_args = {
                **kwargs,
                "upsync_folder_path": folder_path_upsync,
                "upsert_images_folder_path": folder_path_product_images,
            }
            upsync_steps = UpsyncPipeline(
                pipeline_run_folder_path=self.pipeline_run_folder_path,
                run_name=self.run_name,
                parent_pipeline_name=self.get_pipeline_name(),
            ).get_steps(**upsync_args)
            to_return += upsync_steps

        return to_return


if __name__ == "__main__":
    DucksterPipeline().run_from_cli()
