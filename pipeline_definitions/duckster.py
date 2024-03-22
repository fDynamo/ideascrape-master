from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join
import argparse
from custom_helpers_py.get_paths import get_dev_scrape_folder_path
from pipeline_definitions.upsync import UpsyncPipeline


class DucksterPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "duckster"

    def add_cli_args(self, parser):
        parser.add_argument(
            "-i", "--inUrlFilePath", type=str, dest="in_file_path", required=True
        )
        parser.add_argument(
            "--combinedSourceFilePath",
            type=str,
            dest="combined_source_file_path",
        )
        parser.add_argument(
            "--skipUrlFilter",
            type=bool,
            action=argparse.BooleanOptionalAction,
            dest="skip_url_filter",
            default=False,
        )

        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path

        file_path_urls_for_indiv_scrape = join(
            out_folder_path, "urls_for_indiv_scrape.csv"
        )
        folder_path_rejected = join(out_folder_path, "rejected")
        file_path_rejected_urls = join(folder_path_rejected, "rejected_urls.csv")
        com_filter_urls_indiv = ScriptComponent(
            component_name="filter urls",
            body="python com_filters/filter_urls_indiv.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=kwargs["in_file_path"], is_path=True
                ),
                ComponentArg(
                    arg_name="o", arg_val=file_path_urls_for_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_urls, is_path=True
                ),
                ComponentArg(arg_name="prod-env", arg_val=kwargs.get("prod", False)),
                ComponentArg(
                    arg_name="disable-filter",
                    arg_val=kwargs.get("skip_url_filter", False),
                ),
            ],
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

        file_path_cc_indiv_scrape = join(out_folder_path, "cc_indiv_scrape.csv")
        file_path_rejected_indiv_scrape = join(
            folder_path_rejected, "rejected_indiv_scrape.csv"
        )
        folder_path_analyzed_page_copy = join(out_folder_path, "analyzed_page_copy")
        com_analyze_and_cc_indiv_scrape = ScriptComponent(
            component_name="analyze and cc indiv scrape",
            body="python com_cc/analyze_and_cc_indiv_scrape.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=folder_path_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="o", arg_val=file_path_cc_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="r", arg_val=file_path_rejected_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="analyzedPageCopyFolderPath",
                    arg_val=folder_path_analyzed_page_copy,
                    is_path=True,
                ),
            ],
        )

        file_path_filtered_cc_indiv_scrape_domains = join(
            out_folder_path, "new_product_domains.csv"
        )
        com_get_filtered_indiv_scrape_domains = ScriptComponent(
            component_name="get filtered domains",
            body="python com_utils/util_convert_url_column_to_domain.py",
            args=[
                ["i", file_path_cc_indiv_scrape],
                ["o", file_path_filtered_cc_indiv_scrape_domains],
                ["c", "init_url"],
            ],
        )

        file_path_domains_for_sup_similarweb_scrape = join(
            out_folder_path, "domains_for_sup_similarweb_scrape.csv"
        )
        com_filter_domains_for_sup_similarweb = ScriptComponent(
            component_name="filter domains for sup similarweb",
            body="python com_filters/filter_domains_sup_similarweb.py",
            args=[
                ["i", file_path_filtered_cc_indiv_scrape_domains],
                ["o", file_path_domains_for_sup_similarweb_scrape],
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

        file_path_cc_sup_similarweb_scrape = join(
            out_folder_path, "cc_sup_similarweb_scrape.csv"
        )
        com_cc_sup_similarweb_scrape = ScriptComponent(
            component_name="cc sup similarweb scrape",
            body="python com_cc/cc_sup_similarweb_scrape.py",
            args=[
                ["i", folder_path_sup_similarweb_scrape],
                ["o", file_path_cc_sup_similarweb_scrape],
            ],
        )

        file_path_desc_to_embed = join(out_folder_path, "desc_to_embed.csv")
        com_gen_desc_to_embed = ScriptComponent(
            component_name="generate desc to embed",
            body="python com_search_extract/gen_desc_to_embed.py",
            args=[
                ComponentArg(
                    arg_name="i",
                    arg_val=file_path_cc_indiv_scrape,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="o", arg_val=file_path_desc_to_embed, is_path=True
                ),
            ],
        )

        folder_path_desc_embeddings = join(out_folder_path, "desc_embeddings")
        com_embed_desc_vector = ScriptComponent(
            component_name="embed desc vectors",
            body="python com_search_extract/embed_desc_vector.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=file_path_desc_to_embed, is_path=True
                ),
                ComponentArg(
                    arg_name="o",
                    arg_val=folder_path_desc_embeddings,
                    is_path=True,
                ),
            ],
        )
        if kwargs["use_dev_scrape"]:
            folder_path_desc_embeddings = join(
                get_dev_scrape_folder_path(),
                "desc_embeddings",
            )
            com_embed_desc_vector.erase()

        file_path_product_image_urls = join(out_folder_path, "product_image_urls.csv")
        com_gen_product_image_urls = ScriptComponent(
            component_name="generate image urls",
            body="python com_search_extract/gen_product_image_urls.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=file_path_cc_indiv_scrape, is_path=True
                ),
                ComponentArg(
                    arg_name="o", arg_val=file_path_product_image_urls, is_path=True
                ),
            ],
        )

        folder_path_product_images = join(out_folder_path, "product_images")
        com_download_product_images = ScriptComponent(
            component_name="download product images",
            body="python com_search_extract/download_product_images.py",
            args=[
                ComponentArg(
                    arg_name="i", arg_val=file_path_product_image_urls, is_path=True
                ),
                ComponentArg(
                    arg_name="o", arg_val=folder_path_product_images, is_path=True
                ),
            ],
        )
        if kwargs["use_dev_scrape"]:
            folder_path_product_images = join(
                get_dev_scrape_folder_path(),
                "product_images",
            )
            com_download_product_images.erase()

        folder_path_prod_tables = join(out_folder_path, "prod_tables")
        com_prodify = ScriptComponent(
            component_name="prodify",
            body="python com_special/prodify.py",
            args=[
                ComponentArg(
                    arg_name="o", arg_val=folder_path_prod_tables, is_path=True
                ),
                ComponentArg(
                    arg_name="ccIndivScrapeFilePath",
                    arg_val=file_path_cc_indiv_scrape,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="descEmbeddingsFolderPath",
                    arg_val=folder_path_desc_embeddings,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="productImagesFolderPath",
                    arg_val=folder_path_product_images,
                    is_path=True,
                ),
                ComponentArg(
                    arg_name="ccSupSimilarwebScrapeFilePath",
                    arg_val=file_path_cc_sup_similarweb_scrape,
                    is_path=True,
                ),
            ],
        )

        if kwargs.get("combined_source_file_path", False):
            com_prodify.add_arg(
                ComponentArg(
                    arg_name="combinedSourceFilePath",
                    arg_val=kwargs.get("combined_source_file_path"),
                    is_path=True,
                ),
            )

        to_return = [
            com_filter_urls_indiv,
            com_indiv_scrape,
            com_analyze_and_cc_indiv_scrape,
            com_get_filtered_indiv_scrape_domains,
            com_filter_domains_for_sup_similarweb,
            com_scrape_sup_similarweb,
            com_cc_sup_similarweb_scrape,
            com_gen_desc_to_embed,
            com_embed_desc_vector,
            com_gen_product_image_urls,
            com_download_product_images,
            com_prodify,
        ]

        if kwargs.get("upsync"):
            # Call upsync
            upsync_args = {
                **kwargs,
                "upsert_folder_path": folder_path_prod_tables,
                "upsert_images_folder_path": folder_path_product_images,
            }
            upsync_steps = UpsyncPipeline(
                pipeline_run_folder_path=self.pipeline_run_folder_path
            ).get_steps(**upsync_args)

            to_return += upsync_steps

        return to_return


if __name__ == "__main__":
    DucksterPipeline().run_from_cli()
