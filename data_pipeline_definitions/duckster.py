from data_pipeline_definitions.base_classes.data_pipeline import DataPipeline
from data_pipeline_definitions.base_classes.script_component import ScriptComponent
from os.path import join


class DucksterPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "duckster"

    def add_cli_args(self, parser):
        parser.add_argument("-i", "--inUrlFileName", type=str, dest="in_file_path")
        parser.add_argument(
            "--combinedSourceFilePath", type=str, dest="combined_source_file_path"
        )
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path

        file_path_urls_for_indiv_scrape = join(
            out_folder_path, "urls_for_indiv_scrape.csv"
        )
        com_filter_urls_indiv = ScriptComponent(
            component_name="filter urls",
            body="python data_transformers/filter_urls_indiv.py",
            args=[
                ["i", kwargs["in_file_path"]],
                ["o", file_path_urls_for_indiv_scrape],
                ["prod-env", kwargs["prod"]],
            ],
        )

        folder_path_indiv_scrape = join(out_folder_path, "indiv_scrape")
        com_create_folder_indiv_scrape = ScriptComponent(
            component_name="create folder indiv scrape",
            body="python data_transformers/create_folder.py",
            args=[["i", folder_path_indiv_scrape]],
        )

        com_indiv_scrape = ScriptComponent(
            component_name="indiv scrape",
            body="npm run indiv_scrape",
            args=[
                ["urlListFilepath", file_path_urls_for_indiv_scrape],
                ["outFolder", folder_path_indiv_scrape],
            ],
        )

        file_path_cc_indiv_scrape = join(out_folder_path, "cc_indiv_scrape.csv")
        com_cc_indiv_scrape = ScriptComponent(
            component_name="cc indiv scrape",
            body="python data_transformers/cc_indiv_scrape.py",
            args=[
                ["i", folder_path_indiv_scrape],
                ["o", file_path_cc_indiv_scrape],
            ],
        )

        file_path_filtered_cc_indiv_scrape = join(
            out_folder_path, "filtered_cc_indiv_scrape.csv"
        )
        com_filter_indiv_scrape = ScriptComponent(
            component_name="filter indiv scrape",
            body="python data_transformers/filter_indiv_scrape.py",
            args=[
                ["i", file_path_cc_indiv_scrape],
                ["o", file_path_filtered_cc_indiv_scrape],
            ],
        )

        file_path_filtered_cc_indiv_scrape_domains = join(
            out_folder_path, "filtered_cc_indiv_scrape_domains.csv"
        )
        com_get_filtered_indiv_scrape_domains = ScriptComponent(
            component_name="get filtered domains",
            body="python data_transformers/util_convert_url_column_to_domain.py",
            args=[
                ["i", file_path_filtered_cc_indiv_scrape],
                ["o", file_path_filtered_cc_indiv_scrape_domains],
                ["c", "url"],
            ],
        )

        file_path_domains_for_sup_similarweb_scrape = join(
            out_folder_path, "domains_for_sup_similarweb_scrape.csv"
        )
        com_filter_domains_for_sup_similarweb = ScriptComponent(
            component_name="filter domains for sup similarweb",
            body="python data_transformers/filter_domains_sup_similarweb.py",
            args=[
                ["i", file_path_filtered_cc_indiv_scrape_domains],
                ["o", file_path_domains_for_sup_similarweb_scrape],
            ],
        )

        folder_path_sup_similarweb_scrape = join(
            out_folder_path, "sup_similarweb_scrape"
        )
        com_create_folder_sup_similarweb_scrape = ScriptComponent(
            component_name="create folder sup similarweb scrape",
            body="python data_transformers/create_folder.py",
            args=[["i", folder_path_sup_similarweb_scrape]],
        )

        com_scrape_sup_similarweb = ScriptComponent(
            component_name="scrape sup similarweb",
            body="npm run sup_similarweb_scrape",
            args=[
                ["domainListFilepath", file_path_domains_for_sup_similarweb_scrape],
                ["outFolder", folder_path_sup_similarweb_scrape],
            ],
        )

        file_path_cc_sup_similarweb_scrape = join(
            out_folder_path, "cc_sup_similarweb_scrape.csv"
        )
        com_cc_sup_similarweb_scrape = ScriptComponent(
            component_name="cc sup similarweb scrape",
            body="python data_transformers/cc_sup_similarweb_scrape.py",
            args=[
                ["domainListFilepath", file_path_domains_for_sup_similarweb_scrape],
                ["outFolder", folder_path_sup_similarweb_scrape],
            ],
        )

        file_path_pre_extract = join(out_folder_path, "pre_extract.csv")
        com_gen_pre_extract = ScriptComponent(
            component_name="generate pre extract",
            body="python data_transformers/gen_pre_extract.py",
            args=[
                ["cc-indiv-scrape-filepath", file_path_filtered_cc_indiv_scrape],
                [
                    "cc-sup-similarweb-scrape-filepath",
                    file_path_cc_sup_similarweb_scrape,
                ],
                ["o", file_path_pre_extract],
            ],
        )

        if kwargs["combined_source_file_path"]:
            com_gen_pre_extract.add_arg(
                ["combined-source-filepath", kwargs["combined_source_file_path"]]
            )

        file_path_embedded_descriptions = join(
            out_folder_path, "embedded_descriptions.csv"
        )
        com_extract_embed_description = ScriptComponent(
            component_name="extract embed description",
            body="python data_transformers/extract_embed_description.py",
            args=[
                ["i", file_path_pre_extract],
                ["o", file_path_embedded_descriptions],
            ],
        )

        folder_path_product_images = join(out_folder_path, "product_images")
        com_create_folder_product_images = ScriptComponent(
            component_name="create folder product images",
            body="python data_transformers/create_folder.py",
            args=[["i", folder_path_product_images]],
        )

        com_download_product_images = ScriptComponent(
            component_name="download product images",
            body="python data_transformers/download_product_images.py",
            args=[
                ["i", file_path_pre_extract],
                ["o", folder_path_product_images],
            ],
        )

        folder_path_prod_tables = join(out_folder_path, "prod_tables")
        com_create_folder_prod_tables = ScriptComponent(
            component_name="create folder prod tables",
            body="python data_transformers/create_folder.py",
            args=[["i", folder_path_prod_tables]],
        )

        com_prodify = ScriptComponent(
            component_name="prodify",
            body="python data_transformers/prodify.py",
            args=[
                ["i", file_path_pre_extract],
                ["o", folder_path_prod_tables],
                ["embedding-description-filepath", file_path_embedded_descriptions],
                ["product-images-folderpath", folder_path_product_images],
            ],
        )

        return [
            com_filter_urls_indiv,
            com_create_folder_indiv_scrape,
            com_indiv_scrape,
            com_cc_indiv_scrape,
            com_filter_indiv_scrape,
            com_get_filtered_indiv_scrape_domains,
            com_filter_domains_for_sup_similarweb,
            com_create_folder_sup_similarweb_scrape,
            com_scrape_sup_similarweb,
            com_cc_sup_similarweb_scrape,
            com_extract_embed_description,
            com_create_folder_product_images,
            com_download_product_images,
            com_create_folder_prod_tables,
            com_prodify,
        ]


if __name__ == "__main__":
    DucksterPipeline().run_from_cli()
