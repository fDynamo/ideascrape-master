from data_pipeline_definitions.base_classes.data_pipeline import DataPipeline
from data_pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join

# TODO: Remove functionality


class UpsyncPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "upsync"

    def add_cli_args(self, parser):
        parser.add_argument(
            "--upsertFolderPath", type=str, dest="upsert_folder_path", required=True
        )
        parser.add_argument(
            "--upsertImagesFolderPath",
            type=str,
            dest="upsert_folder_path",
            required=True,
        )
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path
        records_folder_path = join(out_folder_path, "records")

        # Upsert tables
        to_return = []

        upsert_folder_path = kwargs.get("upsert_folder_path")
        if upsert_folder_path:
            com_upsert = ScriptComponent(
                component_name="upsert data",
                body="npm run pi_upsert_records",
                args=[
                    ComponentArg(
                        arg_name="toUploadFolderPath",
                        arg_val=upsert_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="recordsFolderPath",
                        arg_val=records_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
                ],
            )
            to_return.append(com_upsert)

        # Upsert images
        upsert_images_folder_path = kwargs.get("upsert_images_folder_path")
        if upsert_images_folder_path:
            com_upsert = ScriptComponent(
                component_name="upsert images",
                body="npm run pi_upsert_images",
                args=[
                    ComponentArg(
                        arg_name="imagesFolderPath",
                        arg_val=upsert_images_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="recordsFolderPath",
                        arg_val=records_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
                ],
            )
            to_return.append(com_upsert)

        return to_return


if __name__ == "__main__":
    UpsyncPipeline().run_from_cli()
