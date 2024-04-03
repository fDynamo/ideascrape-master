from pipeline_definitions.base_classes.data_pipeline import DataPipeline
from pipeline_definitions.base_classes.script_component import (
    ScriptComponent,
    ComponentArg,
)
from os.path import join


class UpsyncPipeline(DataPipeline):
    def get_pipeline_name(self) -> str:
        return "upsync"

    def add_cli_args(self, parser):
        parser.add_argument("--upsync-folder-path", type=str, dest="upsync_folder_path")
        parser.add_argument(
            "--upsert-images-folder-path",
            type=str,
            dest="upsert_images_folder_path",
        )
        super().add_cli_args(parser)

    def get_steps(self, **kwargs) -> list[ScriptComponent]:
        out_folder_path = self.pipeline_run_folder_path
        records_folder_path = join(out_folder_path, "upsync_records")

        # Upsert tables
        to_return = []

        upsync_folder_path = kwargs.get("upsync_folder_path")
        if upsync_folder_path:
            com_upsync = ScriptComponent(
                component_name="upsync data",
                body="bun run pi_upsync_records",
                args=[
                    ComponentArg(
                        arg_name="i",
                        arg_val=upsync_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="r",
                        arg_val=records_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
                ],
            )
            to_return.append(com_upsync)

        # Upsert images
        upsert_images_folder_path = kwargs.get("upsert_images_folder_path")
        if upsert_images_folder_path:
            com_upsert = ScriptComponent(
                component_name="upsert images",
                body="bun run pi_upsert_images",
                args=[
                    ComponentArg(
                        arg_name="i",
                        arg_val=upsert_images_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(
                        arg_name="r",
                        arg_val=records_folder_path,
                        is_path=True,
                    ),
                    ComponentArg(arg_name="prod", arg_val=kwargs.get("prod", False)),
                ],
            )
            to_return.append(com_upsert)

        if len(to_return) == 0:
            raise Exception("Nothing to upsync!")

        return to_return


if __name__ == "__main__":
    UpsyncPipeline().run_from_cli()
