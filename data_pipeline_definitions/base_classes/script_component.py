class ScriptComponent:
    def __init__(self, component_name: str, body: str, args: list[list[str]]) -> None:
        self.component_name = component_name
        self.body = body.strip()
        self.args = args

    def __str__(self) -> str:
        to_return = self.body
        if to_return.startswith("npm run") and len(self.args) > 0:
            to_return += " --"

        for arg in self.args:
            arg_name = arg[0]
            if not arg_name:
                continue
            arg_name = arg_name.strip()
            if len(arg_name) == 1:
                arg_name = "-" + arg_name
            else:
                arg_name = "--" + arg_name

            if len(arg) > 1:
                arg_val = arg[1]

                # If bool, add only arg name if true, skip entirely if false
                if isinstance(arg_val, bool):
                    if arg_val:
                        to_return += " " + arg_name
                        continue
                    else:
                        continue

                # If string, wrap with double quotes
                if isinstance(arg_val, str):
                    arg_val = '"{}"'.format(arg_val)

                # Add arg_name and arg_val
                to_add = " {} {}".format(arg_name, arg_val)
                to_return += to_add
            else:
                to_return += " " + arg_name

        return to_return

    def add_arg(self, new_arg: str | list[str]):
        curr_args = self.args
        if isinstance(new_arg, str):
            new_arg = [
                new_arg,
            ]
        curr_args.append(new_arg)
        self.args = curr_args

    def erase_component(self):
        self.args: list[list[str]] = []
        self.body = ""
