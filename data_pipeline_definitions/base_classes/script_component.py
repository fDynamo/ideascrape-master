class ComponentArg:
    def __init__(
        self, arg_name: str, arg_val: str | bool | None = True, is_path=None
    ) -> None:
        self.arg_name = arg_name
        self.arg_val = arg_val

        if is_path == None:
            new_is_path = False
            # TODO: Add more conditions
            if arg_val and isinstance(arg_val, str):
                if arg_val.startswith("C:"):
                    new_is_path = True

            self.is_path = new_is_path
        else:
            self.is_path = is_path

    def __str__(self) -> str:
        to_return = ""

        if len(self.arg_name) == 1:
            to_return += "-"
        else:
            to_return += "--"
        to_return += self.arg_name

        if isinstance(self.arg_val, bool):
            if not self.arg_val:
                return ""
            else:
                return to_return

        arg_val = self.arg_val
        if isinstance(arg_val, str):
            arg_val = '"{}"'.format(arg_val)
        else:
            arg_val = str(arg_val)

        to_return += " " + arg_val

        return to_return

    def get_debug_str(self) -> str:
        return "\n".join([self.arg_name, self.arg_val, self.is_path])

    @staticmethod
    def convert_list_to_arg(in_list: list):
        if len(in_list) == 1:
            return ComponentArg(arg_name=in_list[0])
        elif len(in_list) == 2:
            return ComponentArg(arg_name=in_list[0], arg_val=in_list[1])
        else:
            return None


class ScriptComponent:
    def __init__(
        self, component_name: str, body: str, args: list[ComponentArg]
    ) -> None:
        self.component_name = component_name
        self.body = body.strip()

        args_to_add = []
        for arg in args:
            to_add = None
            if isinstance(arg, list):
                to_add = ComponentArg.convert_list_to_arg(arg)
            elif isinstance(arg, ComponentArg):
                to_add = arg

            if to_add:
                args_to_add.append(to_add)

        self.args: list[ComponentArg] = args_to_add

    def __str__(self) -> str:
        to_return = self.body
        if to_return.startswith("npm run") and len(self.args) > 0:
            to_return += " --"

        if len(self.args):
            arg_str = " ".join([str(arg) for arg in self.args])
            to_return += " " + arg_str

        return to_return

    def add_arg(self, new_arg: str | ComponentArg | list):
        if isinstance(new_arg, str):
            new_arg = ComponentArg(arg_name=new_arg)

        if isinstance(new_arg, list):
            new_arg = ComponentArg.convert_list_to_arg(new_arg)

        if new_arg:
            curr_args = self.args
            curr_args.append(new_arg)
            self.args = curr_args

    def get_debug_str(self) -> str:
        return "\n".join([self.component_name, str(self)])

    def get_paths_in_args(self) -> list[str]:
        return [arg.arg_val for arg in self.args if arg.is_path]
