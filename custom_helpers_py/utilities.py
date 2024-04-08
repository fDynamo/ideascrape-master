import os


def is_windows() -> bool:
    return os.name == "nt"


def number_str_to_number(in_str: str, to_float=False):
    in_str = in_str.removesuffix("+")
    in_str = in_str.lower()
    if in_str.endswith("k"):
        in_str = in_str.removesuffix("k")
        if to_float:
            num = float(in_str)
        else:
            num = int(in_str)
        num = num * 1000
    elif in_str.endswith("m"):
        in_str = in_str.removesuffix("m")
        if to_float:
            num = float(in_str)
        else:
            num = int(in_str)
        num = num * 1000_000
    elif in_str.endswith("b"):
        in_str = in_str.removesuffix("b")
        if to_float:
            num = float(in_str)
        else:
            num = int(in_str)
        num = num * 1000_000_000
    else:
        if to_float:
            num = float(in_str)
        else:
            num = int(in_str)

    return num
