def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def is_string(to_test):
    return isinstance(to_test, str)
