from json import load


def load_json_as_obj(in_file_path):
    file_obj = open(in_file_path)
    to_return = load(file_obj)
    file_obj.close()
    return to_return
