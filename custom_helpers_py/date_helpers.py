from datetime import datetime, timezone
from custom_helpers_py.string_formatters import replace_non_alphanumeric


def get_current_date_filename():
    current_datetime = datetime.now(timezone.utc)
    iso_string = current_datetime.isoformat()
    return replace_non_alphanumeric(iso_string, "_")


if __name__ == "__main__":
    print(get_current_date_filename())
