from datetime import datetime, timezone
from custom_helpers_py.string_formatters import get_date_filename


def get_current_date_filename():
    current_datetime = datetime.now(timezone.utc)
    date_filename = get_date_filename(current_datetime)
    return date_filename


if __name__ == "__main__":
    print(get_current_date_filename())
