from datetime import datetime, timezone
from custom_helpers_py.string_formatters import replace_non_alphanumeric


def get_current_date_filename():
    current_datetime = datetime.now(timezone.utc)
    iso_string = current_datetime.isoformat()
    return replace_non_alphanumeric(iso_string, "_")


def get_date_diff_string(end_date: datetime, start_date: datetime, format=None) -> str:
    diff_in_seconds = (end_date - start_date).total_seconds()
    if not format:
        if diff_in_seconds < 60:
            format = "s"
        else:
            format = "m"

    if format == "s":
        return "{:10.3f}".format(diff_in_seconds) + " seconds"
    elif format == "m":
        minutes = diff_in_seconds / 60
        return "{:10.3f}".format(minutes) + " minutes"


if __name__ == "__main__":
    print(get_current_date_filename())
