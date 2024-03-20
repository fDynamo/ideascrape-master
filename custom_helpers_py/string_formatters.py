from custom_helpers_py.url_formatters import clean_url
import re


# constants
CAMEL2SNAKE = re.compile(r"(?<!^)(?=[A-Z])")
CLEANR = re.compile("<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});")
CLEANASCII = re.compile("[^\x00-\x7f]")

ACCEPTABLE_PUNCTUATION = [
    ",",
    ".",
    "!",
    '"',
    "'",
    "/",
    " ",
    "-",
]  # Removes non alphanumeric text and new lines


def replace_non_alphanumeric(in_text: str, replace_with: str):
    return re.sub("[^0-9a-zA-Z]+", replace_with, in_text)


def format_count_percentage(count: int, max_count: int) -> str:
    pct = count * 100 / max_count
    pct = "{:10.2f}%".format(pct)
    return pct


def camel_to_snake_case(in_str: str) -> str:
    return re.sub(CAMEL2SNAKE, "_", in_str).lower()


"""
NOTE:
This code is lowkey shit, need to fix somehow
"""


def clean_text(
    in_text: str,
    remove_html=False,
    remove_non_alpha=False,
    invalid_return_none=True,
    remove_commas=False,
) -> str:
    if not in_text or not isinstance(in_text, str):
        if invalid_return_none:
            return None
        else:
            return ""
    new_text = in_text.replace("\n", " ")

    if remove_html:
        new_text = cleanhtml(new_text)
    if remove_non_alpha:
        new_text = "".join(
            list(
                [
                    val
                    for val in new_text
                    if val.isalpha() or val.isnumeric() or val in ACCEPTABLE_PUNCTUATION
                ]
            )
        )

        new_text = re.sub(CLEANASCII, " ", new_text)
    if remove_commas:
        new_text = new_text.replace(",", " ")

    new_text = re.sub(" +", " ", new_text)
    new_text = new_text.strip()
    return new_text


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, " ", raw_html)
    return cleantext


def convert_url_to_file_name(in_url: str):
    in_url = clean_url(in_url)
    in_url = clean_text(in_url, remove_html=True, remove_non_alpha=True)
    return in_url.replace(".", "_").replace(" ", "_").replace("/", "_")


if __name__ == "__main__":
    cleaned_text = clean_text(
        """
        D-ID’s Creative Reality™ studio app brings a new era of AI videos to your fingertips. The app enables users to create customized digital humans that look, sound, and speak 
        
        like real people, offering unparalleled <b> cr"e'ative freedom to creators and businesses. 😊 就立刻给你发短信      bản văn bài khóa bài văn nguyên văn nguyên bản văn bản bài đọc tài liệu " </b>
        """
    )
