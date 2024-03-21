from urllib.parse import urlparse
import re


def clean_url(in_url: str) -> str:
    # Lowercase url
    in_url = in_url.lower()
    in_url = in_url.strip()

    # Remove query params
    question_mark_index = in_url.find("?")
    if question_mark_index > -1:
        in_url = in_url[:question_mark_index]

    # Remove beginning https
    in_url = in_url.removeprefix("https://")
    in_url = in_url.removeprefix("http://")

    # Remove www
    in_url = in_url.removeprefix("www.")

    # Remove product hunt
    # TODO: Make this part extendable
    ph_strings = [
        "/producthunt",
        "/product-hunt",
        "/product_hunt",
        "/ph/",
        "/ph ",
        "/ph-",
        "/phunt",
    ]
    in_url = in_url + " "  # Do this for /ph to hit, but not /photo...
    for ph_string in ph_strings:
        in_url = in_url.replace(ph_string, "")

    # Remove consecutive slashes
    in_url = re.sub("/+", "/", in_url)

    # Remove trailing slash
    in_url = in_url.strip()
    if in_url[-1] == "/":
        in_url = in_url[:-1]

    return in_url


def get_domain_from_url(in_url: str) -> str:
    in_url = clean_url(in_url)
    in_url = "https://" + in_url
    domain = urlparse(in_url).netloc

    return domain


if __name__ == "__main__":
    test_url = "https://www.app.lol.fakeface.io/#index?ref=taaft&utm_source=taaft&utm_medium=referral"
    print(get_domain_from_url(test_url))
