from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import re

"""
Clean url takes in an arbitrary url string and makes sure that it is the most 
"""


def clean_url(in_url: str) -> str:
    # Lowercase url
    in_url = in_url.lower().strip()

    # Remove query params
    GOOGLE_PLAY_STORE_SUBSTRING = "play.google.com/store/apps/details"
    if GOOGLE_PLAY_STORE_SUBSTRING in in_url:
        in_url = remove_query_params(in_url, params_to_keep=["id"])
    else:
        in_url = remove_query_params(in_url, remove_all=True)

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


def remove_query_params(
    url: str,
    params_to_remove: list[str] = [],
    params_to_keep: list[str] = [],
    remove_all=False,
):
    # Parse the URL
    parsed_url = urlparse(url)

    # Parse the query parameters
    query_params = parse_qs(parsed_url.query)

    # Remove specified query parameters
    if remove_all:
        query_params = {}

    elif len(params_to_keep):
        new_params = {}
        for param in params_to_keep:
            tmp = query_params.get(param)
            if tmp:
                new_params[param] = tmp
        query_params = new_params

    elif len(params_to_remove):
        for param in params_to_remove:
            if param in query_params:
                del query_params[param]

    # Reconstruct the query string
    updated_query = urlencode(query_params, doseq=True)

    # Reconstruct the URL
    updated_url = urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            updated_query,
            parsed_url.fragment,
        )
    )

    return updated_url


if __name__ == "__main__":
    test_url = "play.google.com/store/apps/details?id=com.footballscore.mobile&ref=taaft&utm_source=taaft&utm_medium=referral"
    print(
        remove_query_params(
            test_url, ["ref", "utm_source", "utm_medium", "lol"], remove_all=True
        )
    )
