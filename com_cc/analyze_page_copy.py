from os import listdir
from os.path import join
from bs4 import BeautifulSoup
import re


def analyze_page_copy(in_file_path: str):
    with open(in_file_path, "r", encoding="utf-8") as in_file:
        page_copy: str = in_file.read()

    meta_end_splitter = "---"
    meta_end_idx = page_copy.find(meta_end_splitter)
    content_start_idx = meta_end_idx + len(meta_end_splitter)

    # TODO: Use meta info to discriminate page copy types
    meta_info = page_copy[:meta_end_idx].strip()
    content = page_copy[content_start_idx:].strip()

    if "generic" in meta_info:
        return analyze_generic(meta_info, content)


def analyze_generic(meta_info: str, content: str):
    soup = BeautifulSoup(content, features="lxml")

    to_return = {"page_gist": "", "link_list": []}

    # For page gist
    page_gist_list: list[str] = []
    first_h1 = soup.h1
    if first_h1 is not None:
        page_gist_list.append(first_h1.get_text())

        first_h1_siblings = list(first_h1.next_siblings)

        MAX_PARENT_SIBLING_SEARCH = 3
        if len(first_h1_siblings) == 0:
            for _ in range(MAX_PARENT_SIBLING_SEARCH):
                first_h1_siblings = list(first_h1.parent.next_siblings)
                if len(first_h1_siblings) == 0:
                    break

        MAX_SIB_ANALYZED = 3
        for i, sib in enumerate(first_h1_siblings):
            if i + 1 >= MAX_SIB_ANALYZED:
                break

            sib_text = sib.get_text()
            page_gist_list.append(sib_text)

        main_content_el = soup.main
        if not main_content_el:
            main_content_el = soup

        heading_tags = main_content_el.find_all(re.compile("^h[1-2]$"))
        for tag in heading_tags:
            tag_text = tag.get_text()
            page_gist_list.append(tag_text)

        page_gist_list = [text.strip() for text in page_gist_list]

        BANNED_GIST_SUBSTRING_LIST = [
            "LOADING",
            "FAQ",
            "FREQUENTLY ASKED",
            "QUICK LINKS",
            "LEARN MORE",
            "...",
            "CONTACT",
        ]

        new_page_gist_list = []
        for i, in_text in enumerate(page_gist_list):
            if not in_text in new_page_gist_list:
                should_add = True
                should_test = i > 3

                if should_test:
                    for substr in BANNED_GIST_SUBSTRING_LIST:
                        if substr in in_text.upper():
                            should_add = False

                if should_add:
                    new_page_gist_list.append(in_text)

        page_gist = " ".join(new_page_gist_list)
        to_return["page_gist"] = page_gist

    link_list = []
    for link in soup.find_all("a"):
        link_list.append(link.get("href"))

    link_list = list(set(link_list))

    to_return["link_list"] = link_list

    return to_return
