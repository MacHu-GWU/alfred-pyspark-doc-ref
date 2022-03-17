# -*- coding: utf-8 -*-

import json
import requests
from pathlib_mate import Path
from bs4 import BeautifulSoup

fts_dataset_name = "pyspark_api_ref"
dir_here = Path(__file__).parent
dir_home = Path.home()
path_data_json = Path(dir_home, ".alfred-fts", f"{fts_dataset_name}.json")
path_setting_json = Path(dir_home, ".alfred-fts", f"{fts_dataset_name}-setting.json")

path_api_ref = Path(dir_here, "api-ref.html")
api_ref_url = "https://spark.apache.org/docs/latest/api/python/reference/index.html"
pyspark_doc_domain = "https://spark.apache.org"


def download_api_ref_html():
    if not path_api_ref.exists():
        path_api_ref.write_text(requests.get(api_ref_url).text)


def deploy_to_alfred_fts():
    soup = BeautifulSoup(path_api_ref.read_text(), "html.parser")
    nav = soup.find("nav", id="bd-docs-nav")
    data = list()
    for a in nav.find_all("a"):
        import_path = a.text.strip()
        href = a.attrs['href']
        url = f"{pyspark_doc_domain}/docs/latest/api/python/reference/{href}"
        data.append(dict(ipath=import_path, url=url))

    setting = {
        "columns": [
            {
                "name": "ipath",
                "ngram_maxsize": 10,
                "ngram_minsize": 2,
                "type_is_ngram": True
            },
            {
                "name": "url",
                "type_is_store": True
            },
        ],
        "title_field": "{ipath}",
        "subtitle_field": "open {url}",
        "arg_field": "{url}",
        "autocomplete_field": "{ipath}"
    }

    path_data_json.write_text(json.dumps(data, indent=4))
    path_setting_json.write_text(json.dumps(setting, indent=4))

# download_api_ref_html()
deploy_to_alfred_fts()
