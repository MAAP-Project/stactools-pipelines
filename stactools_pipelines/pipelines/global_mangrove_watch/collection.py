import json
import os

import requests
from pystac import Provider, ProviderRole, set_stac_version
from stactools.global_mangrove_watch.stac import create_collection

from stactools_pipelines.cognito.utils import get_token

set_stac_version("1.0.0")


def handler(event, context):
    """handler for collection upload"""
    ingestor_url = os.environ["INGESTOR_URL"]
    collections_endpoint = f"{ingestor_url.strip('/')}/collections"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}

    collection = create_collection()
    collection.providers.append(
        Provider(
            name="NASA MAAP",
            url="https://maap-project.org/",
            roles=[
                ProviderRole.HOST,
            ],
        )
    )
    response = requests.post(
        url=collections_endpoint,
        data=json.dumps(collection.to_dict()),
        headers=headers,
    )

    try:
        response.raise_for_status()
    except Exception:
        print(response.text)
        raise
