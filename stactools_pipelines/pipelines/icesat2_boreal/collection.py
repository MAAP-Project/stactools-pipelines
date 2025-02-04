import json
import os

import requests
from icesat2_boreal_stac.stac import Variable, create_collection
from pystac import set_stac_version

from stactools_pipelines.cognito.utils import get_token

set_stac_version("1.0.0")


def handler(event, context):
    """handler for collection upload"""
    ingestor_url = os.environ["INGESTOR_URL"]
    collections_endpoint = f"{ingestor_url.strip('/')}/collections"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}

    print(collections_endpoint)
    for variable in Variable:
        collection = create_collection(variable)
        response = requests.post(
            url=collections_endpoint,
            data=json.dumps(collection.to_dict()),
            headers=headers,
        )
        print(collection.id)
        try:
            response.raise_for_status()
        except Exception:
            print(response.text)
            raise
