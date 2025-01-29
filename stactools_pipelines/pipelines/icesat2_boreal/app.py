import json
import os
import re

import requests
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from icesat2_boreal_stac.stac import create_item
from pystac import set_stac_version

from stactools_pipelines.cognito.utils import get_token

set_stac_version("1.0.0")


def parse_s3_key(key: str):
    # Pattern matches: boreal_(agb|ht)_2020_\d+_(\d{7})\.tif$
    # \d{7} matches the 7 digit tile ID
    pattern = r"boreal_(agb|ht)_2020_\d+_(\d{7})\.tif$"

    match = re.search(pattern, key)
    if match:
        variable = match.group(1)  # First capture group (agb|ht)
        tile_id = match.group(2)  # Second capture group (7 digits)
        return variable, tile_id

    return None, None


@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context):
    """handler for generating items"""
    ingestor_url = os.environ["INGESTOR_URL"]
    ingestions_endpoint = f"{ingestor_url.strip('/')}/ingestions"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    for record in event.records:
        key = record["body"]
        try:
            variable, tile_id = parse_s3_key(key)

            if not variable and tile_id:
                raise Exception(f"ht/agb and tile_id could not be parsed from {key}")

            item = create_item(
                key,
                copy_to=f"s3://nasa-maap-data-store/file-staging/nasa-map/icesat2-boreal-v2.1/{variable}/{tile_id}",
            )
        except Exception as e:
            print(f"Failed to create item for {key}: {e}")
            continue

        response = requests.post(
            url=ingestions_endpoint,
            data=json.dumps(item.to_dict()),
            headers=headers,
        )
        try:
            response.raise_for_status()
        except Exception:
            print(response.text)
            raise
