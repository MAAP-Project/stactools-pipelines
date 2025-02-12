import json
import os

import requests
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from pystac import set_stac_version
from stactools.global_mangrove_watch.stac import create_item

from stactools_pipelines.cognito.utils import get_token

set_stac_version("1.0.0")


@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context):
    """handler for generating items"""
    ingestor_url = os.environ["INGESTOR_URL"]
    ingestions_endpoint = f"{ingestor_url.strip('/')}/ingestions"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    for record in event.records:
        keys = json.loads(record["body"])
        try:
            item = create_item(**keys)
        except Exception as e:
            print(f"Failed to create item for {keys}: {e}")
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
