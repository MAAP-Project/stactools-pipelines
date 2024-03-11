import json
import os

from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from requests import post as requests_post
from stactools.core import use_fsspec
from stactools.nisar_sim.stac import create_item

from stactools_pipelines.cognito.utils import get_token


@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context):
    print(f"processing event {event}")
    ingestor_url = os.environ["INGESTOR_URL"]
    ingestions_endpoint = f"{ingestor_url.strip('/')}/ingestions"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    use_fsspec()
    for record in event.records:
        key = record["body"]
        stac = create_item(
            source=key,
        )
        stac.collection_id = "nisar-sim"
        print(f"ingesting {stac.to_dict()}")
        response = requests_post(
            url=ingestions_endpoint, data=json.dumps(stac.to_dict()), headers=headers
        )
        try:
            response.raise_for_status()
        except Exception:
            print(response.text)
            raise
