import json
import os
from typing import Optional

from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from pydantic import BaseModel
from requests import post as requests_post
from stactools.core import use_fsspec
from stactools.nisar_sim.stac import create_item

from stactools_pipelines.cognito.utils import get_token

DITHER = "X"
NMODE = "129"
PRODUCT_HREF_PATTERN = "https://downloaduav.jpl.nasa.gov/{release}/{product_id}/"


class AppEvent(BaseModel):
    product_id: str
    release: str
    nmode: Optional[str] = NMODE
    dither: Optional[str] = DITHER


@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context):
    print(f"processing event {event}")
    ingestor_url = os.environ["INGESTOR_URL"]
    ingestions_endpoint = f"{ingestor_url.strip('/')}/ingestions"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    use_fsspec()
    for record in event.records:
        print(f"processing record body {record['body']}")
        app_event = AppEvent(**json.loads(record["body"]))
        href = PRODUCT_HREF_PATTERN.format(
            product_id=app_event.product_id, release=app_event.release
        )
        stac = create_item(
            product_href=href, nmode=app_event.nmode, dither=app_event.dither
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
