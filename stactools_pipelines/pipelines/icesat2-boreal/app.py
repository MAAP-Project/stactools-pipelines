import os

import httpx
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from icesat2_boreal_stac.stac import create_item

from stactools_pipelines.cognito.utils import get_token


@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context):
    ingestor_url = os.environ["INGESTOR_URL"]
    ingestions_endpoint = f"{ingestor_url.strip('/')}/ingestions"
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    use_fsspec()
    for record in event.records:
        key = record["body"]
        _, _, bucket, input_key = key.split("/", 3)
        try:
            item = create_item(
                key,
                copy_to="s3://nasa-maap-data-store/file-staging/nasa-map/icesat2-boreal-v2.1",
            )
        except Exception as e:
            print(f"Failed to create item for {key}: {e}")
            continue

        response = httpx.post(
            url=ingestions_endpoint,
            data=item.to_dict(),
            headers=headers,
        )
        try:
            response.raise_for_status()
        except Exception:
            print(response.text)
            raise
