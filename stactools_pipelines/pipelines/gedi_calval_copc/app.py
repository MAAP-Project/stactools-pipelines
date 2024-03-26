import json
import os

import requests
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from stactools.gedi_calval_copc.stac import create_item
from stactools.core import use_fsspec

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
        try:
            stac = create_item(
                source=key,
                destination="s3://nasa-maap-data-store/file-staging/nasa-map/GEDI_CalVal_Lidar_COPC",
                copc=True
            )
        except Exception as e:
            print(f"Failed to create STAC for {key}: {e}")
            continue
        stac.collection_id = "GEDI_CalVal_Lidar_COPC"

        response = requests.post(
            url=ingestions_endpoint,
            data=json.dumps(stac.to_dict()),
            headers=headers
        )
        try:
            response.raise_for_status()
        except Exception:
            print(response.text)
            raise
