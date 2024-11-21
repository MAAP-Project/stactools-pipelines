import json

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.gedi_calval_copc.app import handler

key = "s3://nasa-maap-data-store/file-staging/nasa-map/GEDI_CalVal_Lidar_Data___2/gabon_mondah_110116_122945_geoid.las"


@pytest.fixture
def sns_message():
    yield key


@pytest.mark.parametrize("pipeline_id", ["gedi_calval_copc"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sns_message, sqs_event, get_token, create_item, requests):
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(
        source=key,
        destination="s3://nasa-maap-data-store/file-staging/nasa-map/GEDI_CalVal_Lidar_COPC/",
        copc=True
    )
    requests.post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
