import json

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.panama_copc.app import handler

key = "https://smithsonian.dataone.org/datasets/ALS_Panama_2023/01_Agua_Salud_Alfagia/02_LAZ_Unclassified/230526_195401.laz"


@pytest.fixture
def sns_message():
    yield key


@pytest.mark.parametrize("pipeline_id", ["panama-copc"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sns_message, sqs_event, get_token, create_item, requests):
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(
        source=key,
        destination="s3://nasa-maap-data-store/file-staging/nasa-map/ALS_Panama_COPC_Unclassified",
        copc=True
    )
    requests.post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
