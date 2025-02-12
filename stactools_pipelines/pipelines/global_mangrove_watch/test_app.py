import json

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.global_mangrove_watch.app import handler

key = json.dumps(
    {
        "cog_asset_href": "s3://path/to/gmw_N00E00_2020_v3.tif",
        "change_asset_href": "s3://path/to/gmw_N00E00_chng_f1996_t2020_v3.tif",
    }
)


@pytest.fixture
def sns_message():
    yield key


@pytest.mark.parametrize("pipeline_id", ["global_mangrove_watch"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sns_message, sqs_event, get_token, create_item, requests):
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(**json.loads(key))
    requests.post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
