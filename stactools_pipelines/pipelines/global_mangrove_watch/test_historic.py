import json
from unittest.mock import patch

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.global_mangrove_watch.historic import (
    handler,
)

keys = {
    "gmw_N00E00_2020_v3": {
        "cog_asset_href": "s3://path/to/gmw_N00E00_2020_v3.tif",
        "change_asset_href": "s3://path/to/gmw_N00E00_chng_f1996_t2020_v3.tif",
    }
}


@pytest.fixture()
def generate_inventory():
    with patch(
        "stactools_pipelines.pipelines.global_mangrove_watch.historic.generate_inventory",
        autospec=True,
    ) as m:
        m.return_value = keys
        yield m


@pytest.mark.parametrize("pipeline_id", ["global_mangrove_watch"])
@pytest.mark.parametrize("query_value", [""])
def test_handler(mock_env, generate_inventory, boto3):
    handler({}, {})
    generate_inventory.assert_called_once()

    conftest.sqs_client.send_message.assert_called_once_with(
        QueueUrl=conftest.queue_url,
        MessageBody=json.dumps(list(keys.values())[0]),
    )
