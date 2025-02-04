from unittest.mock import patch

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.icesat2_boreal.historic import (
    handler,
)

inventory_location = "inventory_location"
key = "/.las"


@pytest.fixture
def mock_inventory_env(monkeypatch):
    monkeypatch.setenv("INVENTORY_LOCATION", inventory_location)


@pytest.fixture()
def inventory_data():
    with patch(
        "stactools_pipelines.pipelines.icesat2_boreal.historic.inventory_data",
        autospec=True,
    ) as m:
        m.return_value = [key]
        yield m


@pytest.mark.parametrize("pipeline_id", ["icesat2_boreal"])
@pytest.mark.parametrize("query_value", [""])
def test_handler(mock_env, mock_inventory_env, inventory_data, boto3):
    handler({}, {})
    inventory_data.assert_called_once_with(inventory_location)

    conftest.sqs_client.send_message.assert_called_once_with(
        QueueUrl=conftest.queue_url,
        MessageBody=key,
    )
