from unittest.mock import patch

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.panama_copc.historic import handler

inventory_location = "inventory_location"
key = "s3://maap-ops-workspace/aliz237/dps_output/run_boreal_biomass_map/dev_v1.5/AGB_H30_2020/full_run/2025/01/21/11/34/44/297688/boreal_agb_2020_202501211737487322_0039261.tif"


@pytest.fixture
def mock_inventory_env(monkeypatch):
    monkeypatch.setenv("INVENTORY_LOCATION", inventory_location)


@pytest.fixture()
def inventory_data():
    with patch(
        "stactools_pipelines.pipelines.panama_copc.historic.inventory_data",
        autospec=True,
    ) as m:
        m.return_value = [key]
        yield m


@pytest.mark.parametrize("pipeline_id", ["panama-copc"])
@pytest.mark.parametrize("query_value", [""])
def test_handler(mock_env, mock_inventory_env, inventory_data, boto3):
    handler({}, {})
    inventory_data.assert_called_once_with(inventory_location)

    conftest.sqs_client.send_message.assert_called_once_with(
        QueueUrl=conftest.queue_url,
        MessageBody=key,
    )
