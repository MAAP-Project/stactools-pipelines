import json

import pytest

from stactools_pipelines.pipelines.nisar_sim import conftest
from stactools_pipelines.pipelines.nisar_sim.app import (
    handler
)

key = (
    "https://nisar.asf.earthdatacloud.nasa.gov/NISAR-SAMPLE-DATA/"
    "L0B/ALOS1_Rosamond_20081012/"
    "NISAR_L0_PR_RRSD_001_005_A_128S_20081012T060910_20081012T060926_P01101_F_J_001.h5"
)


@pytest.mark.parametrize("pipeline_id", ["nisar_sim"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sqs_event, get_token, create_item, post):
    # the handler needs a json format for the body
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(
        source=key
    )
    post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
