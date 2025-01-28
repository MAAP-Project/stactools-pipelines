import json

import pytest

import stactools_pipelines.pipelines.conftest as conftest
from stactools_pipelines.pipelines.icesat2_boreal.app import handler, parse_s3_key

key = "s3://maap-ops-workspace/aliz237/dps_output/run_boreal_biomass_map/dev_v1.5/AGB_H30_2020/full_run/2025/01/21/11/34/44/297688/boreal_agb_2020_202501211737487322_0039261.tif"
variable, tile_id = parse_s3_key(key)


@pytest.fixture
def sns_message():
    yield key


@pytest.mark.parametrize("pipeline_id", ["panama-copc"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sns_message, sqs_event, get_token, create_item, requests):
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(
        cog_key=key,
        copy_to=f"s3://nasa-maap-data-store/file-staging/nasa-map/icesat2-boreal-v2.1/{variable}/{tile_id}",
    )
    requests.post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
