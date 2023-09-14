import json

import pytest

from stactools_pipelines.pipelines.nisar_sim import conftest
from stactools_pipelines.pipelines.nisar_sim.app import (
    DITHER,
    NMODE,
    PRODUCT_HREF_PATTERN,
    AppEvent,
    handler,
)


@pytest.mark.parametrize("pipeline_id", ["nisar_sim"])
@pytest.mark.parametrize("module", ["app"])
def test_handler(mock_env, sqs_event, get_token, create_item, post):
    # the handler needs a json format for the body
    body = sqs_event["Records"][0]["body"].copy()
    sqs_event["Records"][0]["body"] = json.dumps(body)
    app_event = AppEvent(**body)
    handler(sqs_event, {})
    get_token.assert_called_once()
    create_item.assert_called_once_with(
        product_href=PRODUCT_HREF_PATTERN.format(
            product_id=app_event.product_id, release=app_event.release
        ),
        nmode=NMODE,
        dither=DITHER,
    )
    post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/ingestions",
        data=json.dumps(conftest.item),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
