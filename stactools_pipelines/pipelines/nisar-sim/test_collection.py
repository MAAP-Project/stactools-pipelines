import json

import pytest

from stactools_pipelines.pipelines import conftest
from stactools_pipelines.pipelines.nisar_sim.collection import handler


@pytest.mark.parametrize("pipeline_id", ["nisar_sim"])
@pytest.mark.parametrize("module", ["collection"])
def test_handler(mock_env, get_token, create_collection, post):
    handler({}, {})
    get_token.assert_called_once()
    create_collection.assert_called_once()
    post.assert_called_once_with(
        url=f"{conftest.ingestor_url}/collections",
        data=json.dumps(conftest.collection),
        headers={"Authorization": f"bearer {conftest.token}"},
    )
