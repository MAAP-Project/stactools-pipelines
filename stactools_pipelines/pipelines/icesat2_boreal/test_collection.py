import json
from unittest.mock import call

import pytest
from icesat2_boreal_stac.stac import Variable

from stactools_pipelines.pipelines import conftest
from stactools_pipelines.pipelines.icesat2_boreal.collection import handler


@pytest.mark.parametrize("pipeline_id", ["icesat2_boreal"])
@pytest.mark.parametrize("module", ["collection"])
def test_handler(mock_env, get_token, create_collection, requests):
    handler({}, {})
    get_token.assert_called_once()

    # Prepare expected calls for create_collection
    expected_create_collection_calls = [
        call(variable=variable) for variable in Variable
    ]
    create_collection.assert_has_calls(expected_create_collection_calls, any_order=True)

    # Prepare expected calls for requests.post
    expected_requests_calls = [
        call(
            url=f"{conftest.ingestor_url}/collections",
            data=json.dumps(conftest.collection),
            headers={"Authorization": f"bearer {conftest.token}"},
        )
    ] * len(Variable)

    # Check if the post requests were called correctly
    requests.post.assert_has_calls(expected_requests_calls, any_order=True)
