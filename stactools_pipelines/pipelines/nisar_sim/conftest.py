from unittest.mock import MagicMock, patch
import pytest

domain = "domain"
client_secret = "client_secret"
client_id = "client_id"
scope = "scope"
ingestor_url = "https://ingestor_url"
token = "token"
item = {"id": "id"}
collection = {"id": "id"}
output_location = "s3://output_location"
queue_url = "queue_url"
query_id = "id"


sqs_client = MagicMock()


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("DOMAIN", domain)
    monkeypatch.setenv("CLIENT_SECRET", client_secret)
    monkeypatch.setenv("CLIENT_ID", client_id)
    monkeypatch.setenv("SCOPE", scope)
    monkeypatch.setenv("INGESTOR_URL", ingestor_url)
    monkeypatch.setenv("OUTPUT_LOCATION", output_location)
    monkeypatch.setenv("QUEUE_URL", queue_url)


@pytest.fixture()
def sqs_event():
    SQS_MESSAGE = {
            "Records": [
                {
                    "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                    "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                    "body": {
                        "product_id":"Haywrd_14501_08037_007_080729_L090_CX_01",
                        "release": "Release2e"
                    },
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1545082649183",
                        "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                        "ApproximateFirstReceiveTimestamp": "1545082649185",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue",
                    "awsRegion": "us-east-1",
                },
            ]
        }
    yield SQS_MESSAGE

@pytest.fixture()
def get_token(pipeline_id, module):
    with patch(
        f"stactools_pipelines.pipelines.{pipeline_id}.{module}.get_token",
        return_value=token,
        autospec=True,
    ) as m:
        yield m


@pytest.fixture()
def create_item(pipeline_id):
    with patch(
        f"stactools_pipelines.pipelines.{pipeline_id}.app.create_item",
        autospec=True,
    ) as m:
        m.return_value.to_dict.return_value = item
        yield m


@pytest.fixture()
def create_collection(pipeline_id):
    with patch(
        f"stactools_pipelines.pipelines.{pipeline_id}.collection.create_collection",
        autospec=True,
    ) as m:
        m.return_value.to_dict.return_value = collection
        yield m


@pytest.fixture()
def post(pipeline_id, module):
    with patch(
        f"stactools_pipelines.pipelines.{pipeline_id}.{module}.requests_post",
        autospec=True,
    ) as m:
        yield m
