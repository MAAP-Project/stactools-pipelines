import os

import boto3
from moto import mock_s3, mock_sqs

from stactools_pipelines.pipelines.nisar_sim.historic import get_products_info, handler


def mock_inventory() -> str:

    """returns a fake inventory s3 path"""

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket")
    s3 = boto3.client("s3")
    s3.upload_file(
        "stactools_pipelines/pipelines/nisar_sim/test_inventory.json",
        "mybucket",
        "test_inventory.json",
    )

    return "s3://mybucket/test_inventory.json"


def mock_queue() -> str:

    """returns a fake queue URL"""
    sqs = boto3.client("sqs")
    # create fake queue
    queue = sqs.create_queue(QueueName="test_queue")
    return queue["QueueUrl"]


@mock_s3
def test_get_products_info():

    s3_path = mock_inventory()
    expected = [
        {
            "product_id": "Haywrd_14501_08037_007_080729_L090_CX_01",
            "release": "Release2e",
        },
        {
            "product_id": "SDelta_23518_11079_006_111207_L090_CX_02",
            "release": "Release2b",
        },
    ]
    actual = get_products_info(s3_path)

    assert expected == actual


@mock_s3
@mock_sqs
def test_handler():

    s3_path = mock_inventory()
    queue_url = mock_queue()

    os.environ["QUEUE_URL"] = queue_url
    os.environ["INVENTORY_LOCATION"] = s3_path

    handler({}, {})
