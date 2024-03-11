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
            "source": "https://nisar.asf.earthdatacloud.nasa.gov/NISAR-SAMPLE-DATA/L0B/ALOS1_Rosamond_20081012/NISAR_L0_PR_RRSD_001_005_A_128S_20081012T060910_20081012T060926_P01101_F_J_001.h5"
        }
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
