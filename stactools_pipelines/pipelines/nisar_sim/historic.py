import json
import os
from typing import Dict, List

import boto3

FILE_URL_PREFIX = "https://downloaduav.jpl.nasa.gov/"
# expecting download URLs like https://downloaduav.jpl.nasa.gov/Release2c/SDelta_23518_15002_005_150107_L090_CX_02/SDelta_23518_15002_005_150107_L090HHVV_CX_138B_02.grd


def get_products_info(inventory_s3_path: str) -> List[Dict]:

    """obtain a list of dicts with two entries, one with the product id and one with the release."""

    print("getting product ids")
    inventory_s3_path = inventory_s3_path.split("/", 3)
    s3_client = boto3.client("s3")
    print(f"bucket : {inventory_s3_path[2]}")
    print(f"key : {inventory_s3_path[3]}")
    obj = s3_client.get_object(Bucket=inventory_s3_path[2], Key=inventory_s3_path[3])
    inventory = json.loads(obj["Body"].read().decode("utf-8"))

    def get_one_product_info(product) -> Dict:
        return {
            "product_id": product["product"],
            "release": product["files"][0].replace(FILE_URL_PREFIX, "")[:9],
        }

    return [get_one_product_info(x) for x in inventory["products"]]


def handler(event, context):
    print("historic handler")
    QUEUE_URL = os.environ["QUEUE_URL"]
    INVENTORY_LOCATION = os.environ["INVENTORY_LOCATION"]
    to_queue = get_products_info(INVENTORY_LOCATION)
    sqs_client = boto3.client("sqs")
    print("sending messages to queue")
    for item in to_queue:
        print(f"sending content {item}")
        sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(item))
