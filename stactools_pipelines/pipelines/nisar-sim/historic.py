import csv
import os

from typing import Dict, List

import boto3


def get_products_info(inventory_s3_path: str) -> List[Dict]:
    """Get the products info from the inventory file in s3"""

    inventory_s3_path = inventory_s3_path.split("/", 3)
    s3_client = boto3.client("s3")
    print(f"bucket : {inventory_s3_path[2]}")
    print(f"key : {inventory_s3_path[3]}")
    obj = s3_client.get_object(Bucket=inventory_s3_path[2], Key=inventory_s3_path[3])

    with open(obj, "r", encoding="utf-8") as csvfile:
        dict_reader = csv.DictReader(csvfile)
        return [row["location"] for row in dict_reader]


def handler(event, context):
    print("historic handler")
    QUEUE_URL = os.environ["QUEUE_URL"]
    INVENTORY_LOCATION = os.environ["INVENTORY_LOCATION"]
    inventory = get_products_info(INVENTORY_LOCATION)
    sqs_client = boto3.client("sqs")
    print("sending messages to queue")
    for item in inventory:
        print(f"sending content {item}")
        sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=item)
