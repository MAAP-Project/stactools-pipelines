import os

import boto3


def inventory_data(inventory: str) -> list:
    inventory = inventory.split("/", 3)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=inventory[2], Key=inventory[3])
    return response["Body"].read().decode("utf-8").splitlines()


def handler(event, context):
    QUEUE_URL = os.environ["QUEUE_URL"]
    INVENTORY_LOCATION = os.environ["INVENTORY_LOCATION"]
    keys = inventory_data(INVENTORY_LOCATION)
    sqs_client = boto3.client("sqs")
    for key in keys:
        sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=key)
