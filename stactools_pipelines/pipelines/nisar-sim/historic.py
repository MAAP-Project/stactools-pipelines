import csv
import os
from typing import Dict, List
import boto3

def get_products_info(inventory_s3_path: str) -> List[str]:
    """Get the products info from the inventory file in s3"""

    inventory_s3_path = inventory_s3_path.split("/", 3)
    s3_client = boto3.client("s3")
    print(f"bucket : {inventory_s3_path[2]}")
    print(f"key : {inventory_s3_path[3]}")
    obj = s3_client.get_object(Bucket=inventory_s3_path[2], Key=inventory_s3_path[3])

    csv_data = obj["Body"].read().decode("utf-8")
    dict_reader = csv.DictReader(csv_data.splitlines())
    return [row["location"] for row in dict_reader]

def handler(event, context) -> None:
    print("Historic Handler")
    queue_url = os.environ["QUEUE_URL"]
    file_list = os.environ["FILE_LIST"]
    inventory = get_products_info(file_list)
    sqs_client = boto3.client("sqs")
    print("sending inventory to queue")
    for item in inventory:
        print(f"sending content {item}")
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=item)
