"""Prepare historic inventory for processing.

The inventory csv was generated like this:

import os

import pandas as pd

ht_df = pd.read_csv("s3://maap-ops-workspace/shared/montesano/DPS_tile_lists/BOREAL_MAP/dev_v1.5/Ht_H30_2020/full_run/HT_tindex_master.csv")
agb_df = pd.read_csv("s3://maap-ops-workspace/shared/montesano/DPS_tile_lists/BOREAL_MAP/dev_v1.5/AGB_H30_2020/full_run/AGB_tindex_master.csv")

inventory_df = (
    pd.concat(
        [agb_df, ht_df]
    )
    .reset_index()[["s3_path"]]
)

assert not inventory_df.isnull().values.any()

inventory_key = "s3://maap-ops-workspace/shared/henrydevseed/icesat2-boreal-v2.1-inventory.csv"
inventory_df.to_csv(inventory_key, index=False, header=False)
"""

import os

import boto3


def inventory_data(inventory: str) -> list:
    """read the inventory file and create a list of keys"""
    _, _, bucket, inventory_key = inventory.split("/", 3)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=inventory_key)
    return response["Body"].read().decode("utf-8").splitlines()


def handler(event, context):
    """handler for firing messages for historic inventory to the queue"""
    QUEUE_URL = os.environ["QUEUE_URL"]
    INVENTORY_LOCATION = os.environ["INVENTORY_LOCATION"]
    keys = inventory_data(INVENTORY_LOCATION)
    sqs_client = boto3.client("sqs")
    for key in keys:
        sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=key)
