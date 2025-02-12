"""Prepare historic inventory for processing.

Download the zip files from Zenodo and upload COGs to S3, then pass sets of
asset hrefs to the item processing step.

HR: I had to run the generate_inventory function outside of the pipeline because
it would time out on the Lambda otherwise :/
"""

import json
import os
import re
import tempfile
import urllib.request
import zipfile
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

ZIP_URL_FORMAT = "https://zenodo.org/records/6894273/files/gmw_v3_{group}_gtiff.zip"

BASE_YEAR = 1996
YEARS = [
    2007,
    2008,
    2009,
    2010,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
]
ALL_GROUPS = [str(year) for year in [BASE_YEAR] + YEARS] + [
    f"f{BASE_YEAR}_t{year}" for year in YEARS
]

DESTINATION_BUCKET = "nasa-maap-data-store"
DESTINATION_PREFIX = "file-staging/nasa-map/global-mangrove-watch/v3"


def parse_filename(filename: str) -> tuple[str, str]:
    """
    Parse GMW filename and return (item_id, arg_name)
    """
    # Pattern for change files: GMW_N27W110_chng_f1996_t2020_v3.tif
    change_pattern = r"(GMW_[NS]\d+[WE]\d+)_chng_f\d+_t(\d+)_v3\.tif"

    # Pattern for base files: GMW_N27W110_2020_v3.tif
    base_pattern = r"(GMW_[NS]\d+[WE]\d+)_(\d+)_v3\.tif"

    # Try change pattern first
    change_match = re.match(change_pattern, filename)
    if change_match:
        location, year = change_match.groups()
        base_key = f"{location}_{year}_v3"
        return base_key, "change_asset_href"

    # Try base pattern
    base_match = re.match(base_pattern, filename)
    if base_match:
        location, year = base_match.groups()
        base_key = f"{location}_{year}_v3"
        return base_key, "cog_asset_href"

    raise ValueError(f"Unrecognized filename pattern: {filename}")


def generate_inventory() -> defaultdict[str, dict[str, str]]:
    """Download zips, upload contents to S3, and return inventory of uploaded files
    grouped by item ID (cog_asset_href and change_asset_href)."""
    s3_client = boto3.client("s3")
    inventory = defaultdict(dict)

    for group in ALL_GROUPS:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_url = ZIP_URL_FORMAT.format(group=group)
            zip_path = os.path.join(temp_dir, f"gmw_v3_{group}_gtiff.zip")

            print(f"Downloading {zip_url}")
            urllib.request.urlretrieve(zip_url, zip_path)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            for root, _, files in os.walk(temp_dir):
                for filename in sorted(files):
                    if filename.endswith(".tif"):
                        # Parse filename
                        try:
                            base_key, arg = parse_filename(filename)
                        except ValueError as e:
                            print(f"Warning: {e}")
                            continue

                        local_path = os.path.join(root, filename)
                        s3_key = f"{DESTINATION_PREFIX}/{filename}"
                        href = f"s3://{DESTINATION_BUCKET}/{s3_key}"

                        # Check if file already exists in S3
                        try:
                            s3_client.head_object(Bucket=DESTINATION_BUCKET, Key=s3_key)
                        except ClientError as e:
                            if e.response["Error"]["Code"] == "404":
                                s3_client.upload_file(
                                    local_path, DESTINATION_BUCKET, s3_key
                                )
                            else:
                                # If error is not 404, re-raise it
                                raise

                        inventory[base_key][arg] = href
            print(f"Completed processing group: {group}")

    return inventory


def handler(event, context):
    """handler for firing messages for historic inventory to the queue"""
    QUEUE_URL = os.environ["QUEUE_URL"]
    inventory = generate_inventory()
    sqs_client = boto3.client("sqs")
    for i, args in enumerate(inventory.values()):
        if not i % 1000:
            print(f"sending the {i}th set of args: {args}")
        sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(args))
