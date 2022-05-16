"""
# Script Info
# Script Name: extract_nyc_open_data.py
# Created By: Glen Acheampong
# Created On: 2022-05-15
# Description: Script to extract NYC Open Data files from NYC Open Data Portal and load to remote file storage
"""

# Import modules -------------------------------------------------------------------------------------------------------
import os
import sys

import argparse
import io
from datetime import datetime

import boto3
import requests
import pandas as pd
from dotenv import load_dotenv

# Set up argparser -----------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Select endpoint to extract from NYC Open Data.")

parser.add_argument(
    "--endpoint",
    type=str,
    dest="endpoint",
    choices=['311_service_requests',
             'nyc_facilities',
             'film_permits',
             "central_park_squirrel_census",
             "yellow_cab_trips",
             "dog_licenses"],
    help="Name of API for NYC Open Data dataset.",
)

args = parser.parse_args()
# args = parser.parse_args(["--endpoint", "311_service_requests"])

endpoint = args.endpoint

# Start time -----------------------------------------------------------------------------------------------------------
start_time = datetime.now()

# Functions/callables --------------------------------------------------------------------------------------------------

load_dotenv()

AWS_REGION = "us-east-1"
AWS_S3_BUCKET = 'ga-nyc-open-data'

NYC_OPEN_DATA_APP_TOKEN = os.environ['OPEN_DATA_APP_TOKEN']

ENDPOINT_MAP = {
    "311_service_requests": "erm2-nwe9",
    "nyc_facilities": "ji82-xba5",
    "film_permits": "tg4x-b46p",
    "central_park_squirrel_census": "vfnx-vebw",
    "yellow_cab_trips": "t29m-gskq",
    "dog_licenses": "nu7n-tubp",
}

# Set endpoint ---------------------------------------------------------------------------------------------------------

ENDPOINT = f'https://data.cityofnewyork.us/resource/{ENDPOINT_MAP.get(endpoint)}.csv'

# Pre-processing -------------------------------------------------------------------------------------------------------

result = requests.get(
    ENDPOINT,
    headers={
        'X-App-Token': NYC_OPEN_DATA_APP_TOKEN
    }
)

open_data = pd.read_csv(io.StringIO(result.text))

# Upload data to S3 ----------------------------------------------------------------------------------------------------

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=AWS_REGION
)

with io.StringIO() as csv_buffer:

    open_data.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET,
        Key=f"{endpoint}/{datetime.now().strftime(format='%Y-%m-%d')}/{endpoint}.csv",
        Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

# End time -------------------------------------------------------------------------------------------------------------

print(f"Total script time is {datetime.now() - start_time}")

# Exit -----------------------------------------------------------------------------------------------------------------
sys.exit()
