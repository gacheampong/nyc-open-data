"""
# Script Info
# Script Name: load_nyc_open_data.py
# Created By: Glen Acheampong
# Created On: 2022-05-15
# Description: Script to load NYC Open Data files from S3 to a local postgres container
"""

# Import modules -------------------------------------------------------------------------------------------------------
import os
import sys

import argparse
import io
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Set up argparser -----------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Select dataset to load from S3 to Postgres.")

parser.add_argument(
    "--dataset",
    type=str,
    dest="dataset",
    choices=['311_service_requests',
             'nyc_facilities',
             'film_permits',
             "central_park_squirrel_census",
             "yellow_cab_trips",
             "dog_licenses"],
    help="Name of NYC Open Data dataset stored in S3.",
)

args = parser.parse_args()
# args = parser.parse_args(["--dataset", "311_service_requests"])

dataset = args.dataset

# Start time -----------------------------------------------------------------------------------------------------------
start_time = datetime.now()

# Functions/callables --------------------------------------------------------------------------------------------------

load_dotenv()

AWS_REGION = "us-east-1"
AWS_S3_BUCKET = 'ga-nyc-open-data'

# Pre-processing ------------------------------------------------------------------------------------------------------

engine = create_engine(
    f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
)

with engine.begin() as connection:
    connection.execute("CREATE SCHEMA IF NOT EXISTS raw;")

# Processing ----------------------------------------------------------------------------------------------------------

## Read data from S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=AWS_REGION
)

response = s3_client.get_object(
    Bucket=AWS_S3_BUCKET,
    Key=f"{dataset}/{datetime.now().strftime(format='%Y-%m-%d')}/{dataset}.csv"
)

nyc_open_dataset = response["Body"]

nyc_open_dataset = pd.read_csv(
    io.BytesIO(nyc_open_dataset.read()),
    encoding="utf8",
    delimiter=",",
    low_memory=False
)

print(f"Loading {dataset} S3 file to Postgres...")

with engine.begin() as connection:

    nyc_open_dataset.to_sql(
        dataset,
        schema="raw",
        con=connection,
        if_exists='replace',
        index=False
    )

print(f"Load complete!")

# End time -------------------------------------------------------------------------------------------------------------

print(f"Total script time is {datetime.now() - start_time}")

# Exit -----------------------------------------------------------------------------------------------------------------
sys.exit()
