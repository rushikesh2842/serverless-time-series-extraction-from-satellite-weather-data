import boto3
import netCDF4 as nc
import numpy as np
import pandas as pd
from datetime import datetime
import time
import os
import random
import json

# Fetch secrets (executed once per container cold start)
client = boto3.client('secretsmanager', region_name='us-east-1')
get_secret_value_response = client.get_secret_value(
    SecretId='weather-data-lambda-secrets-348962335134'
)

secret = get_secret_value_response['SecretString']
secret_dict = json.loads(secret)

# Buckets
INPUT_BUCKET_NAME = secret_dict['source_bucket_name']
OUTPUT_BUCKET = secret_dict['daily_parquet_data_bucket_name']

# Local file paths
TMP_FILE_NAME = "/tmp/tmp.nc"
LOCAL_OUTPUT_FILE = "/tmp/dataframe.parquet"

# Random coordinates
random.seed(10)
coords = [(random.randint(1000, 2500), random.randint(1000, 2500)) for _ in range(100)]

# S3 resource
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(INPUT_BUCKET_NAME)

def date_to_partition_name(date):
    d = datetime.strptime(date, "%Y%m%d")

    return d.strftime("%Y/%m/%d/")

def lambda_handler(event, context):
    date = str(event)  # event expected as string
    print("Processing date:", date)

    COLUMNS_NAME = ['time', 'point_id', 'DSSF_TOT', 'FRACTION_DIFFUSE']
    df = pd.DataFrame(columns=COLUMNS_NAME)

    prefix = date_to_partition_name(date)
    print("Loading files from prefix:", prefix)

    # List input files
    objects = bucket.objects.filter(Prefix=prefix)
    keys = [obj.key for obj in objects]

    for key in keys:
        bucket.download_file(key, TMP_FILE_NAME)
        print("Processing:", key)

        try:
            dataset = nc.Dataset(TMP_FILE_NAME)
            lats, lons = zip(*coords)
            data_1 = dataset['DSSF_TOT'][0][lats, lons]
            data_2 = dataset['FRACTION_DIFFUSE'][0][lats, lons]

            nb_points = len(lats)
            data_time = dataset.__dict__['time_coverage_start']
            time_list = [data_time for _ in range(nb_points)]
            point_id_list = list(range(nb_points))
            tuple_list = list(zip(time_list, point_id_list, data_1, data_2))
            new_data = pd.DataFrame(tuple_list, columns=COLUMNS_NAME)
            df = pd.concat([df, new_data])
            print("Dataset updated",df)
        except OSError:
            print("Error processing file:", key)

    # Replace masked by NaN
    df = df.map(lambda x: np.NaN if isinstance(x, np.ma.core.MaskedConstant) else x)
    print("Current Dataframe",df)
    # Save to parquet
    print("Writing parquet:", LOCAL_OUTPUT_FILE)
    df.to_parquet(LOCAL_OUTPUT_FILE)

    # Upload to S3
    s3_output_name = date + '.parquet'
    s3_client = boto3.client('s3')
    s3_client.upload_file(LOCAL_OUTPUT_FILE, OUTPUT_BUCKET, s3_output_name)
    print("Successfully uploaded")