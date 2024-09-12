# Cozie Apple Read API
# Purpose: Read data Cozie-Apple data from InfluxDB for researchers
# Author: Mario Frei, 2024
# Status: Under development
# Project: Cozie-Apple

# Lesson learned: Saving dataframes as pickles is sensitive to the Pandas version.
# Pickle file made with one Pandas version, cannot necessarily be opened with another Pandas version
# Hence, the dataframe should be saved as as csv and then compressed as a zip       
# lambda_function.py is in directory /var/task/
# /tmp/ is the directory for ephemeral storage

# For debugging:
#df_csv = pd.read_csv(filename_ephemeral_storage, compression={'method': 'zip', 'archive_name': 'sample.csv'})
#print("my test_csv:")
#print(df_csv.head())

import json
import datetime
import pandas as pd
import numpy as np
from influxdb import DataFrameClient
import os
import boto3
import influx_prep


def lambda_handler(event, context):
    
    print('event:')
    print(event)
    
    if "headers" in event.keys():
        if 'x-api-key' in event['headers'].keys():
            request_key = event['headers']['x-api-key']     
            print('request key:', request_key)
            if event["requestContext"]["domainName"] == "bni6kdfystbmmrulxl7jxonphi0ieqvg.lambda-url.ap-southeast-1.on.aws": 
                # if call is directly to Lambda function URL check API key
                return {"statusCode": 400,
                        "body": json.dumps("Direct Lambda function URL call is not supported")}
    
    # Influx authentication
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']
    s3_bucket_name = os.environ['S3_BUCKET_NAME']

    # Check if there are any parameters, if not 400 error
    if ("queryStringParameters" not in event):
        return {
            "statusCode": 400,
            "body": json.dumps(
                "Query string parameter not found."
            ),
        }
        
    if (len(event)==0):
        return {
            "statusCode": 400,
            "body": json.dumps(
                "Query string parameter not defined. Please indicate the id_experiment and id_participant "
                "in the url string."
            ),
        }
        
    # Check if experiment-id is provided, if not send 400 error
    if "id_participant" not in event["queryStringParameters"]:
        return {
            "statusCode": 400,
            "body": json.dumps("id_participant not in url-string"),
        }
    else:
        id_participant = event["queryStringParameters"]["id_participant"]
        
    # Check if experiment-id is provided, if not send 400 error
    if "id_experiment" not in event["queryStringParameters"]:
        return {
            "statusCode": 400,
            "body": json.dumps("id_experiment not in url-string"),
        }
    else:
        id_experiment = event["queryStringParameters"]["id_experiment"]
                
        
    # Influx client
    client = DataFrameClient(db_host, db_port, db_user, db_password, db_name, ssl=True, verify_ssl=True)
    
    id_experiment = influx_prep.measurement(id_experiment)
    id_password = influx_prep.tag_value(id_password)
    id_participant = influx_prep.tag_value(id_participant)

    # Query all available tag keys
    query1 = f'SHOW FIELD KEYS FROM "cozie-apple"."autogen"."{id_experiment}"'
    result1 = client.query(query1)
    points = result1.get_points()
    
    # Create list of all tag_keys in 'measurement'/'experiment_id'
    list_tag_key = [];
    for item in points:
        list_tag_key.append(item["fieldKey"])
    
    # Assemble query for Cozie data
    str_tag_keys = '"id_participant", "id_device", "id_onesignal"'
    
    for my_tag_key in list_tag_key:
      str_tag_keys = str_tag_keys + ', "' + my_tag_key + '"' 
    

    
    query2 = "SELECT " + str_tag_keys + f' FROM "{db_name}"."autogen"."{id_experiment}" WHERE "id_participant"=\'{id_participant}\'AND "id_password"=\'{id_password}\''
    print(query2)
    
    # Query Cozie data
    result2 = client.query(query2, epoch='ns')

    try:
        df = pd.DataFrame.from_dict(result2[id_experiment])
        
    # no data for that query were available
    except KeyError:
        df = pd.DataFrame()
    
    # Drop columns with all elements equal to NaN
    df = df.dropna(axis=1, how='all')
    
    # Add nanosecond to preserve trail decimals for even timestamps
    #df.index  = df.index + pd.to_timedelta(1, unit='ns')
    
    # Replace None by NaN values
    df = df.fillna(value=np.nan)
    df = df.reset_index()
    
    print(df)
    
    # Convert to CSV
    unique_id = context.aws_request_id
    filename_ephemeral_storage = f'/tmp/{unique_id}.zip'
    my_csv = df.to_csv(filename_ephemeral_storage, date_format='%Y-%m-%d %H:%M:%S.%f%z', compression={'method': 'zip', 'archive_name': 'sample.csv'})
    
    # Upload to S3 bucket
    # How to grant access the Lambda function access to the S3 bucket: https://bobbyhadz.com/blog/aws-grant-lambda-access-to-s3
    s3 = boto3.client('s3')
    s3_filename = f'{unique_id}.zip'
    s3.upload_file(filename_ephemeral_storage, s3_bucket_name, s3_filename)
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': s3_bucket_name,
            'Key': s3_filename
        },
        ExpiresIn= 15 * 60
    )

    print("Upload Successful", url)
    
    return {"statusCode": 200, 
        "body": url}  
