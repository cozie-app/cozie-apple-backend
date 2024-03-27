# Cozie Apple v3 Read API
# Purpose: Read data from Cozie-Apple app from InfluxDB
# Author: Mario Frei, 2023
# Status: Under development
# Project: Cozie
# Experiemnt: Osk

# To do:
#  - Optimize finding of time-zone. It currently takes 5.3s (78%)
#    The execution of the entire function takes 6.8s

import json
import datetime
import pandas as pd
import numpy
from influxdb import InfluxDBClient
import os
from valid_votes import keep_valid_votes
#import timezonefinder

# Debugging
import time

# Influx authentication
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_host = os.environ["DB_HOST"]
db_port = os.environ["DB_PORT"]
db_name = os.environ["DB_NAME"]

# Config parameters
DEFAULT_WEEKS = 100
REQUESTABLE_PARAMETERS = ['ws_survey_count_valid', 'ws_survey_count_invalid', 'ws_timestamp_survey_last']

def lambda_handler(event, context):

    print("Debugging")
    time_start = time.time()
    print("event: ", event)

    if "queryStringParameters" not in event:
        return {
                "statusCode": 400,
                "body": json.dumps("no queryStringParameters found"),
            }    

    # Check if id_experiment, id_participant, id_password is provided, send error otherwise
    #if (("id_participant" not in event["queryStringParameters"]) or 
    #    ("id_experiment" not in event["queryStringParameters"]) or
    #    ("id_password" not in event["queryStringParameters"])):
    #    return {
    #        "statusCode": 400,
    #        "body": json.dumps("id_participant, id_experiment, or id_password not in url-string"),
    #    }
    
    # Parse query string parameters
    id_participant = event["queryStringParameters"]["id_participant"]
    id_experiment = event["queryStringParameters"]["id_experiment"]
    id_password = event["queryStringParameters"]["id_password"]
    print("id_participant: ", id_participant)
    print("id_experiment: ", id_experiment)
    print("id_password: ", id_password)
    
    # Select the number of weeks to query data: Default is 2 weeks
    if "duration" in event["queryStringParameters"]:
        weeks = float(event["queryStringParameters"]["weeks"])
    else:
        weeks = DEFAULT_WEEKS
        
    # Influx client
    client = InfluxDBClient(
        db_host, db_port, db_user, db_password, db_name, ssl=True, verify_ssl=True
    )
    
    # Query database    
    query_influx = f'SELECT "ws_survey_count", "ws_longitude", "ws_latitude", "ws_timestamp_start" '\
                   f'FROM "{db_name}"."autogen"."{id_experiment}" '\
                   f'WHERE "time" > now()-{weeks}w '\
                   f'AND "id_participant"=\'{id_participant}\' '\
                   f'AND "ws_survey_count">=0 '\
    #              f'AND "id_password"=\'{id_password}\''
        
    print("query influx: ", query_influx)
    result = client.query(query_influx)
    print("result: ", result)
    
    response_body = dict()
    
    if len(result)<1:
        # Handle empty result set
        response_body["ws_survey_count_valid"] = "0"
        response_body["ws_survey_count_invalid"] = "0"
        response_body["ws_timestamp_survey_last"]= "-"
    else:
        
        # Convert result from database to dataframe
        df = pd.DataFrame.from_dict(result[id_experiment])
        # Convert 'time' column to datetime-index
        df["time"] = pd.to_datetime(df["time"])
        df["time"] = df["time"].dt.tz_localize(None)
        df.index = df["time"]
        df = df.drop(["time"], axis=1)
        
        # Check valid votes
        df_valid_only = keep_valid_votes(df)
        
        # Get last timestamp of last watch survey
        ws_timestamp_last = pd.to_datetime(df["ws_timestamp_start"][-1], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
        
        # Get timezone from ws_timestamp_last
        #timezone_target = ws_timestamp_last.tzinfo
        
        # Retrieve requested parameters
        #response_body["ws_survey_count"]       = str(df["ws_survey_count"].notna().count())
        response_body["ws_survey_count_valid"] = str(df["ws_survey_count"].notna().count())
        response_body["ws_survey_count_invalid"] = str(df["ws_survey_count"].notna().count() - df_valid_only["ws_survey_count"].notna().count())
        #response_body["ws_timestamp_survey_last"]= df.index[-1].strftime('%d.%m.%Y - %H:%M')
        #response_body["ws_timestamp_survey_last"]= df.index[-1].tz_localize('UTC').tz_convert(timezone_target).strftime('%d.%m.%Y - %H:%M')
        response_body["ws_timestamp_survey_last"]= df.index[-1].tz_localize('UTC').strftime('%d.%m.%Y - %H:%M')
        
    print("response_body")
    print(response_body)
    
    # Remove parameters that were not requested
    request_parameters = []
    if ("request" in event["queryStringParameters"]):
        for parameter in REQUESTABLE_PARAMETERS:
            if parameter not in event["queryStringParameters"]["request"]:
                print("pop ", parameter)
                response_body.pop(parameter)
    
    # Return requested parameters to requestor
    return {"statusCode": 200, "body": json.dumps(response_body)}
