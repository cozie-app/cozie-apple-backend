# Cozie Apple Write API
# Purpose: Transfer data from SQS queue to InfluxDB
# Project: Cozie-Apple
# Experiment: Hwesta, Dev
# Author: Mario Frei, 2024
# Test with Colcab notebook and Cloudwatch instead of test payload provded with 'Test' button on AWS

import os
import json
from influxdb import InfluxDBClient, DataFrameClient
import time
from datetime import datetime
from pprint import pprint
import requests
import logging
from pprint import pformat
from check_type import check_type
from add_timestamp_lambda import add_timestamp_lambda

# Configure the logging format
log_format = 'asdf %(levelname)s: %(message)s'

# Set the log level to capture messages at or above the specified level
root_logger = logging.getLogger()

# Set log level to capture DEBUG, INFO, WARNING, ERROR, CRITICAL messages
root_logger.setLevel(logging.CRITICAL)

# Create a custom formatter
formatter = logging.Formatter('%(message)s')

# Get the existing handlers for the root logger and update their format
for handler in root_logger.handlers:
    handler.setFormatter(formatter)

# influx authentication
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']

def lambda_handler(event, context):

    # Influx client
    client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name, ssl=True, verify_ssl=True)
    
    # Debugging 
    print("##################################### Start #####################################")
    logging.debug(pformat(event))
    
    print('event')
    print(event)
    
    # Process SQS message
    print('Check if Lambda was triggered by SQS')
    if 'Records' in event:
        print('"Records" in event')
        print("Length of event['Records']: ", len(event['Records']))
        if ('messageId' in event['Records'][0]):
            print('"messageId" in event["Records"][0]')
            print('Event source: ', event['Records'][0]['eventSource'])
            print('Event source ARN: ', event['Records'][0]['eventSourceARN'])
            #print('SQS message body type: ', type(event['Records'][0]['body']))
            #print('SQS message body: ', event['Records'][0]['body'])
            payload_list = json.loads(event['Records'][0]['body'])
            #print('Payload_list type: ', type(payload_list))
            #print('Payload_list[0]: ', payload_list[0])
            
            print('Insert payload into DB')
            print('...')
    else:
        print("'body' not found in event")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
                },
            "body": "Error: Body not found"
            }
            
    #print("-----------------------------------------")
    # There are some finer point that I don't quite get between a JSON request
    # using Python package requests and Postman.
    #payload_list = event['body']
    #payload_list = json.loads(payload_list)
    #if type(payload_list) == str:
    #    payload_list = json.loads(payload_list)
    #print(" type(payload_list):", type(payload_list))
    #logging.debug(pformat(payload_list))

    #if type(payload_list)!= list:
    #    print("Convert payload_list to list")
    #    payload_list = [payload_list]
    
    
    print("Iterate through all paylods in list:")
    payload_counter = 0
    for payload in payload_list:
        payload_counter += 1
        print(f"######################## Payload {payload_counter} ########################")
        if payload_counter == 1:
            print('*** Payload ***')
            print('Type: ', type(payload))
            print(payload)
        #print("type(payload):", type(payload))
        #logging.debug(pformat(payload))
        
        # Check for minimal presence of required fields/tags
        required_keys = ['time', 'id_participant', 'measurement'] # XXX add id_password, measurement is synonym for id_experiment
        for key_required in required_keys:
            if ('time' not in payload) or ('measurement' not in payload) or ('id_participant' not in payload['tags']):
                print(f"On or more of the required keys ({required_keys}) were not in the payload")
                return {
                    "statusCode": 500,
                    "headers": {
                        "Content-Type": "application/json"
                        },
                    "body": "Error: Required fields or tags ar missing"
                    }
        
        print('id_experiment  = ', payload['measurement'])
        print('id_participant = ', payload['tags']['id_participant'])
        
        # Check value types in payload
        payload = check_type(payload)
        
        # Get timestamp from call of lambda function for entry (might get overwritten by later data insertions)
        timestamp_lambda = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Add timestamp lambda as field  for entire row (might get overwritten by later data insertions with the same timestamp and tag)
        payload['fields']['timestamp_lambda'] = timestamp_lambda
        
        # Add timestamp lambda as specific field, e.g., ts_heart_rate -> ts_heart_rate_lambda
        payload = add_timestamp_lambda(payload, timestamp_lambda)
        
        # Debug print
        #print("type(payload):", type(payload))
        #logging.debug(pformat(payload))
        
        # Convert payload back to json
        json_body = [payload]
    
        #print("json.dumps(json_body):")
        #logging.debug(json.dumps(json_body))

        try:
            feedback = client.write_points(json_body, batch_size=5000)  # write to InfluxDB
            print("Client write: ", feedback)
        except Exception as e:
            print("InfluxDBServerError:", e)
        

    return_data = {"statusCode": 200,
                   "headers": {"Content-Type": "application/json"},
                   "body": "Success"}
                   
    print(return_data)
    print("##################################### END #####################################")
    return return_data
