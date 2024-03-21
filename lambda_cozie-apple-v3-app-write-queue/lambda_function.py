# Cozie Apple App API
# Purpose: Transfer Cozie-Apple data from the Cozie-Apple app into the SQS queue
# Author: Mario Frei, 2024
# Status: Under development
# Project: Cozie-Apple
# Experiment: Hwesta
# Note:
#  - The memory configuration for this Lambda function was deliberatly oversized.
#    The available CPU for the lambda scales with the memory. 
#    Hence, there is more computational power with more memory, i.e., the Lambda
#    functions runs faster, which results in faster response times.
#    Source: https://docs.aws.amazon.com/lambda/latest/operatorguide/computing-power.html
#  - The payloads needs to be split into smaller chunks due to the message size
#    limit of 256 KiB.
#    Source: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html  

import boto3
import json
import os

def lambda_handler(event, context):
    
    # Read payload
    print("event")
    print(event)
    payload = json.loads(event['body'])
    
    # Initialize SQS client
    sqs_client = boto3.client('sqs')
    sqs_url = os.environ["SQS_URL"]
    
    # Split payload and send it to SQS queue
    print('Split up payload')
    num_payloads_in_message = 100
    for i in range(0, len(payload), num_payloads_in_message):
        print('Send payload to SQS queue')
        response = sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(payload[i:i+num_payloads_in_message])
        )
        print('SQS response:')
        print(response)