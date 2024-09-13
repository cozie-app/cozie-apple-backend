# Send notifications to Cozie-Apple app
# Author: Mario Frei, 2024
# Project: Cozie
# Sources:
#  - https://documentation.onesignal.com/reference/push-channel-properties

import requests
import json
import os
import influx_prep
from influxdb import InfluxDBClient
import pandas as pd

def lambda_handler(event, context):
    print(f'event: {event}')
    print(f'context: {context}')
    
    # Parse payload
    if 'body' in event:
        # Parse payload when function URL is called
        payload = json.loads(event['body'])
    else: 
        # Parse payload when API Gateway is called
        payload = event
        
    print('Payload:\n', payload)
    
    if 'id_experiment' in payload:
        id_experiment = payload['id_experiment']
    else:
        return {
            'statusCode': 400,
            'body': 'Experiment ID is missing'
        }
        
    if 'id_participant' in payload:
        id_participant = payload['id_participant']
    else:
        return {
            'statusCode': 400,
            'body': 'Participant ID is missing'
        }
        
    if 'id_password' in payload:
        id_password = payload['id_password']
    else:
        return {
            'statusCode': 400,
            'body': 'id_password is missing'
        }
    
    message = ''
    heading = ''
    subtitle = ''
    buttons = ''
    
    if 'message' in payload:
        message = payload['message']

    if 'heading' in  payload:
        heading = payload['heading']
    
    if 'subtitle' in  payload:
        subtitle = payload['subtitle']
        
    if 'buttons' in payload:
        buttons = payload['buttons']

    # Sanitize payload
    id_experiment = influx_prep.measurement(id_experiment)
    id_participant = influx_prep.tag_value(id_participant)
    id_password = influx_prep.tag_value(id_password)
    
    # Initialize InfluxDB client
    influx_client = InfluxDBClient(os.environ['db_host'], 
                                   os.environ['db_port'],
                                   os.environ['db_user'],
                                   os.environ['db_password'],
                                   os.environ['db_name'],
                                   ssl=True, 
                                   verify_ssl=True)
    
    # Query DB
    db_name = os.environ['db_name']
    influx_query = f'SELECT * FROM "{db_name}"."autogen"."{id_experiment}" WHERE "id_participant"=\'{id_participant}\' AND "id_password"=\'{id_password}\' ORDER BY DESC LIMIT 1'
    print('query influx: ', influx_query)
    
    result = influx_client.query(influx_query)
    print('result:\n', result)
    
    # Get OneSignal ID
    response_body = dict()
    if len(result)<1:
        # Handle empty result set
        id_onesignal = ''
        id_password_db = ''
    else:
        # Convert result from database to dataframe
        df = pd.DataFrame.from_dict(result[id_experiment])
        id_onesignal = df['id_onesignal'][0]
        print(f'id_onesignal: {id_onesignal}')
    
    if id_onesignal == '':
        return {
            'headers': {'Content-Type': 'application/json; charset=utf-8'},
            'statusCode': 400,
            'body': {'Error message': 'No valid OneSignal Player ID found.'}
        }
    
    # Send push notification
    token = os.environ["onesignal_token"] # provided by OneSignal. Can be found on the Keys & IDs page on the OneSignal Dashboard for this particular app
    app_id = os.environ["onesignal_app_id"] # provided by OneSignal, needs to be implemented in the app
    
    header = {"Content-Type": "application/json; charset=utf-8",
              "Authorization": "Basic "+ token}
    
    payload_out = {"app_id": app_id,
                   "include_player_ids": [id_onesignal],
                   "contents": {"en": message},
                   "headings": {"en": heading},
                   "subtitle": {"en": subtitle},
                   }
    if buttons != '':
        payload_out['buttons'] = buttons
        
    print('payload_out\n', payload_out)
    
    response = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload_out))
    print('response.content: ', response.content) # The content contains and ID that is not the player id. It appears to be a request ID.
    print('req.reason: ', response.reason)
    
    
    return {
        'headers': {'Content-Type': 'application/json; charset=utf-8'},
        'statusCode': response.status_code,
        'body': response.content
    }