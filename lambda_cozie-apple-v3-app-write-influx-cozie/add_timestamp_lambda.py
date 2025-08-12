# Function to add timestamp lambda and _trigger field for specific column
# Project: Cozie-Apple
# Experiment: Hwesta, Dev
# Author: Mario Frei, 2025
# Note: Most fields have been commented out to save InfluxDB resources.

import datetime

def add_timestamp_lambda(payload, timestamp_lambda, timestamp_lambda_ms):
    print("start add_timestamp_lambda")
    
    # Fields for which the _lambda and _trigger meta data should be logged
    # Most fields are commented out to lower the strain on InfluxDB
    lambda_fields = ['ws_survey_count',
                     'action_button',
                     'action_button_pressed',
                     'notification_title',
                    
                     'ts_latitude',    
                     'ts_longitude',
                    #'ts_altitude',
                    #'ts_location_accuracy_horizontal',
                    #'ts_location_accuracy_vertical',
                    #'ts_location_floor',
                    
                    #'wss_goal',
                    #'wss_reminder_interval',
                    #'wss_time_out',
                    #'si_iphone_battery_charge_state',
                    #'si_iphone_cellular_signal_strength',
                    #'si_iphone_wifi_signal_strength',
                    #'si_watch_battery_charge_state',
                    #'si_watch_cellular_signal_strength',
                    #'si_watch_wifi_signal_strength',
                    
                     'ts_heart_rate',
                    #'ts_oxygen_saturation',
                    #'ts_audio_exposure_environment',
                    #'ts_audio_exposure_headphones',
                    #'ts_BMI',
                    #'ts_body_mass',
                    #'ts_resting_heart_rate',
                    #'ts_stand_time',
                    #'ts_step_count',
                    #'ts_HRV',
                    #'ts_walking_distance',
                    #'ts_wrist_temperature',
                    #'ts_sleep_awake',
                    #'ts_sleep_core',
                    #'ts_sleep_deep',
                    #'ts_sleep_in_bed',
                    #'ts_sleep_REM',
                    #'ts_sleep_unspecified',
                     
                    #'ts_exercise_time',
                    #'ts_walking_distance',
                    #'ts_active_energy_burned',
                    #'ts_workout_type', 
                    #'ts_workout_duration',
                    
                    'ws_heart_rate',
                    #'ws_audio_exposure_environment',
                    #'ws_audio_exposure_headphones',
                    #'ws_HRV',
                    #'ws_oxygen_saturation',
                    #'ws_resting_heart_rate',
                    #'ws_stand_time',
                    #'ws_step_count',
                    #'ws_walking_distance',
                    #'ws_wrist_temperature',
                    #'ws_sleep_core',
                    #'ws_sleep_deep',
                    #'ws_sleep_unspecified', 
                    #'ws_sleep_awake',
                    #'ws_sleep_in_bed',
                    #'ws_sleep_REM',
                    #'ws_sleep_unspecified',
                     
                    #'ws_exercise_time',
                    #'ws_walking_distance',
                    #'ws_active_energy_burned',
                    #'ws_workout_type',
                    #'ws_workout_duration',
                  
                    #'ws_latitude',
                    #'ws_longitude',
                    #'ws_altitude',
                    #'ws_location_accuracy_horizontal',
                    #'ws_location_accuracy_vertical',
                    #'ws_location_floor',
                  ]
    # Add missing 'transmit_trigger' information
    if 'transmit_trigger' not in list(payload["fields"].keys()):
        payload["fields"]['transmit_trigger'] = 'empty'
        
    # Add timestamp_lambda for selected fields
    for key in list(payload["fields"].keys()):
        #print(key, end=": ")
        if key in lambda_fields:
            #print("Key for x_lambda column detected", end="\r")
            #payload["fields"][f"{key}_lambda"] = timestamp_lambda # No longer logged to save memory in DB
            payload["fields"][f"{key}_lambda_ms"] = timestamp_lambda_ms
            payload["fields"][f"{key}_trigger"] = payload["fields"]["transmit_trigger"]
        
    print("end add timestamp lambda column")
    return payload
