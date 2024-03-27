# Function to add timestamp lambda for specific column
# Project: Cozie-Apple
# Experiment: Hwesta, Dev
# Author: Mario Frei, 2024

import datetime

def add_timestamp_lambda(payload, timestamp_lambda):
    print("start add_timestamp_lambda")
    
    # Fields to which the lambda timestamp should be added
    lambda_fields = ['ws_survey_count',
                     'action_button',
                    
                     'ts_latitude',    
                     'ts_longitude',
                     'ts_altitude',
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
                     'ts_oxygen_saturation',
                     'ts_audio_exposure_environment',
                     'ts_audio_exposure_headphones',
                     'ts_BMI',
                     'ts_body_mass',
                     'ts_resting_heart_rate',
                     'ts_stand_time',
                     'ts_step_count',
                     'ts_HRV',
                     'ts_walking_distance',
                     'ts_wrist_temperature',
                     'ts_sleep_awake',       # not yet in DB
                     'ts_sleep_core',        # not yet in DB
                     'ts_sleep_deep',        # not yet in DB
                     'ts_sleep_in_bed',      # not yet in DB
                     'ts_sleep_REM',         # not yet in DB
                     'ts_sleep_unspecified', # not yet in DB
                     
                     'ts_exercise_time',
                     'ts_walking_distance',
                     'ts_active_energy_burned',
                     'ts_workout_type', 
                     'ts_workout_duration',
                    
                     'ws_heart_rate',
                     'ws_audio_exposure_environment',
                     'ws_audio_exposure_headphones', # not yet in DB
                     'ws_HRV',
                     'ws_oxygen_saturation',
                     'ws_resting_heart_rate',
                     'ws_stand_time',
                     'ws_step_count',
                     'ws_walking_distance',
                     'ws_wrist_temperature',
                     'ws_sleep_core',
                     'ws_sleep_deep',
                     'ws_sleep_unspecified', 
                     'ws_sleep_awake',
                     'ws_sleep_in_bed',
                     'ws_sleep_REM',
                     'ws_sleep_unspecified',
                     
                     'ws_exercise_time',
                     'ws_walking_distance',
                     'ws_active_energy_burned',
                     'ws_workout_type',
                     'ws_workout_duration',
                  
                     'ws_latitude',
                     'ws_longitude',
                     'ws_altitude',
                    #'ws_location_accuracy_horizontal',
                    #'ws_location_accuracy_vertical',
                    #'ws_location_floor',
                  ]
    # Add missing 'transmit_trigger' infomration
    if 'transmit_trigger' not in list(payload["fields"].keys()):
        payload["fields"]['transmit_trigger'] = 'empty'
        
    
    # Add timestamp_lambda for selected fields
    for key in list(payload["fields"].keys()):
        #print(key, end=": ")
        if key in lambda_fields:
            #print("Key for x_lambda column detected", end="\r")
            payload["fields"][f"{key}_lambda"] = timestamp_lambda
            payload["fields"][f"{key}_trigger"] = payload["fields"]["transmit_trigger"]
        
    print("end add timestamp lambda column")
    return payload