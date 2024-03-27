# Function to check the datatype of payloads
# This function makes sure that the payloads have always the same datatypes. 
# This check is necessary because influx only accepts values of the same datatype for a field (column), as the previous values in this field (column)
# Ideally, datatype consistency is ensured in the app/payload instead of here.
# Project: Cozie-Apple
# Experiment: Hwesta, Dev
# Author: Mario Frei, 2024

# question: data type of sound_pressure, ts_restingHeartRate # XXX

import datetime

def check_type(payload):
    print("start check type")
    # Fields with integer values
    int_fields = ['si_iphone_battery_charge_state',
                  'si_iphone_cellular_signal_strength',
                  'si_iphone_wifi_signal_strength',
                  'si_watch_battery_charge_state',
                  'si_watch_cellular_signal_strength',
                  'si_watch_wifi_signal_strength',
                  'ts_altitude',
                  'ts_exercise_time',
                  'ts_heart_rate',
                  'ts_location_accuracy_horizontal',
                  'ts_location_accuracy_vertical',
                  'ts_location_floor',
                  'ts_oxygen_saturation',
                  'ts_resting_heart_rate',
                  'ts_stand_time',
                  'ts_step_count',
                  'ws_exercise_time',
                  'ws_heart_rate',
                  'ws_location_floor',
                  'ws_oxygen_saturation,',
                  'ws_resting_heart_rate',
                  'ws_sleep_core',
                  'ws_sleep_deep',
                  'ws_sleep_unspecified',
                  'ws_stand_time',
                  'ws_step_count',
                  'ws_survey_count',
                  'wss_goal',
                  'wss_reminder_interval',
                  'wss_time_out']
                  
    # Fields with float values
    float_fields = ['ts_audio_exposure_environment',
                    'ts_audio_exposure_headphones',
                    'ts_BMI',
                    'ts_body_mass',
                    'ts_HRV',
                    'ts_latitude',
                    'ts_longitude',
                    'ts_walking_distance',
                    'ts_active_energy_burned',
                    'ts_workout_duration',
                    'ts_move_time',
                    'ws_altitude',
                    'ws_audio_exposure_environment',
                    'ws_audio_exposure_headphones', # not yet in DB
                    'ws_HRV',
                    'ws_latitude',
                    'ws_location_accuracy_horizontal',
                    'ws_location_accuracy_vertical',
                    'ws_longitude',
                    'ws_sleep_awake',
                    'ws_sleep_in_bed',
                    'ws_sleep_REM',
                    'ws_sleep_unspecified',
                    'ws_walking_distance',
                    'ws_active_energy_burned',
                    'ws_workout_duration',
                    'ws_move_time',
                    'ts_sleep_awake',           # not yet in DB
                    'ts_sleep_core',            # not yet in DB
                    'ts_sleep_deep',            # not yet in DB
                    'ts_sleep_in_bed',          # not yet in DB
                    'ts_sleep_REM',             # not yet in DB
                    'ts_sleep_unspecified',     # not yet in DB
                    'ts_appleWalkingSteadiness' # Waseda university
                     ]
    # Fields with string values (just for documentation)
    #string_fields = ['ts_workout_type',
    #                 'ws_workout_type'
    #                 ]
    
                    
    for key in payload["fields"]:
        print(key, end=": ")
        print("is value=", payload["fields"][key], ", type=", type(payload["fields"][key]))
        if key in int_fields:
            if payload["fields"][key] == '':
                print(key ," is blank. Set to 0 (int).", end=", ")
                payload["fields"][key] = 0
            else:
                print(key ," cast to int", end=", ")
                payload["fields"][key] =  int(payload["fields"][key])
            
        elif key in float_fields:
            print(key ,": cast to float", end=", ")
            payload["fields"][key] =  float(payload["fields"][key])
        
        print(payload["fields"][key], end="\r")
        
        # Offset ws_data by one second
        offset_fields = ['ws_heart_rate',
                         'ws_oxygen_saturation',
                         'ws_resting_heart_rate',
                         'ws_sleep_unspecified',
                         'ws_stand_time',
                         'ws_step_count',
                         'ws_audio_exposure_environment',
                         'ws_audio_exposure_headphones', # not yet in DB
                         'ws_HRV',
                         'ws_sleep_awake',
                         'ws_sleep_core',
                         'ws_sleep_in_bed',
                         'ws_sleep_deep',
                         'ws_sleep_REM',
                         'ws_sleep_unspecified',
                         'ws_walking_distance']
        #if key in offset_fields:
        #    print("")
        #    print("Key for time offset detected", end="\r")
        #    time_old = datetime.datetime.strptime(payload['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
        #    time_new = time_old + datetime.timedelta(seconds=1)
        #    time_new = datetime.datetime.strftime(time_new, "%Y-%m-%dT%H:%M:%S.%f%z")
        #    payload['time'] = time_new
        #    print("payload['time']:", payload['time'], end="\r")
        #    print("time_old:", time_old, end="\r")
        #    print("time_new:", time_new, end="\r")
        #    print("payload['time']:", payload['time'], end="\r")
        
    print("end check type")
    return payload