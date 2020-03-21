import datetime

def display_script_duration(script_start_time):
    script_end_time = datetime.datetime.now()
    script_duration = script_end_time - script_start_time
    script_duration_in_seconds = script_duration.total_seconds()
    print('Script execution time (secs): ' + str(script_duration_in_seconds))
