import datetime

def display_timestamped_message(message):
    print('[' + str(datetime.datetime.utcnow()) + '] ' + message)
