# Load a set of tweets residing in a jsonl file into a postgres database.

import postgres_config
import psycopg2
import psycopg2.extras as extras
import sys
import json
import time
from datetime import datetime
import os
import subprocess

def convert_tweet_dict_to_tuple(tweet_dict):
    tweet_data_list = []
    
    for key in tweet_fields():
        tweet_data_list.append(tweet_dict[key])
            
    return tuple(tweet_data_list)

def convert_tweet_json_to_dict(tweet_json):
    tweet_dict = {}
    
    tweet_dict['status_id'] = tweet_json['id']
    tweet_dict['text'] = tweet_json['text']
    tweet_dict['created_at_str'] = tweet_json['created_at']
    tweet_dict['user_screen_name'] = tweet_json['user']['screen_name']
    
    if 'retweeted_status' in tweet_json:
        tweet_dict['is_retweet'] = True
        retweeted_status = tweet_json['retweeted_status']
        tweet_dict['retweeted_status_id'] = retweeted_status['id']
        tweet_dict['retweeted_status_retweet_count'] = retweeted_status['retweet_count']
        tweet_dict['retweeted_status_text'] = retweeted_status['text']
        tweet_dict['retweeted_status_user_screen_name'] = retweeted_status['user']['screen_name']
        
        if 'lang' in retweeted_status:
            tweet_dict['retweeted_status_language'] = retweeted_status['lang']
        else:
            tweet_dict['retweeted_status_language'] = None
    else:
        tweet_dict['is_retweet'] = False
        tweet_dict['retweeted_status_id'] = None
        tweet_dict['retweeted_status_retweet_count'] = None
        tweet_dict['retweeted_status_text'] = None
        tweet_dict['retweeted_status_user_screen_name'] = None
        tweet_dict['retweeted_status_language'] = None
    
    if 'lang' in tweet_json:
        tweet_dict['language'] = tweet_json['lang']
    else:
        tweet_dict['language'] = None
    
    return tweet_dict
    
def display_sleep_message(sleep_duration):
    display_timestamped_message('Sleeping ' + str(sleep_duration) + ' seconds...')
    time.sleep(sleep_duration)
    
def display_timestamped_message(message):
    print('[' + str(datetime.utcnow()) + '] ' + message)
    
def insert_tweet_rows_into_database(tweet_data_rows):
    display_timestamped_message('Inserting batch of ' + str(len(tweet_data_rows)) + ' tweets...')
    
    try:
        connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
        cursor = connection.cursor()
        sql = "INSERT INTO tweet ("
        tweet_field_names = tweet_fields()
        
        for index, key in enumerate(tweet_field_names):
            sql += key
            
            if index + 1 < len(tweet_field_names):
                sql += ','
                
        sql += ") VALUES %s;"
        
        data = tuple(tweet_data_rows)
        extras.execute_values(cursor, sql, data)
        connection.commit()
        cursor.close()
        connection.close()
        
        display_timestamped_message('Batch insert complete')
        
    except Exception as ex:
        print('Error occurred while attempting to connect to postgres database.')
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        
def inspect_tweet_dict(tweet_dict):
    print('=' * 30)
    print(tweet_dict)
    
    for attribute in tweet_dict.keys():
        print('Attribute name: ' + attribute + '; Type: ' + str(type(tweet_dict[attribute])) + '; Value: ' + str(tweet_dict[attribute]))
    
def load_tweets_into_postgres():
    script_start_time = datetime.now()
    display_timestamped_message('Loading tweets into postgres database from jsonl file...')
    script_parameter_count = len(sys.argv) - 1
    expected_parameter_count = 1
    
    if script_parameter_count < expected_parameter_count:
        print('Script is missing paramters. ' + str(script_parameter_count) + ' provided. ' + str(expected_parameter_count) + ' expected.')
        print('Parameters: (tweet_jsonl_file_path[, tweet_quantity, delay_time_between_tweets, display_loaded_tweet_data])')
        return False
    
    tweet_jsonl_file_path = sys.argv[1]
    tweet_quantity_is_limited = False
    delay_time_between_tweets_is_specified = False
    display_tweet_data_is_specified = False
    
    if script_parameter_count > 1:
        tweet_quantity_is_limited = True
        tweet_quantity = int(sys.argv[2])
        
    if script_parameter_count > 2:
        delay_time_between_tweets_is_specified = True
        delay_time_between_tweets = float(sys.argv[3])
        
    if script_parameter_count > 3:
        display_tweet_data_is_specified = True
        display_tweet_data = sys.argv[3].upper() == 'T'
        
    current_batch_data = []
    max_batch_size = 20000
    processed_tweets_count = 0
    current_batch_size = 0
    
    start_postgresql_service()
    display_timestamped_message('Reading tweets from file...')
    
    with open(tweet_jsonl_file_path, 'r') as file:
        for index, line in enumerate(file):
            tweet_json = json.loads(line)
            tweet_dict = convert_tweet_json_to_dict(tweet_json)
            
            if display_tweet_data_is_specified:
                inspect_tweet_dict(tweet_dict)
            
            if delay_time_between_tweets_is_specified:
                display_sleep_message(delay_time_between_tweets)
                
            current_batch_data.append(convert_tweet_dict_to_tuple(tweet_dict))
            current_batch_size += 1
            processed_tweets_count += 1
            
            if current_batch_size >= max_batch_size or (tweet_quantity_is_limited and processed_tweets_count >= tweet_quantity):
                insert_tweet_rows_into_database(current_batch_data)
                current_batch_size = 0
                current_batch_data = []
            
                if tweet_quantity_is_limited and processed_tweets_count >= tweet_quantity:
                    display_timestamped_message('Halting due to reaching max tweet load limit specified: ' + str(processed_tweets_count) + '/' + str(tweet_quantity))
                    break
                
    if current_batch_size > 0:
        insert_tweet_rows_into_database(current_batch_data)
                
    display_timestamped_message('Total tweets processed: ' + str(processed_tweets_count))
    script_end_time = datetime.now()
    script_duration = script_end_time - script_start_time
    script_duration_in_seconds = script_duration.total_seconds()
    print('Script execution time (secs): ' + str(script_duration_in_seconds))
    
def postgresql_service_is_running():
    postgresql_service_status = os.system('service postgresql status')
    
    return postgresql_service_status == 0
    
def start_postgresql_service():
    if postgresql_service_is_running():
        display_timestamped_message('Postgresql service is running.')
    else:
        display_timestamped_message('Postgresql service is not running.')
        display_timestamped_message('Starting postgresql service...')
        command_output = subprocess.call(['sudo', 'service', 'postgresql', 'start'])
        print(command_output)
                
def tweet_fields():
    tweet_fields = [
        'status_id',
        'created_at_str',
        'language',
        'is_retweet',
        'retweeted_status_id',
        'text',
        'user_screen_name',
        'retweeted_status_retweet_count',
        'retweeted_status_text',
        'retweeted_status_user_screen_name',
        'retweeted_status_language'
    ]
    
    return tweet_fields
    
load_tweets_into_postgres()
