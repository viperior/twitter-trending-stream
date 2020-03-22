# Load a set of tweets residing in a jsonl file into a postgres database.

import datetime
import display_script_duration
import display_timestamped_message
import json
import os
import postgres_config
import psycopg2
import psycopg2.extras as extras
import time
import subprocess
import sys

def convert_tweet_dict_to_tuple(tweet_dict):
    tweet_data_list = []
    
    for field_dict in field_map():
        for field_data in field_dict.values():
            tweet_data_list.append(tweet_dict[field_data['database_field']])
            
    return tuple(tweet_data_list)

def convert_tweet_json_to_dict(tweet_json):
    tweet_dict = {}
    
    for field_dict in field_map():
        for field_map_data in field_dict.values():
            if 'expression' in field_map_data:
                tweet_dict[field_map_data['database_field']] = eval(field_map_data['expression'])
            else:
                tweet_dict[field_map_data['database_field']] = extract_value_from_json(tweet_json, field_map_data['json_path'], field_map_data['default_value'])
            
    return tweet_dict
            
def display_sleep_message(sleep_duration):
    display_timestamped_message.display_timestamped_message('Sleeping ' + str(sleep_duration) + ' seconds...')
    time.sleep(sleep_duration)
    
def extract_value_from_json(json, json_path_list, default_value):
    new_json_path_list = json_path_list
    current_json_node = new_json_path_list.pop(0)
    
    if current_json_node in json:
        new_json = json[current_json_node]
        
        if len(new_json_path_list) < 1:
            return new_json
        else:
            return extract_value_from_json(new_json, new_json_path_list, default_value)
    else:
        return default_value

def insert_tweet_rows_into_database(tweet_data_rows):
    display_timestamped_message.display_timestamped_message('Inserting batch of ' + str(len(tweet_data_rows)) + ' tweets...')
    
    try:
        connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
        cursor = connection.cursor()
        sql = "INSERT INTO tweet ("
        tweet_field_names = []
        
        for field_dict in field_map():
            for field_map_data in field_dict.values():
                tweet_field_names.append(field_map_data['database_field'])
        
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
        
        display_timestamped_message.display_timestamped_message('Batch insert complete')
        
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
    script_start_time = datetime.datetime.now()
    display_timestamped_message.display_timestamped_message('Loading tweets into postgres database from jsonl file...')
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
    display_timestamped_message.display_timestamped_message('Reading tweets from file...')
    
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
                    display_timestamped_message.display_timestamped_message('Halting due to reaching max tweet load limit specified: ' + str(processed_tweets_count) + '/' + str(tweet_quantity))
                    break
                
    if current_batch_size > 0:
        insert_tweet_rows_into_database(current_batch_data)
                
    display_timestamped_message.display_timestamped_message('Total tweets processed: ' + str(processed_tweets_count))
    display_script_duration.display_script_duration(script_start_time)
    
def postgresql_service_is_running():
    postgresql_service_status = os.system('service postgresql status')
    
    return postgresql_service_status == 0
    
def start_postgresql_service():
    if postgresql_service_is_running():
        display_timestamped_message.display_timestamped_message('Postgresql service is running.')
    else:
        display_timestamped_message.display_timestamped_message('Postgresql service is not running.')
        display_timestamped_message.display_timestamped_message('Starting postgresql service...')
        command_output = subprocess.call(['sudo', 'service', 'postgresql', 'start'])
        print(command_output)
        
def field_map():
    """Map database fields to JSON entities from the Twitter API."""
    
    field_map = [
        {'tweet.status_id': {
            'database_table': 'tweet',
            'database_field': 'status_id',
            'json_entity': 'tweet',
            'json_path': ['id'],
            'default_value': None
        }},
        {'tweet.text': {
            'database_table': 'tweet',
            'database_field': 'text',
            'json_entity': 'tweet',
            'json_path': ['text'],
            'default_value': None
        }},
        {'tweet.created_at_str': {
            'database_table': 'tweet',
            'database_field': 'created_at_str',
            'json_entity': 'tweet',
            'json_path': ['created_at'],
            'default_value': None
        }},
        {'tweet.language': {
            'database_table': 'tweet',
            'database_field': 'language',
            'json_entity': 'tweet',
            'json_path': ['lang'],
            'default_value': None
        }},
        {'tweet.user_screen_name': {
            'database_table': 'tweet',
            'database_field': 'user_screen_name',
            'json_entity': 'tweet',
            'json_path': ['user', 'screen_name'],
            'default_value': None
        }},
        {'tweet.is_retweet': {
            'database_table': 'tweet',
            'database_field': 'is_retweet',
            'json_entity': 'tweet',
            'json_path': None,
            'default_value': None,
            'expression': '"retweeted_status" in tweet_json'
        }},
        {'tweet.retweeted_status_id': {
            'database_table': 'tweet',
            'database_field': 'retweeted_status_id',
            'json_entity': 'tweet',
            'json_path': ['retweeted_status', 'id'],
            'default_value': None
        }},
        {'tweet.retweeted_status_retweet_count': {
            'database_table': 'tweet',
            'database_field': 'retweeted_status_retweet_count',
            'json_entity': 'tweet',
            'json_path': ['retweeted_status', 'retweet_count'],
            'default_value': None
        }},
        {'tweet.retweeted_status_text': {
            'database_table': 'tweet',
            'database_field': 'retweeted_status_text',
            'json_entity': 'tweet',
            'json_path': ['retweeted_status', 'text'],
            'default_value': None
        }},
        {'tweet.retweeted_status_user_screen_name': {
            'database_table': 'tweet',
            'database_field': 'retweeted_status_user_screen_name',
            'json_entity': 'tweet',
            'json_path': ['retweeted_status', 'user', 'screen_name'],
            'default_value': None
        }},
        {'tweet.retweeted_status_language': {
            'database_table': 'tweet',
            'database_field': 'retweeted_status_language',
            'json_entity': 'tweet',
            'json_path': ['retweeted_status', 'lang'],
            'default_value': None
        }}
    ]
    
    return field_map
                
load_tweets_into_postgres()
