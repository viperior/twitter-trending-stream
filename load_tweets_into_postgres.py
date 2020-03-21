# Load a set of tweets residing in a jsonl file into a postgres database.

import postgres_config
import psycopg2
import sys
import json
import time

def convert_tweet_json_to_dict(tweet_json):
    tweet_dict = {}
    
    tweet_dict['status_id'] = tweet_json['id']
    tweet_dict['text'] = tweet_json['text']
    tweet_dict['created_at_str'] = tweet_json['created_at']
    
    if 'retweeted_status' in tweet_json:
        tweet_dict['is_retweet'] = True
        tweet_dict['retweet_status_id'] = tweet_json['retweeted_status']['id']
    else:
        tweet_dict['is_retweet'] = False
    
    if 'lang' in tweet_json:
        tweet_dict['language_code'] = tweet_json['lang']
    
    # Add:
        # Media type
        # Media URL
    
    return tweet_dict
    
def load_tweets_into_postgres():
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
    
    # Process up to 100 tweets at a time, adding them to a dictionary.
    # Insert tweets that are not already stored in the database.
    with open(tweet_jsonl_file_path, 'r') as file:
        for index, line in enumerate(file):
            if tweet_quantity_is_limited and index >= tweet_quantity:
                break
            
            tweet_json = json.loads(line)
            tweet_dict = convert_tweet_json_to_dict(tweet_json)
            
            if display_tweet_data_is_specified:
                print(tweet_dict)
                
                for attribute in tweet_dict.keys():
                    print('Attribute name: ' + attribute + '; Type: ' + str(type(tweet_dict[attribute])) + '; Value: ' + str(tweet_dict[attribute]))
            
            if delay_time_between_tweets_is_specified:
                print('Sleeping ' + str(delay_time_between_tweets) + ' seconds...')
                time.sleep(delay_time_between_tweets)
    
            try:
                connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
                cursor = connection.cursor()
                
                sql = "INSERT INTO tweet ("
                data_list = []
                
                for index, key in enumerate(tweet_dict.keys()):
                    sql += key
                    data_list.append(tweet_dict[key])
                    
                    if index + 1 < len(tweet_dict):
                        sql += ','
                        
                sql += ') VALUES ('
                data = tuple(data_list)
                        
                for index, value in enumerate(range(len(tweet_dict))):
                    sql += '%s'
                    
                    if index + 1 < len(tweet_dict):
                        sql += ','
                        
                sql += ');'
                
                cursor.execute(sql, data)
                connection.commit()
                
                cursor.close()
                connection.close()
            except Exception as ex:
                print('Error occurred while attempting to connect to postgres database.')
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(message)
        
load_tweets_into_postgres()
