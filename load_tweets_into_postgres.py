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
    tweet_dict['is_retweet'] = 'retweeted_status' in tweet_json
    
    # Add:
        # Media type
        # Media URL
        # Retweet flag
        # Retweeted status id
    
    return tweet_dict
    
def load_tweets_into_postgres():
    script_parameter_count = len(sys.argv) - 1
    expected_parameter_count = 1
    
    if script_parameter_count < expected_parameter_count:
        print('Script is missing paramters. ' + str(script_parameter_count) + ' provided. ' + str(expected_parameter_count) + ' expected.')
        print('Parameters: (tweet_jsonl_file_path, tweet_quantity [optional])')
        return False
    
    tweet_jsonl_file_path = sys.argv[1]
    tweet_quantity_is_limited = False
    
    if script_parameter_count > 1:
        tweet_quantity_is_limited = True
        tweet_quantity = int(sys.argv[2])
    
    # Process up to 100 tweets at a time, adding them to a dictionary.
    # Insert tweets that are not already stored in the database.
    with open(tweet_jsonl_file_path, 'r') as file:
        for index, line in enumerate(file):
            tweet_json = json.loads(line)
            tweet_dict = convert_tweet_json_to_dict(tweet_json)
            
            if tweet_quantity_is_limited and index >= tweet_quantity:
                break
            
            print(tweet_dict)
            
            attributes_to_inspect = [
                'status_id',
                'text',
                'created_at_str',
                'is_retweet'
            ]
            
            for attribute in attributes_to_inspect:
                print('Attribute name: ' + attribute + '; Type: ' + str(type(tweet_dict[attribute])) + '; Value: ' + str(tweet_dict[attribute]))
            
            i = 3
            
            while i > 0:
                print(str(i) + '...')
                time.sleep(1)
                i -= 1
    
            try:
                connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
                cursor = connection.cursor()
                
                sql = "INSERT INTO tweet (status_id, created_at_str, is_retweet, text) VALUES (%s, %s, %s, %s);"
                data = (
                    tweet_dict['status_id'],
                    tweet_dict['created_at_str'],
                    tweet_dict['is_retweet'],
                    tweet_dict['text']
                )
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
