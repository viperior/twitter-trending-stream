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
    
    print(tweet_dict)
    
    i = 10
    
    while i > 0:
        print(str(i) + '...')
        time.sleep(1)
        i -= 1
    
    return tweet_dict
    
def load_tweets_into_postgres():
    tweet_jsonl_file_path = sys.argv[1]
    
    # Process up to 100 tweets at a time, adding them to a dictionary.
    # Insert tweets that are not already stored in the database.
    with open(tweet_jsonl_file_path, 'r') as file:
        for line in file:
            tweet_json = json.loads(line)
            tweet_dict = convert_tweet_json_to_dict(tweet_json)
    """
    try:
        connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
        cursor = connection.cursor()
        
        sql = \"\"\"SQL CODE
                 ;\"\"\"
        cursor.execute(sql)
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as ex:
        print('Error occurred while attempting to connect to postgres database.')
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)"""
        
load_tweets_into_postgres()
