# Extract one or more sample tweets from a jsonl file by line number.

import json
import sys

def erase_file_contents(file_path):
    print(file_path)
    input_is_valid = False
    
    while not input_is_valid:
        user_confirmed_deletion = input('Are you sure you want to delete this file? y/n\n')
        
        if user_confirmed_deletion == 'y':
            input_is_valid = True
            open(file_path, 'w').close()
            print('File deleted: ' + file_path)
        elif user_confirmed_deletion == 'n':
            input_is_valid = True
            print('File deletion aborted.')
        else:
            print('Invalid command. Please type "y" or "no" and press enter to confirm file deletion.')

def extract_sample_tweets_from_json():
    script_parameter_count = len(sys.argv) - 1
    expected_parameter_count = 3
    
    if script_parameter_count < expected_parameter_count:
        print('Script is missing paramters. ' + str(script_parameter_count) + ' provided. ' + str(expected_parameter_count) + ' expected.')
        print('Parameters: (tweet_jsonl_source_file_path, tweet_jsonl_target_file_path, tweet_quantity)')
        return False
    
    tweet_jsonl_source_file_path = sys.argv[1]
    tweet_jsonl_target_file_path = sys.argv[2]
    tweet_quantity = int(sys.argv[3])
    
    erase_file_contents(tweet_jsonl_target_file_path)
    
    with open(tweet_jsonl_source_file_path, 'r') as source_file:
        for index, line in enumerate(source_file):
            if (index + 1) > tweet_quantity:
                break
            
            tweet_json = json.loads(line)
            
            with open(tweet_jsonl_target_file_path, 'a') as target_file:
                target_file.write(line)
                
    return True
            
extract_sample_tweets_from_json()
