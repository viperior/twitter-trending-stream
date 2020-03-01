import json
import time
import nltk
from collections import Counter

def read_tweets_from_jsonl(jsonl_file_path):
    token_counter = Counter()
    token_ignore_list_under = [':', '@', 'RT', '.', 'the', ',', 'coronavirus', '#', 'a', 'to', 'https', 'in', 'of', '’', 'is', 'and', 'for', 'I', '?', 'de', 'have', '!', 'not', 'that', 'you', 'are', 'que', 'this', 'but', 's', 'from', 'on', 'el', '2', 'about', 'en', 'were', 'has', 'there', 'been', '...', 'be', 'their', 'going', 'who', '“', '(', '\'s', 'as', 'do', 'la', 'your', 'was', 'with', 'no', 'it', '``', '-', 'y', ';', 't', ')', '\'\'', 'being', 're', '&', 'del', 'because', 'an', 'just', 'at', 'we', 'por', 'se', 'other', 'n\'t', 'un', '…']
    token_ignore_list = []
    
    for token in token_ignore_list_under:
        token_ignore_list.append(token.upper())
    
    with open(jsonl_file_path, 'r') as file:
        for index, line in enumerate(file):
            tweet_dict = json.loads(line)
            tokens = nltk.word_tokenize(tweet_dict['text'])
            
            for token in tokens:
                if not token.upper() in token_ignore_list:
                    token_counter[token] += 1
                    
            if index % 10000 == 0:
                print('============================================================')
                print(token_counter.most_common(300))
            
read_tweets_from_jsonl('./data/covid19/covid19_tweets.jsonl')
