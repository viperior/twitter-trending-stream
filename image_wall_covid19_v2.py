import json
import time
import nltk
from collections import Counter
from dominate import document
from dominate.tags import *

def read_tweets_from_jsonl(jsonl_file_path):
    image_url_counter = Counter()
    media_tweets = []
    
    with open(jsonl_file_path, 'r') as file:
        for index, line in enumerate(file):
            if index > 10000:
                break
            
            tweet_dict = json.loads(line)
            
            if('media' in tweet_dict['entities']):
                image_url = tweet_dict['entities']['media'][0]['media_url_https']
                image_url_counter[image_url] += 1
            
    # Find most popular tweet text for each image url
    top_images = image_url_counter.most_common(7)
    image_captions = {}
    
    for image in top_images:
        current_image_caption_counter = Counter()
        current_media_tweet = []
        current_media_tweet.append(image[0])
        
        with open(jsonl_file_path, 'r') as file:
            for index, line in enumerate(file):
                tweet_dict = json.loads(line)
                current_tweet_is_media_tweet = 'media' in tweet_dict['entities']
                
                if current_tweet_is_media_tweet:
                    current_image_caption_counter[tweet_dict['text']] += 1
                
        print('Image URL: ' + image[0])
        print('Caption: ' + current_image_caption_counter.most_common(1)[0][0])
        
        if index > 10000:
            break
        
    # Create HTML file
    with document(title='Photos') as doc:
        with doc.head:
            link(rel='stylesheet', href='covid19_slideshow.css')
        
        h1('Photos')
        
        for tweet in media_tweets:
          div(img(src=tweet[0], alt=tweet[1], title=tweet[1]), _class='photo')
          
        script(type='text/javascript', src='covid19_slideshow.js')
    
        with open('output/covid19_slideshow_v2.html', 'w') as f:
            f.write(doc.render())
            
read_tweets_from_jsonl('./data/covid19/covid19_tweets.jsonl')
