import json
from dominate import document
from dominate.tags import *

media_tweets = []

with open("data/covid19/covid19_tweets.jsonl", "r") as file:
    for line in file:
        tweet_data = json.loads(line)
      
        if('media' in tweet_data['entities']):
            media_entity = tweet_data['entities']['media'][0]
            image_url = media_entity['media_url_https']
            image_alt_text = tweet_data['text']
            media_tweet = [image_url, image_alt_text]
            media_tweets.append(media_tweet)

with document(title='Photos') as doc:
    h1('Photos')
    
    for tweet in media_tweets:
      div(img(src=tweet[0], alt=tweet[1], title=tweet[1]), _class='photo')

with open('output/gallery.html', 'w') as f:
    f.write(doc.render())
