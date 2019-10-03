import json
from dominate import document
from dominate.tags import *

with open("data/tweets.json", "r") as read_file:
  data = json.load(read_file)
  batch_ids = data.keys()
  media_tweets = []
  
  for batch_id in batch_ids:
    tweet_ids = data[batch_id].keys()
    
    for tweet_id in tweet_ids:
      tweet_data = data[batch_id][tweet_id]['tweet_json_data']
      
      if('media' in tweet_data['entities']):
        media_entity = tweet_data['entities']['media'][0]
        image_url = media_entity['media_url_https']
        image_alt_text = tweet_data['full_text']
        media_tweet = [image_url, image_alt_text]
        media_tweets.append(media_tweet)

with document(title='Photos') as doc:
  h1('Photos')
  
  for tweet in media_tweets:
    div(img(src=tweet[0], alt=tweet[1], title=tweet[1]), _class='photo')

with open('output/gallery.html', 'w') as f:
  f.write(doc.render())
