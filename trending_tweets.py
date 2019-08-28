import twittercredentials, time
from twarc import Twarc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.style as style
import json

def get_trending_topic_tweets(woeid = 1, result_type = 'popular', max_pages_per_topic = 1, max_topics_to_fetch = 100, fetch_topic_tweets = False, max_tweets_per_topic = 100, slow_output = False, verbose_output = False):
  print('Getting trending Twitter topics...')
  
  twarc_session = Twarc(twittercredentials.twitter_consumer_key(), twittercredentials.twitter_consumer_secret(), twittercredentials.twitter_access_token(), twittercredentials.twitter_access_token_secret())
  trends = get_twitter_trends(twarc_session, woeid)
  trends_list = []
  tweets_dict = {}
  
  # Sort the list of trending Twitter topics by tweet volume.    
  trends_list.sort(key=lambda x: int(x[1]), reverse = True)
  
  # Reduce the list to the number of maximum topics specified.
  trends_list = trends_list[:max_topics_to_fetch]
  
  # Resort the truncated topic list.
  trends_list.sort(key=lambda x: int(x[1]), reverse = False)
  
  for trend in trends:
    current_trend = []
    
    if trend['tweet_volume'] is not None:
      current_trend.append(trend['name'])
      current_trend.append(trend['tweet_volume'])
      trends_list.append(current_trend)
    
    trend_query = trend['query']
    
    if verbose_output:
      print('Trending topic: "' + trend['name'] + '" | Tweet volume: ' + str(trend['tweet_volume']))
    
    if slow_output:
      time.sleep(1)
    
    tweets = twarc_session.search(q = trend_query, result_type = result_type, max_pages = max_pages_per_topic)
    tweets_fetched = 0
    sentinel = object()
    
    # Fetch tweets
    while fetch_topic_tweets and (tweets_fetched < max_tweets_per_topic):
      tweet = next(tweets, sentinel)
      if tweet is sentinel:
        break
      
      # Add originating query to current tweet dictionary object.
      tweet['query'] = trend_query
      
      # Add current tweet to tweet dictionary.
      tweets_dict[tweet['id']] = tweet
      
      if verbose_output:
        print('\t' + tweet['user']['name'] + ': "' + tweet['full_text'] + '" (Retweeted ' + str(tweet['retweet_count']) + 'X, favorited ' + str(tweet['favorite_count']) + 'X)')
      
      if slow_output:
        time.sleep(0.5)
        
      tweets_fetched += 1
      
    if verbose_output:
      print("")
    
    if slow_output:
      time.sleep(1)
  
  # Use the fivethirtyeight style
  style.use('fivethirtyeight')
  
  # Assemble the pandas data frame and matplotlib chart.
  chart_labels = ['Topic', 'Tweet Volume']
  df = pd.DataFrame.from_records(trends_list, columns = chart_labels)
  ax = df.plot.barh(x='Topic', y='Tweet Volume', rot=0)
  ax.set_xlabel('Tweet Volume')
  ax.get_legend().remove()
  ax.tick_params(axis='both', which='major', labelsize = 9)
  ax.set_title('Trending Twitter Topics')
  
  # Add more left margin for long topic names.
  plt.subplots_adjust(bottom = 0.35, left = 0.35)
  
  # Rotate x-axis labels 90 degrees.
  plt.xticks(rotation=90)

  # Save plot to image file.
  plt.savefig('twitter_topics_by_tweet_volume_bar_chart.svg')
  
  # Save tweets to file.
  with open('./data/tweets.json', 'w') as json_file:
    json.dump(tweets_dict, json_file)
      
def get_twitter_trends(twarc_session, woeid = 1):
  return twarc_session.trends_place(woeid)[0]['trends']

get_trending_topic_tweets(max_topics_to_fetch = 1, max_tweets_per_topic = 1, woeid = 23424977, fetch_topic_tweets = True, slow_output = False, verbose_output = True)
