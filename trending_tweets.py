import twittercredentials, time
from twarc import Twarc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def get_trending_topic_tweets(woeid = 1, result_type = 'popular', max_pages_per_topic = 1, max_topics_to_fetch = 100, fetch_topic_tweets = False, max_tweets_per_topic = 100, slow_output = False, verbose_output = False):
  print('Getting trending Twitter topics...')
  
  twarc_session = Twarc(twittercredentials.twitter_consumer_key(), twittercredentials.twitter_consumer_secret(), twittercredentials.twitter_access_token(), twittercredentials.twitter_access_token_secret())
  trends = get_twitter_trends(twarc_session, woeid)
  trends_list = []
  
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
    
    while fetch_topic_tweets and (tweets_fetched < max_tweets_per_topic):
      tweet = next(tweets, sentinel)
      if tweet is sentinel:
        break
      
      if verbose_output:
        print('\t' + tweet['user']['name'] + ': "' + tweet['full_text'] + '" (Retweeted ' + str(tweet['retweet_count']) + 'X, favorited ' + str(tweet['favorite_count']) + 'X)')
      
      if slow_output:
        time.sleep(0.5)
        
      tweets_fetched += 1
      
    if verbose_output:
      print("")
    
    if slow_output:
      time.sleep(1)
  
  # Sort the list of trending Twitter topics by tweet volume.    
  trends_list.sort(key=lambda x: int(x[1]), reverse = True)
  
  # Reduce the list to the number of maximum topics specified.
  trends_list = trends_list[:max_topics_to_fetch]
  
  # Resort the truncated topic list.
  trends_list.sort(key=lambda x: int(x[1]), reverse = False)
  
  # Assemble the pandas data frame and matplotlib chart.
  labels = ['Topic', 'Tweet Volume']
  df = pd.DataFrame.from_records(trends_list, columns=labels)
  ax = df.plot.barh(x='Topic', y='Tweet Volume', rot=0)
  ax.set_xlabel('Tweet Volume')
  ax.set_title('Trending Twitter Topics')
  plt.savefig('twitter_topics_by_tweet_volume_bar_chart.svg')
      
def get_twitter_trends(twarc_session, woeid = 1):
  return twarc_session.trends_place(woeid)[0]['trends']

get_trending_topic_tweets(max_topics_to_fetch = 8, max_tweets_per_topic = 5, slow_output = False, verbose_output = False)
