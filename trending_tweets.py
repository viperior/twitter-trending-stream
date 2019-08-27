import twittercredentials, time
from twarc import Twarc

def get_trending_topic_tweets(woeid = 1, result_type = 'popular', max_pages_per_topic = 1, max_topics_to_fetch = 100, max_tweets_per_topic = 100, slow_output = False, verbose_output = False):
  print('Getting trending Twitter topics...')
  
  twarc_session = Twarc(twittercredentials.twitter_consumer_key(), twittercredentials.twitter_consumer_secret(), twittercredentials.twitter_access_token(), twittercredentials.twitter_access_token_secret())
  trends_list = get_twitter_trends(twarc_session, woeid, max_topics_to_fetch)
  
  for trend in trends_list:
    trend_query = trend['query']
    
    if verbose_output:
      print('Trending topic: "' + trend['name'] + '" | Tweet volume: ' + str(trend['tweet_volume']))
    
    if slow_output:
      time.sleep(1)
    
    tweets = twarc_session.search(q = trend_query, result_type = result_type, max_pages = max_pages_per_topic)
    tweets_fetched = 0
    
    while tweets_fetched < max_tweets_per_topic:
      tweet = next(tweets)
      
      if verbose_output:
        print('\t' + tweet['user']['name'] + ': "' + tweet['full_text'] + '" (Retweeted ' + str(tweet['retweet_count']) + 'X, favorited ' + str(tweet['favorite_count']) + 'X)')
      
      if slow_output:
        time.sleep(0.5)
        
      tweets_fetched += 1
      
    if verbose_output:
      print("")
    
    if slow_output:
      time.sleep(1)
      
def get_twitter_trends(twarc_session, woeid = 1, max_topics_to_fetch = 100):
  return twarc_session.trends_place(woeid)[0]['trends'][:max_topics_to_fetch]

get_trending_topic_tweets(max_topics_to_fetch = 3, max_tweets_per_topic = 5, slow_output = True, verbose_output = True)
