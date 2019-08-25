import twittercredentials, time
from twarc import Twarc

print("Getting trending Twitter topics...")

t = Twarc(twittercredentials.twitter_consumer_key(), twittercredentials.twitter_consumer_secret(), twittercredentials.twitter_access_token(), twittercredentials.twitter_access_token_secret())

woeid = 1
trends = t.trends_place(woeid)
trends_list = trends[0]["trends"]

for trend in trends_list:
  trend_query = trend["query"]
  print("Trending topic: \"" + trend["name"] + "\" | Tweet volume: " + str(trend["tweet_volume"]))
  time.sleep(1)
  
  tweets = t.search(q = trend_query, result_type = "popular", max_pages = 1)
  
  for tweet in tweets:
    print("\t" + tweet["user"]["name"] + ": \"" + tweet["full_text"] + "\" (Retweeted " + str(tweet["retweet_count"]) + "X, favorited " + str(tweet["favorite_count"]) + "X)")
    time.sleep(0.5)
    
  print("")
  time.sleep(1)
