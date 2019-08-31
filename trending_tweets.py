import twittercredentials, time
from twarc import Twarc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.style as style
import json

def convert_twarc_tweet_data_to_dict(tweets, search_query):
  tweets_dict = {}
  sentinel = object()
  tweet = next(tweets, sentinel)

  while tweet is not sentinel:
    tweet_dict = {}
    tweet_dict['tweet_json_data'] = tweet
    tweet_dict['search_query'] = search_query
    tweets_dict[tweet['id']] = tweet_dict
    tweet = next(tweets, sentinel)
    
  return tweets_dict

def get_trending_topics(woeid = 1):
  trends = get_twitter_trends(woeid)
  trends_list = []
  
  for trend in trends:
    if trend['tweet_volume'] is not None:
      current_trend = []
      current_trend.append(trend['name'])
      current_trend.append(trend['tweet_volume'])
      current_trend.append(trend['query'])
      trends_list.append(current_trend)
      
  trends_list = sort_nested_list_by_element_index(trends_list, 1, 'desc')
  
  return trends_list

def get_trending_topics_chart(trends):
  return get_vertical_bar_chart_plot(trends, 'Trending Twitter Topics', 'Topic', 'Tweet Volume')

def get_trending_topic_tweets(woeid = 1, result_type = 'popular', max_pages_per_topic = 1, max_topics_to_fetch = 100, fetch_topic_tweets = False, max_tweets_per_topic = 100):
  # Get Twitter trending topics.
  trends_list = get_trending_topics(woeid)

  # Reduce the list to the number of maximum topics specified.
  trends_list = trends_list[:max_topics_to_fetch]
  
  # Re-sort the truncated topic list.
  trends_list = sort_nested_list_by_element_index(trends_list, 1)
  
  # Fetch tweets for trending topics
  if fetch_topic_tweets:
    trending_tweets_data = {}
    i = 0
    
    for trend in trends_list:
      tweet_batch = {}
      trend_query = trend[2]
      trend_query_name = trend[0]
      tweets = get_twarc_session().search(q = trend_query, result_type = result_type, max_pages = max_pages_per_topic)
      tweets_dict = convert_twarc_tweet_data_to_dict(tweets, trend_query_name)
      trending_tweets_data[i] = tweets_dict
      i += 1
    
    # Save tweets to file.
    save_tweet_data_to_json_file(trending_tweets_data, './data/tweets.json')
  
  # Generate trending topics plot and save to file.
  trends_list_2d = remove_last_element_of_children_in_nested_list(trends_list)
  plot = get_trending_topics_chart(trends_list_2d)
  filename = 'twitter_topics_by_tweet_volume_bar_chart.svg'
  save_trending_topics_chart_to_file(plot, filename)
    
def get_twarc_session():
  return Twarc(twittercredentials.twitter_consumer_key(), twittercredentials.twitter_consumer_secret(), twittercredentials.twitter_access_token(), twittercredentials.twitter_access_token_secret())

def get_twitter_trends(woeid = 1):
  return get_twarc_session().trends_place(woeid)[0]['trends']
  
def get_vertical_bar_chart_plot(data, title, dimension_label, measure_label):
  style.use('fivethirtyeight')
  
  # Assemble the pandas data frame and matplotlib chart.
  chart_labels = [dimension_label, measure_label]
  df = pd.DataFrame.from_records(data, columns = chart_labels)
  ax = df.plot.barh(x = dimension_label, y = measure_label, rot = 0)
  ax.set_xlabel(measure_label)
  ax.get_legend().remove()
  ax.tick_params(axis = 'both', which = 'major', labelsize = 9)
  ax.set_title(title)
  
  # Add more left margin for long topic names.
  plt.subplots_adjust(bottom = 0.35, left = 0.35)
  
  # Rotate x-axis labels 90 degrees.
  plt.xticks(rotation=90)
  
  return plt
  
def remove_last_element_of_children_in_nested_list(nested_list):
  new_list = []
  
  for child in nested_list:
    removed_child = child.pop()
    new_list.append(child)
    
  return new_list

def save_trending_topics_chart_to_file(plot, filename):
  plot.savefig(filename)

def save_tweet_data_to_json_file(tweets_dict, filename):
  with open(filename, 'w') as json_file:
    json.dump(tweets_dict, json_file)

def sort_nested_list_by_element_index(unsorted_list, element_index, sort_order = 'asc'):
  reverse_value = sort_order == 'desc'
  unsorted_list.sort(key=lambda x: int(x[element_index]), reverse = reverse_value)
  return unsorted_list

get_trending_topic_tweets(max_topics_to_fetch = 15, max_pages_per_topic = 1, max_tweets_per_topic = 100, woeid = 23424977, fetch_topic_tweets = True)
