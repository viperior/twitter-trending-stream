# twitter-trending-stream
Fetches tweets for trending Twitter topics.

# Trending Topics
![Twitter trending topics bar chart](https://raw.githubusercontent.com/viperior/twitter-trending-stream/master/twitter_topics_by_tweet_volume_bar_chart.svg?sanitize=true)

Chart data last fetched: 2019-03-01

# Feature Roadmap
* Hydration / dehydration features
* Continuous running features, including disk space monitor
* Image wall / slideshow from tweet media
* Additional pass to separately extract retweeted statuses
* Normalize storage of retweeted status data
* Normalize storage of user, media, location, and other dimensional data
* Token storage
* Geocoordinates

# Postgres commands

## Start postgres service
`sudo service postgresql start`

## Login to postgres
`psql postgres`
