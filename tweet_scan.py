import tweet_scan_config
import datetime
import time
import importlib

def tweet_scan_worker():
    worker_is_active = True
    
    while worker_is_active:
        print(datetime.datetime.utcnow())
        importlib.reload(tweet_scan_config)
        print(tweet_scan_config.tweet_scan_config('data_directory'))
        worker_is_active = tweet_scan_config.tweet_scan_config('worker_is_active')
        time.sleep(1)

tweet_scan_worker()
