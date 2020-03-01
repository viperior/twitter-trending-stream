def tweet_scan_config(attribute):
    tweet_scan_config = {}
    
    tweet_scan_config['worker_is_active'] = True
    tweet_scan_config['data_directory'] = 'tweet_scan_test'
    
    return tweet_scan_config[attribute]
