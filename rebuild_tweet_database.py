import datetime
import display_script_duration
import display_timestamped_message
import psycopg2
import postgres_config
import setup_tweet_database

def rebuild_tweet_database():
    script_start_time = datetime.datetime.now()
    
    try:
        display_timestamped_message.display_timestamped_message('Connecting to postgresql server...')
        connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
        connection.autocommit = True
        cursor = connection.cursor()
        
        display_timestamped_message.display_timestamped_message('Dropping tweets table...')
        sql = """DROP TABLE IF EXISTS tweet;"""
        cursor.execute(sql)
        
        cursor.close()
        connection.close()
    except Exception as ex:
        print('Error occurred while attempting to rebuild tweet database.')
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        
    display_timestamped_message.display_timestamped_message('Rebuilding database...')
    setup_tweet_database.setup_tweet_database()
    display_script_duration.display_script_duration(script_start_time)
    
rebuild_tweet_database()
