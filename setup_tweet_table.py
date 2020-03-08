import postgres_config
import psycopg2

def setup_tweet_table():
    try:
        connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
        cursor = connection.cursor()
        
        sql = """CREATE TABLE IF NOT EXISTS tweet (
                    tweet_id bigserial PRIMARY KEY,
                    status_id bigint NOT NULL,
                    created_at_str varchar(30) NOT NULL,
                    language_code int,
                    is_retweet boolean NOT NULL,
                    retweet_status_id bigint,
                    text varchar(600) NOT NULL
                 );"""
        cursor.execute(sql)
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as ex:
        print('Error occurred while attempting to connect to postgres database.')
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
