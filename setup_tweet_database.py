import postgres_config
import psycopg2
import setup_tweet_table

try:
    setup_tweet_table.setup_tweet_table()
except Exception as ex:
    print('Error occurred while attempting to connect to postgres database.')
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message)
