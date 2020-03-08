import postgres_config
import psycopg2

connection = psycopg2.connect(host = 'localhost', database = postgres_config.db_name(), user = postgres_config.db_user(), password = postgres_config.db_password())
cursor = connection.cursor()

sql = 'select * from tweet;'
cursor.execute(sql)

print("The number of parts: ", cursor.rowcount)
row = cursor.fetchone()

while row is not None:
    print(row)
    row = cursor.fetchone()

cursor.close()
connection.close()
