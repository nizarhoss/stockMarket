import psaw
import psycopg2
import psycopg2.extras
from psaw import PushshiftAPI
import datetime
import config


connection = psycopg2.connect("dbname='stockmarket' user='postgres' host='localhost' password='admin12345' port='5430'")

cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

cursor.execute("""SELECT * from stock;
""")
rows = cursor.fetchall()
stocks= {}
for row in rows:
    stocks['$'+ row['symbol']] =row['id']

print(stocks)


api = PushshiftAPI()

start_time = int(datetime.datetime(2021,2,12).timestamp())

submissions = api.search_submissions(after=start_time,
                                    subreddit='pennystocks',
                                    filter=['url','author','title','subreddit'])

for submission in submissions:
    #    print(submission.created_utc)
        
    

    words= submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))

    if len(cashtags) >0:
        # print(cashtags)
        # print(submission.title)
        # print(submission.url)

        for cashtag in cashtags:

            submitted_time= datetime.datetime.fromtimestamp(submission.created_utc).isoformat()
            try:
                cursor.execute("""INSERT into mention (dt,stock_id,message,source,url) 
                VALUES (%s,%s,%s,'pennystocks',%s)
                """, (submitted_time,stocks[cashtag], submission.title,submission.url ))
                
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()

