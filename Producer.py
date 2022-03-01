import requests
from kafka import KafkaProducer
import os
import csv
from dotenv import load_dotenv


load_dotenv('.env')

Key = os.getenv('API')

URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&slice=year1month1&apikey={}'.format(Key)

topic = "StockStream"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer= lambda x: x.encode('utf-8'))

#we are converting to csv first, and then sending them after encoding to json format.
#this is done so that we could send the data in a proper format and in rows.

with requests.Session() as s:
    downloaded = s.get(URL)
    rows = downloaded.content.decode('utf-8')
    cr = csv.reader(rows.splitlines(), delimiter=',')
    my_list = list(cr)
    del my_list[0]

    for row in my_list:
        message = ",".join(row)
        print(message)
        producer.send(topic,message)

    



