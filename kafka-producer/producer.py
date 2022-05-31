from kafka.errors import KafkaError
from kafka import KafkaProducer
from time import sleep
import pandas as pd
import boto3
import json

BUCKET = 'stocks-process'
BOOTSTRAP_SERVER = '172.31.85.15:9092'
TOPIC = 'stocks-process'

def getData(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    response = obj.get()
    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        data = pd.read_csv(response['Body']) 
        return data
    else:
        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

def produce(bootstrap_server, topic, data):
    producer = KafkaProducer(bootstrap_servers = bootstrap_server,
            value_serializer= lambda x: x.encode('utf-8'))
    for i, row in data.iterrows():
        r = row.to_json()
        response = producer.send(topic, value=r)
        print(f"data {i} send to queue")
        sleep(1)

def main():
    key = 'input/SPY_TICK_TRADE.csv'
    data = getData(BUCKET, key)
    produce(BOOTSTRAP_SERVER, TOPIC, data)

if __name__ == "__main__":
    main()

