from kafka import KafkaConsumer
from json import loads
import argparse

BOOTSTRAP_SERVER = '172.31.85.15:9092'
GROUP_ID = 'stocks-group'
TOPIC = 'stocks-process'

def init_argparse():
    parser = argparse.ArgumentParser(
            description="Kafka's messages consumer, that shows an alert when price is below or above desire limits.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0.0')

    parser.add_argument('min', type=float, help="min value to activate alert when price drops below this.")
    parser.add_argument('max', type=float, help="max value to activate alert when price go up obove this.")

    return parser

def init_consumer(topic, bootstrap_server, group_id):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: loads(x.decode('utf-8')))
    return consumer

def consume(consumer):
    for message in consumer:
        yield message.value

def process_data(data, min_price, max_price):
    data = loads(data)
    price = data['PRICE']
    if price < min_price:
        print(f"ALERT:\tprice {price} below min price {min_price}!!!")
    if price > max_price:
        print(f"ALERT:\tprice {price} above max price {max_price}!!!")
    

def main(min_price, max_price):
    consumer = init_consumer(TOPIC, BOOTSTRAP_SERVER, GROUP_ID)
    for data in consume(consumer):
        process_data(data, min_price, max_price)
    


if __name__ == '__main__':
    parser = init_argparse()
    args = parser.parse_args()
    main(args.min, args.max)
