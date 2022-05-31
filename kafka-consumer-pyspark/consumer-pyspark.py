from pyspark.sql import SparkSession
from pyspark.sql.functions import decode, json_tuple, window
from pyspark.sql.functions import avg
from json import loads

BOOTSTRAP_SERVER = '172.31.85.15:9092'
GROUP_ID = 'stocks-group'
TOPIC = 'stocks-process'
PATH = 's3://stocks-process/output/averages/'
CHECKPOINT_PATH = 's3://stocks-process/checkpoint/averages/'

def init_consumer(topic, bootstrap_server, group_id):
    spark = SparkSession.builder \
            .appName("stocks-process") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()

    kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_server) \
            .option("subscribe", topic) \
            .option('includeTimestamp', 'true') \
            .load()

    return kafka_df


def process_data(df):
    windowSize = "5 seconds"
    slideSize = "3 seconds"
    values = df.select(decode('value', 'utf-8').alias('value'), 'timestamp')
    df = values.select(json_tuple('value',
        'TIME', 'PRICE', 'SIZE', 'EXCHANGE',
        'SALE_CONDITION', 'SUSPICIOUS'), 'timestamp')
    df = df.toDF('TIME', 'PRICE', 'SIZE', 'EXCHANGE',
        'SALE_CONDITION', 'SUSPICIOUS', 'timestamp')

    df = df.withColumn('PRICE', df.PRICE.cast('float'))

    windowed_df = df.withWatermark("timestamp", windowSize) \
            .groupBy(window('timestamp', windowSize, slideSize)) \
            .agg(avg('PRICE').alias('averages'))

    windowed_df = windowed_df.select('window.start', 'window.end', 'averages')

    windowed_df.writeStream \
            .outputMode('append') \
            .format('csv') \
            .option('path', PATH) \
            .option('checkpointLocation', CHECKPOINT_PATH) \
            .option('truncate', 'false') \
            .start().awaitTermination()

def main():
    kafka_df = init_consumer(TOPIC, BOOTSTRAP_SERVER, GROUP_ID)
    process_data(kafka_df)



if __name__ == '__main__':
    main()

