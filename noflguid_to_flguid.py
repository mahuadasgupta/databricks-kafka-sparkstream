from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import kafka
import json
# from pyspark.streaming.kafka import Kafka
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import SimpleProducer, KafkaClient

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def handler(message):
    records = message.collect()
    for record in records:
        producer.send('events.flguid', str(record))
        producer.flush()

def main():
    sc = SparkContext("local", "wordcount")
    ssc = StreamingContext(sc, 10)
    sc.setLogLevel(logLevel="OFF")
  #  brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc,["events.noflguid"],{"metadata.broker.list": "localhost:9092"})

    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.foreachRDD(handler)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
