from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext


if __name__ == "__main__":
    sc = SparkContext("local", "wordcount")
    #sc.setLogLevel(logLevel="OFF")
    ssc = StreamingContext(sc, 20)
    #ssc.checkpoint("c:\Playground\spark\logs")
    brokers, topic = sys.argv[1:]
 
    kvs = KafkaUtils.createDirectStream(ssc, ["events.noflguid"],{"metadata.broker.list":"ec2-52-203-200-3.compute-1.amazonaws.com:9092"})

    print(str(brokers))
    print(str(topic))
  
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    print(kvs.count())
    ssc.start()
    ssc.awaitTermination()
