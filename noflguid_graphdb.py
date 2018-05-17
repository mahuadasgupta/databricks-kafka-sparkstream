from __future__ import print_function
from pyspark.sql.context import SQLContext

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

from gremlin_python.driver import client, serializer
import sys, traceback

import random


producer = KafkaProducer(bootstrap_servers='localhost:9092')
client = client.Client('wss://sparkkafka.gremlin.cosmosdb.azure.com:443/', 'g',
                       username="/dbs/sparkkafka_db/colls/sparkkafka_graph",
                       password="zRY7IuiIF4mBXdKAiKQlZpdKQvT7v0La8HyGE6DtcQT9wWKHLUWxSniCP4AkiYEbHeeNTIe2c5C92ivleLjbUQ==",
                       message_serializer=serializer.GraphSONSerializersV2d0()
                       )

_gremlin_insert_vertices = []


def handler(message):
    _gremlin_insert_vertices = []
    # rdd = message.map(lambda k : k.split(":"))
    records = message.collect()

    # records = message.collectAsMap()
    # print(records['timestamp'])

    guidfound = False
    key1 = ""
    key2 = ""
    value1 = ""
    value2 = ""
    guidvalue = ""
    initialvalue = ""
    query = ""
    for record in records:
        recordsplit = record.split(":")
        print(recordsplit[0])

        if len(recordsplit) > 1:
            print(recordsplit[1])
            firstpart = recordsplit[0].replace('"', '')
            secondpart = recordsplit[1].strip()
            print ("****") 
            print (firstpart) 
            secondpart = secondpart.replace('"', "")
            if (secondpart.endswith(",")):
                secondpart = secondpart[:-1]

            if (firstpart.strip() == "keys"):
                print ("inser keys section")
                key1 = (secondpart.split(",")[0]).replace("[", "")
                key2 = (secondpart.split(",")[1]).replace("]", "")
            if (firstpart.strip() == "values"):
                value1 = (secondpart.split(",")[0]).replace("[", "")
                value2 = (secondpart.split(",")[1]).replace("]", "")
            if (firstpart.strip() == "guid"):
                guidfound = True
                guidvalue = secondpart[1].strip()
            if(firstpart.strip() == "data"):
                query = "g.addV('Customer').property('" + str(key1.strip()) + "', '" + str(value1) + "')"
                query = query + ".property('" + str(key2.strip()) + "'," + str(value2) + ")"
                if (guidfound):
                    query = query + ".property('guid'," + guidvalue + ")"
                else:
                    query = query + ".property('guid'," + str(generateguid()) + ")"
                _gremlin_insert_vertices.append(query)
                print(query)


    if (len(_gremlin_insert_vertices)>0):
        for qry in _gremlin_insert_vertices:
            print("Record being inserted for query")
            print(qry)
            callback = client.submitAsync(qry)
            if callback.result() is not None:
                print("Record Inserted")
            else:
                print("Something went wrong")


def generateguid():
    random_test = random.randint(100000000000, 999999999999)
    return random_test


def main():
    sc = SparkContext("local", "wordcount")
    sc.setLogLevel(logLevel="OFF")
    ssc = StreamingContext(sc, 20)

    #brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, ["events.noflguid"], {"metadata.broker.list": "localhost:9092"})

    lines = kvs.map(lambda x: x[1])

    lines.foreachRDD(handler)


    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main() 
