# -*- coding: utf-8 -*-
import findspark
findspark.init('/home/rafal/spark-2.1.0-bin-hadoop2.7')
from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.streaming import StreamingContext
from textblob import TextBlob
#from pyspark.sql import SparkSession
import json

# my_spark = SparkSession \
#     .builder \
#     .appName("myapp") \
#     .config("spark.mongodb.input.uri","mongodb://127.0.0.1/tweets.tweetsCollection") \
#     .config("spark.mongodb.output.uri","mongodb://127.0.0.1/tweets.tweetsCollection") \
#     .getOrCreate()


def filter_tweets(text):
    if text == "":
        return False
    else:
        return True


def sentyment_func(tweet):
    analysis = TextBlob(tweet)
    sentiment = analysis.sentiment.polarity
    return sentiment


sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 60)  # 10 is the batch interval in seconds
IP = "localhost"
Port = 999
lines = ssc.socketTextStream(IP, Port)

#, "sentiment": sentyment_func(tweet)
# query1 = lines.foreachRDD(lambda rdd: rdd.filter(filter_tweets))
query = lines.filter(filter_tweets).map(lambda tweet: {"text": tweet})
#
# tweet = spark.createDataFrame(query)
# tweet.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
#
query.foreachRDD(lambda rdd: rdd.write.format("mongodb://127.0.0.1/tweets.tweetsCollection").mode("append").save())



#
#
# query.saveAsTextFiles("/home/rafal/Desktop/Tweety/tweets")

ssc.start()
ssc.awaitTermination()