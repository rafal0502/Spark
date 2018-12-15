# -*- coding: utf-8 -*-
import findspark
findspark.init('/home/rafal/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from textblob import TextBlob
import time
import json
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


spark = SparkSession.builder.appName('Twittersy').getOrCreate()

#
# userSchema = StructType()\
#             .add("text", "string")


# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 6006) \
    .load()
   # .schema(userSchema) \


# socketDF.isStreaming()    # Returns True for DataFrames that have streaming sources
#
# socketDF.printSchema()
#
# # Read all the csv files written atomically in a directory
# userSchema = StructType().add("text_tweet")
# csvDF = spark \
#     .readStream() \
#     .option("sep", ";") \
#     .schema(userSchema) \
#     .csv("/home/rafal/Desktop/twittersy")  # Equivalent to format("csv").load("/path/to/directory")

# userSchema = StructType().add("texttweet","string")
# dataframe = socketDF.toDF()
#
# writer = dataframe.writeStream.schema(userSchema).outputmode("complete").format("console")
# writer.start()
# writer.awaitTermination()
# def compute_sentiment(text):
#    # json_tweet = json.loads(tweet)
#    # if json_tweet.has_key('lang'):  # When the lang key was not present it caused issues
#    #     if json_tweet['lang'] == 'en':
#    #         return False # filter() requires a Boolean value
#    analysis = TextBlob(text)
#    sentiment = analysis.sentiment.polarity
#    return sentiment


#tweets = socketDF.select()


# query = socketDF.writeStream.trigger(processingTime="10 seconds")\
#     .format("json")\
#     .option("checkpointLocation","applicationHistory")\
#     .option("path","/home/rafal/Desktop/Tweety")\
#     .start()

#
# sentyment = socketDF.rdd.map(compute_sentiment)
# socketDF['sentiment'] = sentyment

# query2 = socketDF\
#     .writeStream\
#     .format("console")\
#     .start()

query2 = socketDF\
    .writeStream\
    .format("csv")\
    .option("format","append")\
    .option("path","/home/rafal/Desktop/Tweety/structured_tweets")\
    .option("checkpointLocation","/home/rafal/Desktop/Tweety/checkpoint")\
    .outputMode("append")\
    .start()



query2.awaitTermination()