import findspark
from textblob import TextBlob
findspark.init('/home/rafal/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming import StreamingContext
import time
import json

def filter_tweets(text):
    if text == "":
        return False
    else:
        return True


def sentyment_func(tweet):
    analysis = TextBlob(tweet)
    sentiment = analysis.sentiment.polarity
    return sentiment

my_spark = SparkSession \
    .builder \
    .appName("myapp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/tweets.tweetsCollection") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/tweets.tweetsCollection") \
    .getOrCreate()

my_spark.sparkContext.stop()

sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 5) # 5 second batch interval

IP = "localhost"	# Replace with your stream IP
Port = 6006		# Replace with your stream port

lines = ssc.socketTextStream(IP, Port)
query = lines.filter(filter_tweets).map(lambda tweet: {"text": tweet, "sentiment": sentyment_func(tweet)})
query.pprint()
# query.foreachRDD(lambda rdd: rdd.coalesce(1).saveAsTextFile("./tweets/%f" % time.time()) )
#query.foreachRDD(lambda rdd: rdd.toDF().write.option("uri", "mongodb://127.0.0.1/tweets.tweetsCollection").mode("append").save())
#write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save())

query.foreachRDD(lambda rdd: rdd.coalesce(1).saveAsTextFile("/home/rafal/Desktop/Tweety/tweets/%f" % time.time()) )
ssc.start()			   # Start reading the stream
ssc.awaitTermination() # Wait for the process to terminate