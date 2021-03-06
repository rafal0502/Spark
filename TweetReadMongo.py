import os
from pprint import pprint
import tweepy
from pymongo import MongoClient
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
from signal import signal, SIGPIPE, SIG_DFL

MONGO_HOST = 'mongodb://localhost/tweets'

consumer_key=""
consumer_secret=""
access_token =''
access_secret=""


class TweetsListener(StreamListener):

    def on_connect(self):
        print("You are connected to the streaming API.")

    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.tweets
            data = json.loads(data)
            #msg = json.loads(data)
            #pprint(tweet['text'].encode('utf-8'))
            #self.client_socket.send(msg['text'].encode('utf-8'))
            #self.client_socket.send(tweet['text'].encode('utf-8'))
            print("Tweet collected at: " + str(data['created_at']))
            db.tweetsCollection.insert({"text": data['text']})
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

#
# def sendData(c_socket):
#     auth = OAuthHandler(consumer_key, consumer_secret)
#     auth.set_access_token(access_token, access_secret)
#
#     twitter_stream = Stream(auth, TweetsListener(c_socket))
#     twitter_stream.filter(track=['trump'])


#if __name__ == "__main__":
    # s = socket.socket()  # Create a socket object
    # host = "localhost"  # Get local machine name
    # port = 6006  # Reserve a port for your service.
    # s.bind((host, port))  # Bind to the port
    #
    # print("Listening on port: %s" % str(port))
    #
    # s.listen(5)  # Now wait for client connection.
    # c, addr = s.accept()  # Establish connection with client.
    #
    # print("Received request from: " + str(addr))
    #
    # sendData(c)

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = TweetsListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking:")
streamer.filter(track="Google")




