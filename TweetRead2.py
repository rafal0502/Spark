import os
from pprint import pprint

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
from signal import signal, SIGPIPE, SIG_DFL



consumer_key="oW2uw8As0rEkBFKIpyoRUTXUV"
consumer_secret="zxXspRVODqJdCwK7WYclAUVyqegIXLoss4gTXEPDqyh2FvqFP6"
access_token ='744293077037256704-3P6rEYX3Pk7Xy6OirkO0Z1sWFNTO1Sg'
access_secret="cbbJ7tCFn9AqL9MalVPADBjE5KzHlt8SNj9fv7HMbr56D"


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            #msg = json.loads(data)
            pprint(tweet['text'].encode('utf-8'))
            #self.client_socket.send(msg['text'].encode('utf-8'))
            self.client_socket.send(tweet['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 6006  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)