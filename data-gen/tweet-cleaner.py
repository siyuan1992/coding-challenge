#!/usr/bin/env python

# Import the necessary methods from tweepy library
from __future__ import unicode_literals
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from pprint import pprint
import os
import json


# loads Twitter credentials from .twitter file that is in the same directory as this script
file_dir = os.path.dirname(os.path.realpath(__file__)) 
with open(file_dir + '/.twitter') as twitter_file:  
    twitter_cred = json.load(twitter_file)

access_token = twitter_cred["access_token"]
access_token_secret = twitter_cred["access_token_secret"]
consumer_key = twitter_cred["consumer_key"]
consumer_secret = twitter_cred["consumer_secret"]

class StdOutListener(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, filename):
        self.filename = filename

    def on_data(self, data):
        if not os.path.isfile(self.filename):    # check if file doesn't exist
            f = file(self.filename, 'w')
            f.close()
        with open(self.filename, 'ab') as f:
            print "writing to {}".format(self.filename)
            f.write(data)
            f.write("\n")
        f.closed
        
    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener(file_dir + "/tweets.txt")
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Use CTRL + C to exit at any time.\n"
    stream = Stream(auth, l)
    stream.filter(track=[
        '#BigData', '#Spark', '#ApacheKafka', '#ApacheStorm', '#ApacheSpark', 'Data Engineering', 'Hadoop', 'MapReduce',
        'Mahout', 'MLlib', 'Logstash', 'RabbitMQ', 'Fluentd', 'AWS', 'Avro', '#Samza', 'HBase', '#Cassandra', 'MongoDB',
        'Elasticsearch', 'Kibana', 'Neo4j', 'CouchDB', 'Redis', 'Memcached', '#Hive', '#ApacehPig', 'Cascalog', 'Giraph',
        '#Presto', '#Impala', '#ApacheDrill', 'GraphX', 'GraphLab', '#Redshift', 'Solr', '#Riak', 'Hazelcast'
        ])
