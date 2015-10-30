Insight Data Engineering - Coding Challenge
===========================================================

For this coding challenge, you will develop tools that could help analyze the community of Twitter users.  For simplicity, the features we will build are primitive, but you could easily build more complicated features on top of these.   

## Challenge Summary

This challenge is to implement two features:

1. Clean and extract the text from the raw JSON tweets that come from the Twitter Streaming API, and track the number of tweets that contain unicode.
2. Calculate the average degree of a vertex in a Twitter hashtag graph for the last 60 seconds, and update this each time a new tweet appears.

Here, we have to define a few concepts (though there will be examples below to clarify):

- A tweet's text is considered "clean" once all of the escape characters (e.g. \n, \", \/ ) and unicode have been removed.
- A Twitter hashtag graph is a graph connecting all the hashtags that have mentioned together in a single tweet.

## Details of Implementation

We'd like you to implement your own version of these two features.  However, we don't want this challenge to focus on the relatively uninteresting "dev ops" of connecting to the Twitter API, which can be complicated for some users.  Normally, tweets can be obtained through Twitter's API in JSON format, but you may assume that this has already been done and written to a file named `tweets.txt` inside a directory named `tweet_input`.  For simplicity, this file `tweets.txt` will only contain the actual JSON messages (in reality, the API also can emit messages about the connection and the API rate limits).  Additionally, `tweets.txt` will have the content of each tweet on a newline:

tweets.txt:

	{JSON of first tweet}  
	{JSON of second tweet}  
	{JSON of third tweet}  
	.
	.
	.
	{JSON of last tweet}  

## First Feature

The point of the first feature is to extract and clean the relevant data for the Twitter JSON messages.  For example, a typical tweet might come in the following JSON message (which we have expanded on to multiple lines to make it easier to read):

<pre>
{
 "created_at":"<b>Thu Oct 29 17:51:01 +0000 2015</b>","id":659789759787589632,
 "id_str":"659789759787589632","text":"<b>Spark Summit East this week! #Spark #Apache</b>",
 "source":"\u003ca href=\"http:\/\/twitter.com\" rel=\"nofollow\"\u003eTwitter Web Client\u003c\/a\u003e",
 "truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,
 "in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,
 "user":{"id":40077534,"id_str":"40077534","name":"scott bordow","screen_name":"sbordow",
 "location":null,"url":null,"description":"azcentral sports high school sports columnist. If you send me a tweet, you consent to letting azcentral sports use and showcase it in any media.",
 "protected":false,"verified":true,"followers_count":4704,"friends_count":2249,"listed_count":94,
 "favourites_count":51,"statuses_count":15878,"created_at":"Thu May 14 20:36:46 +0000 2009",
 "utc_offset":-25200,"time_zone":"Pacific Time (US & Canada)","geo_enabled":true,"lang":"en",
 "contributors_enabled":false,"is_translator":false,"profile_background_color":"C0DEED",
 "profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
 "profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
 "profile_background_tile":false,"profile_link_color":"0084B4","profile_sidebar_border_color":"C0DEED",
 "profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,
 "profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/576178462496423936\/YnOZ-StV_normal.jpeg",
 "profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/576178462496423936\/YnOZ-StV_normal.jpeg",
 "default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},
 "geo":null,"coordinates":null,"place":{"id":"a612c69b44b2e5da","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/a612c69b44b2e5da.json","place_type":"admin","name":"Arizona","full_name":"Arizona, USA","country_code":"US",
 "country":"United States","bounding_box":{"type":"Polygon","coordinates":[[[-114.818269,31.332246],[-114.818269,37.004261],[-109.045152,37.004261],[-109.045152,31.332246]]]},
 "attributes":{}},"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,
 "entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]},
 "favorited":false,"retweeted":false,"filter_level":"low","lang":"en","timestamp_ms":"1446141111691"
}  
</pre>

Where the relevant text that we want to extract has been bolded.  After extracting this information, this tweet should be outputted as

	Spark Summit East this week! #Spark #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)

with the format of 

	<contents of "text" field> (timestamp: <contents of "created_at" field>)

In this case, the tweet's text was already clean, but another example tweet might be:

<pre>
{
 "created_at":"<b>Thu Oct 29 18:10:49 +0000 2015</b>","id":659794531844509700,"id_str":"659794531844509700",
 "text":"<b>I'm at Terminal de Integra\u00e7\u00e3o do Varadouro in Jo\u00e3o Pessoa, PB https:\/\/t.co\/HOl34REL1a</b>",
 "source":"\u003ca href=\"http:\/\/foursquare.com\" rel=\"nofollow\"\u003eFoursquare\u003c\/a\u003e",
 "truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,
 "in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":60196177,"id_str":"60196177","name":"Jo\u00e3o Cassimiro","screen_name":"Jcassimironeto","location":"Paraiba","url":"http:\/\/www.facebook.com\/profile.php?id=1818814650",
 "description":"jcassimironeto","protected":false,"verified":false,"followers_count":240,"friends_count":654,
 "listed_count":0,"favourites_count":26,"statuses_count":2065,"created_at":"Sun Jul 26 01:15:03 +0000 2009",
 utc_offset":-7200,"time_zone":"Brasilia","geo_enabled":true,"lang":"pt","contributors_enabled":false,
 "is_translator":false,"profile_background_color":"022330","profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/671814600\/1028c894ede2eb444ebfd12f94f6cb93.jpeg",
 "profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/671814600\/1028c894ede2eb444ebfd12f94f6cb93.jpeg",
 "profile_background_tile":true,"profile_link_color":"0084B4","profile_sidebar_border_color":"FFFFFF",
 "profile_sidebar_fill_color":"C0DFEC","profile_text_color":"333333","profile_use_background_image":true,
 "profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/618238977565433856\/YM1aKFZj_normal.jpg",
 "profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/618238977565433856\/YM1aKFZj_normal.jpg",
 "profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/60196177\/1395970110","default_profile":false,
 "default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},
 "geo":{"type":"Point","coordinates":[-7.11792683,-34.88985837]},"coordinates":{"type":"Point",
 "coordinates":[-34.88985837,-7.11792683]},"place":{"id":"c9f2f46c0d1b963d","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/c9f2f46c0d1b963d.json","place_type":"city","name":"Jo\u00e3o Pessoa","full_name":"Jo\u00e3o Pessoa, Para\u00edba","country_code":"BR","country":"Brasil","bounding_box":{"type":"Polygon",
 "coordinates":[[[-34.971299,-7.243257],[-34.971299,-7.055696],[-34.792907,-7.055696],[-34.792907,-7.243257]]]},"attributes":{}},
 "contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],
 "urls":[{"url":"https:\/\/t.co\/HOl34REL1a","expanded_url":"https:\/\/www.swarmapp.com\/c\/2tATygSTvBu",
 "display_url":"swarmapp.com\/c\/2tATygSTvBu","indices":[62,85]}],"user_mentions":[],"symbols":[]},
 "favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"pt","timestamp_ms":"1446142249438"
}
</pre>

Now, the tweet's text needs to be cleaned by replacing the escape characters and removing the non-ASCII unicode characters to get the result:

	I'm at Terminal de Integrao do Varadouro in Joo Pessoa, PB https://t.co/HOl34REL1a (timestamp: Thu Oct 29 18:10:49 +0000 2015)

Perhaps it would make more sense to convert the unicode to similar ASCII letters, but we would like you to remove them instead for simplicity.  To help decide whether it would be worth spending more time on the unicode (perhaps for a future version of this feature), you will have to track the number of tweets that contain unicode, and write the following message at the bottom of the output file (with a newline preceding it):

	<number of tweets that had unicode> tweets contained unicode.

Your program should output the results of this first feature to a text file named `ft1.txt` in a directory named `tweet_output`, with each new tweet on a newline.  To be clear, if `tweets.txt` originally contained the following tweets:
```
{"created_at":"Thu Oct 29 17:51:01 +0000 2015","id":659789756637822976,"id_str":"659789756637822976","text":"Spark Summit East this week! #Spark #Apache","source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":42353977,"in_reply_to_user_id_str":"42353977","in_reply_to_screen_name":"KayKay121","user":{"id":317846866,"id_str":"317846866","name":"BaddieWinkleIsBae","screen_name":"WhoIsPetlo","location":"Polokwane | Grahamstown ","url":null,"description":"Extrovert","protected":false,"verified":false,"followers_count":767,"friends_count":393,"listed_count":0,"favourites_count":891,"statuses_count":41162,"created_at":"Wed Jun 15 15:31:30 +0000 2011","utc_offset":-10800,"time_zone":"Greenland","geo_enabled":true,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"C0DEED","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":false,"profile_link_color":"0084B4","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/659384066798694401\/Ogdtb4HJ_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/659384066798694401\/Ogdtb4HJ_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/317846866\/1443969906","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":{"id":"59efa64f5c8f5340","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/59efa64f5c8f5340.json","place_type":"city","name":"Grahamstown","full_name":"Grahamstown, South Africa","country_code":"ZA","country":"South Africa","bounding_box":{"type":"Polygon","coordinates":[[[26.479477,-33.326355],[26.479477,-33.270332],[26.560003,-33.270332],[26.560003,-33.326355]]]},"attributes":{}},"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[{"screen_name":"KayKay121","name":"KK_Rakitla95","id":42353977,"id_str":"42353977","indices":[0,10]}],"symbols":[],"media":[{"id":659789679491858432,"id_str":"659789679491858432","indices":[68,91],"media_url":"http:\/\/pbs.twimg.com\/tweet_video_thumb\/CSgLN8CUwAAzgez.png","media_url_https":"https:\/\/pbs.twimg.com\/tweet_video_thumb\/CSgLN8CUwAAzgez.png","url":"https:\/\/t.co\/HjZR3d5QaQ","display_url":"pic.twitter.com\/HjZR3d5QaQ","expanded_url":"http:\/\/twitter.com\/WhoIsPetlo\/status\/659789756637822976\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":300,"h":300,"resize":"fit"},"large":{"w":300,"h":300,"resize":"fit"},"medium":{"w":300,"h":300,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":659789679491858432,"id_str":"659789679491858432","indices":[68,91],"media_url":"http:\/\/pbs.twimg.com\/tweet_video_thumb\/CSgLN8CUwAAzgez.png","media_url_https":"https:\/\/pbs.twimg.com\/tweet_video_thumb\/CSgLN8CUwAAzgez.png","url":"https:\/\/t.co\/HjZR3d5QaQ","display_url":"pic.twitter.com\/HjZR3d5QaQ","expanded_url":"http:\/\/twitter.com\/WhoIsPetlo\/status\/659789756637822976\/photo\/1","type":"animated_gif","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":300,"h":300,"resize":"fit"},"large":{"w":300,"h":300,"resize":"fit"},"medium":{"w":300,"h":300,"resize":"fit"}},"video_info":{"aspect_ratio":[1,1],"variants":[{"bitrate":0,"content_type":"video\/mp4","url":"https:\/\/pbs.twimg.com\/tweet_video\/CSgLN8CUwAAzgez.mp4"}]}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1446141110940"}
{"created_at":"Thu Oct 29 18:10:49 +0000 2015","id":659794531844509700,"id_str":"659794531844509700","text":"I'm at Terminal de Integra\u00e7\u00e3o do Varadouro in Jo\u00e3o Pessoa, PB https:\/\/t.co\/HOl34REL1a","source":"\u003ca href=\"http:\/\/foursquare.com\" rel=\"nofollow\"\u003eFoursquare\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":60196177,"id_str":"60196177","name":"Jo\u00e3o Cassimiro","screen_name":"Jcassimironeto","location":"Paraiba","url":"http:\/\/www.facebook.com\/profile.php?id=1818814650","description":"jcassimironeto","protected":false,"verified":false,"followers_count":240,"friends_count":654,"listed_count":0,"favourites_count":26,"statuses_count":2065,"created_at":"Sun Jul 26 01:15:03 +0000 2009","utc_offset":-7200,"time_zone":"Brasilia","geo_enabled":true,"lang":"pt","contributors_enabled":false,"is_translator":false,"profile_background_color":"022330","profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/671814600\/1028c894ede2eb444ebfd12f94f6cb93.jpeg","profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/671814600\/1028c894ede2eb444ebfd12f94f6cb93.jpeg","profile_background_tile":true,"profile_link_color":"0084B4","profile_sidebar_border_color":"FFFFFF","profile_sidebar_fill_color":"C0DFEC","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/618238977565433856\/YM1aKFZj_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/618238977565433856\/YM1aKFZj_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/60196177\/1395970110","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":{"type":"Point","coordinates":[-7.11792683,-34.88985837]},"coordinates":{"type":"Point","coordinates":[-34.88985837,-7.11792683]},"place":{"id":"c9f2f46c0d1b963d","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/c9f2f46c0d1b963d.json","place_type":"city","name":"Jo\u00e3o Pessoa","full_name":"Jo\u00e3o Pessoa, Para\u00edba","country_code":"BR","country":"Brasil","bounding_box":{"type":"Polygon","coordinates":[[[-34.971299,-7.243257],[-34.971299,-7.055696],[-34.792907,-7.055696],[-34.792907,-7.243257]]]},"attributes":{}},"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[{"url":"https:\/\/t.co\/HOl34REL1a","expanded_url":"https:\/\/www.swarmapp.com\/c\/2tATygSTvBu","display_url":"swarmapp.com\/c\/2tATygSTvBu","indices":[62,85]}],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"pt","timestamp_ms":"1446142249438"}
```

then the output in `ft1.txt` should contain:
```
Spark Summit East this week! #Spark #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)
I'm at Terminal de Integrao do Varadouro in Joo Pessoa, PB https://t.co/HOl34REL1a (timestamp: Thu Oct 29 18:10:49 +0000 2015)

1 tweets contained unicode.
```

## Second Feature
The second feature will continually update the Twitter hashtag graph and hence, the average degree of the graph. The graph should just be built using tweets that arrived in the last 60 seconds as compared to the timestamp of the latest tweet. As new tweets come in, edges formed with tweets older than 60 seconds from the timstamp of the latest tweet should be evicted. For each incoming tweet, only extract the following fields in the JSON response
* "hastags" - hashtags found in the tweet
* "created_at" - timestamp of the tweet

### Building the Twitter Hashtag Graph
Example of 4 tweets (using the same format from the first feature)
```
Spark Summit East this week! #Spark #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)
Just saw a great post on Insight Data Engineering #Apache #Hadoop #Storm (timestamp: Thu Oct 29 17:51:30 +0000 2015)
Doing great work #Apache (timestamp: Thu Oct 29 17:51:55 +0000 2015)
Excellent post on #Flink and #Spark (timestamp: Thu Oct 29 17:51:56 +0000 2015)
```

Extracted hashtags from each tweet
```
#Spark, #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)
#Apache, #Hadoop, #Storm (timestamp: Thu Oct 29 17:51:30 +0000 2015)
#Apache (timestamp: Thu Oct 29 17:51:55 +0000 2015)
#Flink, #Spark (timestamp: Thu Oct 29 17:51:56 +0000 2015)
```

Two hashtags will be connected if and only if they are present in the same tweet. Only tweets that contain two or more **DISTINCT** hashtags can create new edges.

A good way to create this graph is with an edge list where an edge is defined by two hashtags that are connected. 

The edge list made by all the above tweets is as follows:
```
#Spark <-> #Apache

#Apache <-> #Hadoop
#Hadoop <-> #Storm
#Storm <-> #Apache

#Flink <-> #Spark
```

Notice that the third tweet did not generate a new edge since there were no other hashtags besides `#Apache` in that tweet. Also, all tweets occured in the 60 seconds time window as compared to the latest tweet and they all are included in building the graph.

The edge list can be visualized with the following diagrams where each node is a hashtag. The first tweet will generate the `#Spark` and `#Apache` nodes.

![spark-apache-graph](images/htag_graph_1.png)

The second tweet contains 3 hashtags `#Apache`, `#Hadoop`, and `#Storm`. `#Apache` already exists, so only `#Hadoop` and `#Storm` are added to the graph.

![apache-hadoop-storm-graph](images/htag_graph_2.png)

The third tweet generated no edges, so no new nodes will be added to the graph.

The fourth tweet contains `#Flink` and `#Spark`. `#Spark` already exists, so only `#Flink` will be added.

![flink-spark-graph](images/htag_graph_3.png)

We can now calculate the degree of each node which is defined as the number of connected neighboring nodes.

![graph-degree3](images/htag_degree_3.png)

The average degree for simplicity will be calculated by summing the degrees of all nodes in all graphs and dividing by the total number of nodes in all graphs.

Average Degree = (1+2+3+2+2)/5 = 2.00

The rolling average degree is now 
```
2.00
```

### Modifying the Twitter Hashtag Graph with Incoming Tweet
Now let's say another tweet has arrived
```
New and improved #HBase connector for #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)
```

The extracted hashtags are then
```
#HBase, #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)
```

and added to the edge list
```
#Spark <-> #Apache

#Apache <-> #Hadoop
#Hadoop <-> #Storm
#Storm <-> #Apache

#Flink <-> #Spark

#HBase <-> $Spark
```

The graph now looks like the following

![hbase-spark-graph](images/htag_graph_4.png)

with the updated degree calculation for each node. Here only `#Spark` needs to be incremented due to the additional `#HBase` node.

![graph-degree4](images/htag_degree_4.png)

The average degree will be recalculated using the same formula as before.

Average Degree = (1+3+1+3+2+2)/6 = 2.00

The rolling average degree is now 
```
2.00
2.00
```

### Maintaining Data within the 60 Second Window
Now let's say that the next tweet that comes in has the following timestamp
```
New 2.7.1 version update for #Hadoop #Apache (timestamp: Thu Oct 29 17:52:05 +0000 2015)
```

The full list of tweets now is 
```
Spark Summit East this week! #Spark #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)
Just saw a great post on Insight Data Engineering #Apache #Hadoop #Storm (timestamp: Thu Oct 29 17:51:30 +0000 2015)
Doing great work #Apache (timestamp: Thu Oct 29 17:51:55 +0000 2015)
Excellent post on #Flink and #Spark (timestamp: Thu Oct 29 17:51:56 +0000 2015)
New and improved #HBase connector for #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)
New 2.7.1 version update for #Hadoop #Apache (timestamp: Thu Oct 29 17:52:05 +0000 2015)
```

We can see that the very first tweet has a timestamp that is more than 60 seconds behind this new tweet. This means that we do not want to include this tweet in our average degree calculation.

The new hashtags to be used are as follows
```
#Apache, #Hadoop, #Storm (timestamp: Thu Oct 29 17:51:30 +0000 2015)
#Apache (timestamp: Thu Oct 29 17:51:55 +0000 2015)
#Flink, #Spark (timestamp: Thu Oct 29 17:51:56 +0000 2015)
#HBase, #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)
#Hadoop #Apache (timestamp: Thu Oct 29 17:52:05 +0000 2015)
```

The new edge list only has the `#Spark` <-> `#Apache` edge removed since `#Hadoop` <-> `#Apache` from the new tweet already exists in the edge list.
```
#Apache <-> #Hadoop
#Hadoop <-> #Storm
#Storm <-> #Apache

#Flink <-> #Spark

#HBase <-> $Spark
```

The old graph has now been disconnected forming two graphs.

![evicted-spark-apache](images/htag_graph_5.png)

We'll then calculate the new degree for all the nodes in both graphs.

![graph-degree5](images/htag_degree_5.png)

Recalculating the average degree of all nodes in all graphs is as follows
Average Degree = (1+2+1+2+2+2)/6 = 1.67

Normally the average degree is calculated for a single graph, but maintaining multiple graphs for this problem can be quite difficult. For simplicity we are only interested in calculating the average degree of of all the nodes in all graphs despite them being disconnected.

The rolling average degree is now 
```
2.00
2.00
1.67
```

## Collecting tweets from the Twitter API
Ideally, the second feature that updates the average degree of a Twitter hashtag graph as each tweet arrives would be connected to the Twitter streaming API and would add new tweets to the end of `tweets.txt`.  However, connecting to the API requires more system specific "dev ops" work, which isn't the primary focus for data engineers.  Instead, you should simply assume that each new line of the text file corresponds to a new tweet and design your program to handle a text file with a large number of tweets.  Your program should output the results of this second feature to a text file named `ft2.txt` in the `tweet_output` directory.


## Writing clean, scalable, and well-tested code  
As a data engineer, it’s important that you write clean, well-documented code that scales for large amounts of data.  It's also important to use software engineering best practices like unit tests, especially since public data is not clean and predictable.  For this reason, it’s important to ensure that your solution works well for a huge number of tweets, rather than just the simple examples above.  For example, your solution should be able to account for a large number of tweets coming in a short period of time, and need to keep up with the input (i.e. need to process a minute of tweets in less than a minute).  For more details about the implementation, please refer to FAQ below or email us at cc@insightdataengineering.com

You may write your solution in any mainstream programming language such as C, C++, C#, Clojure, Erlang, Go, Haskell, Java, Python, Ruby, or Scala - then submit a link to a Github repo with your source code.  In addition to the source code, the top-most directory of your repo must include the `tweet_input` and `tweet_output` directories, and a shell script named `run.sh` that compiles and runs the program(s) that implement these features.  If your solution requires additional libraries, environments, or dependencies, you must specify these in your README documentation.  See the figure below for the required structure of the top-most directory in your repo, or simply clone this repo.

## Repo directory structure
![Example Repo Structure](images/directory-pic.png)

Alternatively, here is example output of the `tree` command:

	├── README.md  
	├── run.sh  
	├── src  
	│   ├── average_degree.py  
	│   └── tweets_cleaned.py  
	├── tweet_input  
	│   └── tweets.txt  
	└── tweet_output  
	    ├── ft1.txt  
	    └── ft2.txt  


## FAQ

Here are some common questions we've received.  If you have additional questions, feel free fork this repo, add them to the README.md, then issue a pull request.  Alternatively, you can email cc@insightdataengineering.com and we'll add the answers as quickly as we can.

* *Which Github link should I submit?*  
You should submit the URL for the top-level root of your repository.  For example, this repo would be submitted by copying the URL `https://github.com/InsightDataScience/cc-example` into the appropriate field on the application.  Please do NOT try to submit your coding challenge using a pull request, which will make your source code publicly available.  

* *Do I need a private Github repo?*  
No, you may use a public repo, there is no need to purchase a private repo.   

* *Do you have any larger sample inputs?*  
Yes, we have just added an example input with 10,000 tweets in the `data-gen` directory of this repo.  It also contains a simplified producer that can connect to the live Twitter API and save the tweets to an input file that conforms to the requirements of this data challenge.  This is not required for this challenge, but may be helpful for testing your solution.  

* *May I use R or other analytics programming languages to solve the challenge?*  
While you may use any programming language to complete the challenge, it's important that your implementation scales to handle large amounts of data.  Many applicants have found that R is unable to process data in a scalable fashion, so it may be more practical to use another language.  

* *May I use distributed technologies like Hadoop or Spark?*  
While you're welcome to use any language or technology, it will be tested on a single machine so there may not be a significant benefit to using these technologies prior to the program.  With that said, learning distributed systems would be a valuable skill for all data engineers.

* *What sort of system should I use to run my program on (Windows, Linux, Mac)?*  
You may write your solution on any system, but your code should be portable and work on all systems.  In particular, your code must be able to run on either Unix or Linux, as that's what the system will be tested on.  This means that you must submit a working `run.sh` script.  Linux machines are the industry standard for most data engineering companies, so it is helpful to be familiar with this.  If you're currently using Windows, we recommend using Cygwin or a free online IDE such as Cloud9 (c9.io).  

* *When are two hashtags considered the same?*  
Hashtags must be identical to be considered the same. 

* *Can I use pre-built packages, modules, or libraries?*   
Yes, you may use any publicly available package, module, or library as long as you document any dependencies in your accompanying `README` file.  When we review your submission, we will download these libraries and attempt to run your program.   This is why it's very important that you document any dependencies or system specific details in your accompanying README file.  However, you should always ensure that the module you're using works efficiently for the specific use-case in the challenge, many libraries are not designed for large amounts of data.

* *Will you email me if my code doesn't run?*   
Unfortunately, we receive hundreds of submissions in a very short time and are unable to email individuals if code doesn't compile or run.  This is why it's so important to document any dependencies you have, as described in the previous question.  We will do everything we can to properly test your code, but this requires good documentation.  

* *Do I need to use multi-threading?*   
No, your solution doesn't necessarily need to include multi-threading - there are many solutions that don't require multiple threads/cores or any distributed systems.  

* *Do I need to account for and updating `tweets.txt` file?*   
No, your solution doesn't have to re-process `tweets.txt`.  Instead, it should be designed to handle a very large input size.  If you were doing this project as a data engineer in industry, you would probably re-run your program daily to handle batches, but this is beyond the scope of this challenge.  

* *What should the format of the output be?*  
In order to be tested correctly, you must use the format described above.  We will try our best to correct any minor formatting issues, but try to follow the examples above as closely as possible.  

* *Do I need to account for complicated Unicode characters by replacing them?*  
No, you simply need to remove them and track how many tweets require this removal.  

* *Should I check if the files in the input directory are text files or non-text files(binary)?*  
No, for simplicity you may assume that all of the files in the input directory are standard text files.  

* *Do I need to account for empty tweets?*  
No, for simplicity you may assume that all the tweets contain at least one word.  However, many tweets contain only unicode chracters, which will be effectively empty after you clean them.  This means you will have to test properly when implementing the second feature on real data.   

* *Do I need separate programs for different features?*  
You may use a single combined program or several programs, as long as they are all executed by the `run.sh` script.

* *Can I use an IDE like Eclipse to write my program?*  
Yes, you can use what ever tools you want -  as long as your `run.sh` script correctly runs the relevant target files and creates the `ft1.txt` and `ft2.txt` files in the `tweet_output` directory.

* *What should be in the `tweet_input` directory?*  
You can put any text file you want in the directory.  In fact, this could be quite helpful for testing your solutions.

* *How will the coding challenge be evaluated?*  
Generally, we will evaluate your coding challenge with a testing suite that provides a variety of input tweets and checks the corresponding output.  This suite will attempt to use your 'run.sh' and is fairly tolerant to different runtime environments.  Of course, there are many aspects that cannot be tested by our suite, so each submission will be reviewed manually by a person as well. 

* *How long will it take for me to hear back from you about my submission?*  
We receive hundreds of submissions and try to evaluate them all in a timely manner.  We try to get back to all applicants within two or three weeks of submission, but if you have a specific deadline that requires expedited review, you may email us at cc@insightdataengineering.com.  

