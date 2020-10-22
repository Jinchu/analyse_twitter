# Tweet Fetcher
A tool for searching tweets from twitter and feeding them to ElasticSearch. ElasticSearch and Kibana will do all the heavy lifting for the analysis part. This tool supports several modes. From following a list of users to recording certain search results e.g. hashtags. You can also write results on a disc in case you don't have an ElasticSearch cluster available at the moment and you want to return to the things you have recorded later.

## Requirements

In order to access tweets you need to have an app in [Twitter Developer Portal](https://developer.twitter.com/en/portal). To get one you need a Twitter account. Login and surf to the portal, where you can leave an application for API access. It takes a while for twitter to review your application. For me it took two weeks and required responding to a couple of emails with further questions.

You also need a running [ElasticSearch](https://developer.twitter.com/en/portal/) cluster. I am running it in a [Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) container on my desktop with [Kibana](https://www.elastic.co/guide/en/kibana/current/docker.html) on a separate container.

## Prerequisites

- [Python3](https://docs.python.org/3/tutorial/introduction.html#)
- [Tweepy](https://github.com/tweepy/tweepy)

      pip install tweepy
 
- [Python ElasticSearch Client](https://github.com/elastic/elasticsearch-py)

      python3 -m pip install elasticsearch


## Usage

The help message can be revealed with command::

      $ python3 -m pip install elasticsearch
      usage: tweet_fetcher [-h] [-c CONFIG] [-t TARGET] [-v] [-s TERM] [-m MODE]
                     [-j PROC_COUNT] [-i INDEX] [-p PATH]

      Fetch tweets Twitter´s developer API. Push the tweets to elastic search.

      optional arguments:
        -h, --help     show this help message and exit
        -c CONFIG      Path to the configuration file
        -t TARGET      The twitter handle of the target user
        -v             Enable verbose output. Optional
        -s TERM        Search tweets with this term.
        -m MODE        Mode of operation: user, term, list, generate.
        -j PROC_COUNT  Number of parallel processes used in list mode.
        -i INDEX       Name of the index to be used.
        -p PATH        Path to file where the timeline will be stored. Used with
                       _to_file

