# Tweet Fetcher

A tool for searching tweets from twitter and feeding them to ElasticSearch. ElasticSearch and Kibana
will do all the heavy lifting for the analysis part. This tool supports several modes. From
following a list of users to recording certain search results e.g. hashtags. You can also write
results on a disc in case you don't have an ElasticSearch cluster available at the moment and you
want to return to the things you have recorded later.

## Requirements

In order to access tweets you need to have an app in
[Twitter Developer Portal](https://developer.twitter.com/en/portal). To get one you need a Twitter
account. Login and surf to the portal, where you can leave an application for API access. It takes
a while for twitter to review your application. For me it took two weeks and
required responding to a couple of emails with further questions.

You also need a running [ElasticSearch](https://developer.twitter.com/en/portal/) cluster. I am
running it in a
[Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) container on
my desktop with [Kibana](https://www.elastic.co/guide/en/kibana/current/docker.html) on a separate
container.

## Prerequisites

- [Python3](https://docs.python.org/3/tutorial/introduction.html#)
- [pip](https://pypi.org/project/pip/)
- [Tweepy](https://github.com/tweepy/tweepy)
- [Python ElasticSearch Client](https://github.com/elastic/elasticsearch-py)

I have gathered the exact versions I use to requirements.txt. Please, note that fresh versions
of the Python ElasticSearch Client might not be compatible with OpenSearch
(Amazon's for of the Elastic Search)

## Usage

The help message can be revealed with command

      $ ELASTICSEARCH_PASS='secret_pw' python3 tweet_fetcher -h
      usage: tweet_fetcher [-h] [-c CONFIG] [-t TARGET] [-v] [-s TERM] [-m MODE]
      [-j PROC_COUNT] [-i INDEX] [-p PATH] [-q TIME_PATH]

      Fetch tweets Twitter´s developer API. Push the tweets to elastic search.
      Networking debuging can be done useing curl:
      curl http://localhost:9200/_cluster/health\?pretty

      To promote safe development practises pass the elasticsearch password as
      an enviromental variable ELASTICSEARCH_PASS.

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
      -q TIME_PATH   Path to timestamp file

There is an example configuration in the root of the repository. The password
for the ElasticSearch API is not in the configuration file, but must be defined
as an environemntal variable.

In case you have recorded some tweets to files you can upload the files to an
ElasticSearch cluster with the _tweet_uploader.py_ script. This script assumes
that the files are all in a single folder and that the folder doesn't
contain other files.

      $ ELASTICSEARCH_PASS='secret_pw' python3 tweet_fetcher/tweet_uploader.py -h
      usage: tweet_uploader.py [-h] [-c CONFIG] [-v] [-i INDEX] [-p PATH]

      Upload tweets from file. Helper script

      optional arguments:
      -h, --help  show this help message and exit
      -c CONFIG   Path to the configuration file
      -v          Enable verbose output. Optional
      -i INDEX    Name of the index to be used.
      -p PATH     Path to file where the timeline was stored
