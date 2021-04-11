from datetime import datetime
import json
import re

class TwitterEsSchema(object):
    """ Modification of twitter provided tweet object to ElasticSearch document. Strips a way
    several fields to improve ES performance. """
    def __init__(self):
        self.empty = True

    def trim_user(self, twitter_user):
        """ Trims nonintersting fields out of Twitter's user object. """
        # Fields of interest
        fields = ['id_str', 'name', 'screen_name', 'location', 'description', 'protected',
                  'followers_count', 'utc_offset']
        trimmed = {}
        for f in fields:
            trimmed[f] = twitter_user[f]

        date_obj = datetime.strptime(twitter_user['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        trimmed['created_at'] = date_obj.isoformat()
        return trimmed

    def handle_urls(self):
        """ strip display url to domain level. """
        if self.empty:
            raise ValueError
        for url_obj in self.tweet['entities']['urls']:
            only_domain = url_obj['display_url'].split('/')[0]
            url_obj['display_url'] = only_domain

    def handle_hashtags(self):
        """ Simplify hashtag list in tweets. """
        if self.empty:
            raise ValueError
        simple_tag_list = []
        for tag in self.tweet['entities']['hashtags']:
            simple_tag_list.append(tag['text'].lower())
        self.tweet['entities']['hashtags'] = simple_tag_list

    def trim_retweet(self):
        """ Trim out unnecessary fields from re-tweets. """
        if self.empty:
            raise ValueError
        self.tweet['is_retweet_status'] = True
        trimmed_rt = {}

        trimmed_rt_user = self.trim_user(self.tweet['retweeted_status']['user'])
        trimmed_rt['user'] = trimmed_rt_user

        date_obj = datetime.strptime(self.tweet['retweeted_status']['created_at'],
                                    '%a %b %d %H:%M:%S +0000 %Y')
        trimmed_rt['created_at'] = date_obj.isoformat()
        trimmed_rt['id_str'] = self.tweet['retweeted_status']['id_str']

        self.tweet['retweeted_status'] = trimmed_rt

    def trim_quote(self):
        """ trim out unnecessary field from tweets that quote another tweets. """
        if self.empty:
            raise ValueError
        trimmed_quote = {}

        try:
            date_obj = datetime.strptime(self.tweet['quoted_status']['created_at'],
                                         '%a %b %d %H:%M:%S +0000 %Y')
            trimmed_quote['created_at'] = date_obj.isoformat()
        except KeyError:  # The quoted tweet was a retweet creating a nested structure.
            return

        trimmed_quoted_user = self.trim_user(self.tweet['quoted_status']['user'])
        trimmed_quote['user'] = trimmed_quoted_user

        self.tweet['quoted_status'] = trimmed_quote

    def trim_media(self):
        """ Trim out excessive media metadata. """
        del self.tweet['extended_entities']  # unnecessary duplicate data
        for item in self.tweet['entities']['media']:
            del item['media_url_https']
            del item['sizes']
            del item['display_url']
            del item['media_url']  # there is a https version of this
            del item['id_str']

    def populate(self, tweet_obj):
        """ Add data from the twitter object to this object. Lossy operation. """
        self.timestamp = datetime.strptime(tweet_obj['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        tweet_obj['@timestamp'] = self.timestamp.isoformat()

        midnight_of_day = self.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        tweet_obj['time_of_day'] = int((self.timestamp - midnight_of_day).total_seconds())
        del tweet_obj['created_at']

        trimmed_user = self.trim_user(tweet_obj['user'])
        tweet_obj['user'] = trimmed_user
        tweet_obj['is_retweet_status'] = False

        # strip html elements from source field
        try:
            tweet_obj['source'] = re.split('<|>', tweet_obj['source'])[2]
        except IndexError:
            # Empty or malformed field in the tweet data.
            tweet_obj['source'] = tweet_obj['source']
        self.tweet = tweet_obj
        self.empty = False

        self.handle_urls()
        self.handle_hashtags()

        if 'retweeted_status' in self.tweet:
            self.trim_retweet()
        if self.tweet['is_quote_status']:
            self.trim_quote()
        if 'media' in self.tweet['entities']:
            self.trim_media()

    def get_json(self):
        """ Return json string. Suitable for bulk ingest in ElasticSearch. """
        if self.empty:
            print("ERROR: Tweet has not been populated!")
            raise ValueError
        return json.dumps(self.tweet)
