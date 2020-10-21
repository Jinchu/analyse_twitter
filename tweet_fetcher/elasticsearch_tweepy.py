from tweepy import API
from tweepy.error import RateLimitError
from elasticsearch import Elasticsearch
from datetime import timedelta, datetime
import os
import pickle

import twitter_es_schema

class ElasticSearchTweepy(API):
    """Extion to tweepy's Twitter API. It provides Functions for integrating with ElasticSearch."""

    def set_es_index(self, index_name, es_handle, debug = False):
        """ Set the index to be used. """
        self.index = index_name

        if es_handle.indices.exists(index=index_name):
            if debug:
                print("index %s exists" % index_name)
        else:
            if debug:
                print("index %s must be created" % index_name)
            self.create_index(es_handle)

    def create_index(self, es_handle):
        """ Creats a new index with given name. Uses standard config. """
        request_body = {
            "settings": {
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "entities": {
                        "properties": {
                            "hashtags": {
                                "type": "keyword"
                            },
                            "urls": {
                                "properties": {
                                    "display_url": {
                                        "type": "keyword"
                                    },
                                    "expanded_url": {
                                        "type": "keyword"
                                    },
                                    "indices": {
                                        "type": "long"
                                    },
                                    "url": {
                                        "type": "keyword"
                                    }
                                }
                            },
                            "media": {
                                "properties": {
                                    "expanded_url": {
                                        "type": "keyword"
                                    },
                                    "source_status_id_str": {
                                        "type": "keyword"
                                    },
                                    "source_user_id_str": {
                                        "type": "keyword"
                                    }
                                }
                            }
                        }
                    },
                    "favorite_count": {
                        "type": "long"
                    },
                    "source": {
                        "type": "keyword"
                    },
                    "favorited": {
                        "type": "boolean"
                    },
                    "full_text": {
                        "type": "text"
                    },
                    "id": {
                        "type": "long"
                    },
                    "id_str": {
                        "type": "keyword"
                    }
                }
            }
        }

        return es_handle.indices.create(index = self.index, body = request_body)

    def create_es_bulk_string_from_timeline(self, timeline):
        """ Create a string that can be pushed to ElasticSearch bulk API from a timeline. """
        bulk_string = ""
        for tweet in timeline:
            raw_tweet = tweet._json
            schema = twitter_es_schema.TwitterEsSchema()
            try:
                schema.populate(raw_tweet)
                bulk_string += '{ "index": { "_id": %d} }\n' % raw_tweet['id']
                bulk_string += '%s\n' % schema.get_json()
            except ValueError:
                print("...")
                return False

        return bulk_string

    def user_timeline_to_es(self, target_handle, es_handle, _count=200,
                            _tweet_mode="extended", debug=False):
        """ Fetches timeline from a single user and pushes the tweets using ElasticSearch
        Bulk command. """

        user_timeline = self.user_timeline(
            target_handle, count=_count, tweet_mode=_tweet_mode)

        if debug:
            print("Fetched %d tweets from user: %s" % (len(user_timeline), target_handle))

        bulk_string = self.create_es_bulk_string_from_timeline(user_timeline)

        res = es_handle.bulk(bulk_string, index=self.index)

        if res['errors']:
            if debug:
                print("At least some ingests FAILED!")
            return False
        else:
            if debug:
                print("Clean run!")
        return True

    def list_timeline_to_es(self, storage_path, parallels, es_handle, debug = False, test = False):
        """ Fetches timelines of all users listed in the given file. """

        target_list = []
        with open(storage_path, 'r') as handle:
            for line in handle:
                target_list.append(int(line))

        for i in range(parallels):
            if test:  # When testing don't fork. Main process will act as a child process
                newpid = 0
            else:
                newpid = os.fork()

            # Child prosess will handle every n:th entry in the target_list with dedicated offset.
            if newpid == 0:
                for j in range(len(target_list)):
                    if j % parallels == i:
                        try:
                            self.user_timeline_to_es(target_list[j], es_handle = es_handle,
                                                     debug = debug)
                        except Exception as e:
                            print(target_list[j])
                            print(e)
                            print('---')

        return True

    def get_most_recent_id_from_es():
        """ Query the id of the most recent tweet from ElasticSearch. In case no matches
        in the search return -1. """
        return -1

    def get_most_recent_id_from_file():
        """ Read the id of the most recent tweet from ElasticSearch. In case no file is not found.
        return -1. """
        return -1

    # ToDo: take the most recent id as variable and return the bulk string as a variable.
    def search_term_to_es(self, search_term, es_handle, debug = False):
        """ Searches tweets matching the given search term and pushes them to ElasticSearch. """
        current_id = -1
        search_results = []

        query = '{"query": {"match_all": {}}, "size": 1, "sort": [ {"@timestamp": {"order": "desc"} } ] }'
        most_recent_tweet = es_handle.search(index=self.index, body = query)

        try:
            # Fetch the time of the previous search from the ElasticSearch
            most_recent_id = most_recent_tweet['hits']['hits'][0]['_id']
            if debug:
                print('most_recent_id: %s' % most_recent_id)
        except IndexError:
            # ElasticSearch contains no matches. Getting everything we can from Twitter.
            if debug:
                print('ElasticSearch is empty.')
            most_recent_id = '-1'

        # Limit comes for the Twitter API rate limit:  180 requests / 15-min window
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        for i in range(80):
            if debug:
                print(i, end=', ', flush = True)
            try:
                current_results = self.search(search_term, count=100, result_type='recent',
                                              max_id=current_id, since_id=most_recent_id)
            except RateLimitError:
                print('Rate limit exceeded!')
                break

            if len(current_results) <= 0:
                if debug:
                    print ('The search has been exhausted')
                break
            current_id = current_results[-1].id - 1
            search_results.extend(current_results)

        bulk_string = self.create_es_bulk_string_from_timeline(search_results)

        res = es_handle.bulk(bulk_string, index=self.index)

        if res['errors']:
            if debug:
                print("At least some ingests FAILED!")
            return False
        else:
            if debug:
                print("Clean run!")
        return True

    def search_term_to_file(self, search_term, file_path, debug=False):
        """ Searches tweets matching the given search term and store them in pickle file. """

        try:
            with open(file_path, 'rb') as handle:
                search_results = pickle.load(handle)
            most_recent_id = search_results[0].id
        except FileNotFoundError:
            # Starting from scratch. Getting everything we can from Twitter.
            most_recent_id = -1
            search_results = []

        current_id = -1

        # Limit comes for the Twitter API rate limit:  180 requests / 15-min window
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        for i in range(80):
            if debug:
                print(i, end=', ', flush = True)
            try:
                current_results = self.search(search_term, count = 100, result_type = 'recent',
                                              max_id = current_id, since_id = most_recent_id)
            except RateLimitError:
                print('Rate limit exceeded!')
                break

            if len(current_results) <= 0:
                if debug:
                    print ('The search has been exhausted')
                break
            current_id = current_results[-1].id - 1
            current_results.extend(search_results)
            search_results = current_results

        with open(file_path, 'wb') as handle:
            pickle.dump(search_results, handle)

    def send_tweets_from_file_to_es(self, es_handle, file_path, debug=False):
        """ Read a pickle file and send the contained timeline of tweets to ElasticSearch. """

        with open(file_path, 'rb') as handle:
                search_results = pickle.load(handle)
        bulk_string = self.create_es_bulk_string_from_timeline(search_results)

        res = es_handle.bulk(bulk_string, index=self.index)
        if res['errors']:
            if debug:
                print("At least some ingests FAILED!")
            return False
        else:
            if debug:
                print("Clean run!")
        return True

    def clean_up_friends_file(self, storage_path, debug=False):
        """ Cleans up the generated file of user_ids. For example users that have not tweeted for
        six months will be removed. """
        treshold = timedelta(days=180)
        saved = []

        with open(storage_path, 'r') as handle:
            for line in handle:
                if debug:
                    print("Checking: %s" % line.strip())
                try:
                    user_timeline = self.user_timeline(int(line), count=2)
                except Exception as e:
                    print(e)

                since_active = datetime.now() - user_timeline[0].created_at
                if since_active < treshold:
                    saved.append(int(line))
                elif debug:
                    print("Discard: %s" % line)

        with open(storage_path, 'w') as handle:
            for uid in saved:
                handle.write('%d\n' % uid)
        return True

    def save_friends_file(self, target_handle, storage_path, debug=False):
        """ Generates list that can be used in list mode. Utilizes target account's followed field.
        Because the basic API keeps hitting rate limits this uses user_id instead of full objects.
        Max number of friends returned with friends_ids() is 5000 where as with friends() is 20."""
        unique = set()
        user_ids = self.friends_ids(screen_name=target_handle)

        try:
            with open(storage_path, 'r') as handle:
                for line in handle:
                    unique.add(int(line.strip()))
            if debug:
                print("%s found. Adding new unique ids there." % storage_path)
        except FileNotFoundError:
            if debug:
                print("%s not found. Creating one." % storage_path)

        for i in user_ids:
            if i not in unique:
                unique.add(i)

        with open(storage_path, 'w') as handle:
            for u in unique:
                handle.write('%d\n' % u)

        return True
