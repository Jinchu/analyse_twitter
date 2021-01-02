import unittest
import json
import os
import pickle
from unittest.mock import MagicMock
from elasticsearch import Elasticsearch
from time import sleep

import elasticsearch_tweepy

class MockTweepy(elasticsearch_tweepy.ElasticSearchTweepy):
    def __init__(self):
        self.index = -1

    def friends_ids(self, screen_name):
        if screen_name == "mikko":
            return [10, 12, 96]
        elif screen_name == "joni":
            return [67, 96, 22]

    def user_timeline(self, target_handle, count = 2, tweet_mode = 'extended'):
        with open('./test_data/test_timeline', 'rb') as handle:
            test_timeline = pickle.load(handle)
        return test_timeline

    def search(self, search_term, count = 20, result_type = 'recent',
               max_id = '-1', since_id = '-1'):
        self.latest_since = since_id
        if search_term == 'Rate limit':
            from tweepy.error import RateLimitError
            raise RateLimitError(reason=88)
        if max_id == -1:
            with open('./test_data/test_timeline', 'rb') as handle:
                test_timeline = pickle.load(handle)
            return test_timeline
        else:
            return []

class TestCleanUp(unittest.TestCase):
    def test_clean_up(self):
        storage_path = "./test_data/test_cleanup_storage.txt"

        test_api = MockTweepy()
        ret = test_api.clean_up_friends_file(storage_path)
        self.assertTrue(ret)

class TestListTimeline(unittest.TestCase):
    def test_list_timeline(self):
        test_api = MockTweepy()
        es = Elasticsearch()
        es_test_response = {}
        es_test_response['errors'] = False

        Elasticsearch.bulk = MagicMock(return_value = es_test_response)
        es.indices.exists = MagicMock(return_value = True)
        es.indices.create = MagicMock(return_value = True)

        test_api.user_timeline_to_es = MagicMock(return_value = True)

        test_file_path = './test_data/test_user_list.txt'
        retval = test_api.list_timeline_to_es(test_file_path, 1, es_handle = es, test = True)
        self.assertTrue(retval)
        test_api.user_timeline_to_es.assert_called_with(966444231249317889, es_handle = es,
                                                        debug = False)

        test_api.user_timeline_to_es = MagicMock(side_effect = Exception('Not authorized.'))
        retval = test_api.list_timeline_to_es(test_file_path, 1, es_handle = es, test = True)
        self.assertTrue(retval)

class TestTermSearchWithFile(unittest.TestCase):
    def test_search_term_rate_limit_with_file(self):
        test_api = MockTweepy()

        test_file_path = './test_data/create_search_test'
        test_time_path = './test_data/no_data_time.txt'

        full_test_path = test_api.search_term_to_file('Rate limit', file_path = test_file_path,
                                     time_stamp = test_time_path)
        self.assertFalse(os.path.exists(full_test_path))

    def test_search_term_create_new_file(self):
        test_api = MockTweepy()

        test_file_path = './test_data/create_search_test'
        test_time_path = './test_data/data_time.txt'

        if os.path.exists(test_file_path):
            os.remove(test_file_path)
        if os.path.exists(test_file_path):
            os.remove(test_time_path)

        full_test_path = test_api.search_term_to_file('dummy_search', file_path = test_file_path,
                                                      time_stamp = test_time_path)
        self.assertTrue(os.path.exists(full_test_path))

        with open(full_test_path, 'rb') as handle:
            test_search_lines = handle.readlines()

        self.assertEqual(len(test_search_lines), 4)
        test_search_res = json.loads(test_search_lines[3])
        self.assertEqual(test_search_res['@timestamp'], '2020-09-12T13:36:15')
        os.remove(full_test_path)

    ## This test has been deprecated. In order to keep filesizes reasonable new searches are stored
    #  in new files with prepended timestamps in the file name.
    # def test_search_term_update_old_file(self):

class TestUserTimelineToEs(unittest.TestCase):
    def test_user_timeline_to_elasticsears(self):
        es_test_response = {}
        es_test_response['errors'] = False
        es = Elasticsearch()
        Elasticsearch.bulk = MagicMock(return_value = es_test_response)
        es.indices.exists = MagicMock(return_value = True)
        es.indices.create = MagicMock(return_value = True)
        test_api = MockTweepy()
        test_api.set_es_index('test-a-tweet', es, debug = False)
        es.indices.create.assert_not_called()

        ret = test_api.user_timeline_to_es('mikko', es_handle = es)
        self.assertTrue(ret)

    def test_user_timeline_to_es_fail(self):
        es_test_response = {}
        es_test_response['errors'] = True
        Elasticsearch.bulk = MagicMock(return_value = es_test_response)
        es = Elasticsearch()
        es.indices.exists = MagicMock(return_value = False)
        es.indices.create = MagicMock(return_value = True)
        es.indices.create()
        test_api = MockTweepy()
        test_api.set_es_index('test-a-tweet', es, debug = False)
        es.indices.create.assert_called()

        ret = test_api.user_timeline_to_es('mikko', es_handle = es)
        self.assertFalse(ret)

class TestSaveFriendsFile(unittest.TestCase):
    def test_save_friends_ids(self):
        storage_path = "./test_data/test_id_storage.txt"
        if os.path.exists(storage_path):
            os.remove(storage_path)

        test_api = MockTweepy()
        ret = test_api.save_friends_file("mikko", storage_path)
        self.assertTrue(ret)

        stored = []
        with open(storage_path, 'r') as handle:
            for line in handle:
                stored.append(int(line))
        self.assertEqual(stored[0], 96)
        self.assertEqual(len(stored), 3)

        ret = test_api.save_friends_file("joni", storage_path)
        self.assertTrue(ret)

        stored = []
        with open(storage_path, 'r') as handle:
            for line in handle:
                stored.append(int(line))
        self.assertEqual(stored[0], 96)
        self.assertEqual(len(stored), 5)
        if os.path.exists(storage_path):
            os.remove(storage_path)

class TestTermSearchWithEs(unittest.TestCase):
    def test_search_term_push_es_rate_limit(self):
        test_api = MockTweepy()

        # test_file_path = './create_search_test.p'
        es = Elasticsearch()
        es_test_response = {}
        es_test_response['errors'] = False
        empty_response_str = '{ "took" : 1, "timed_out" : false, "_shards" : { "total" : 1, "successful" : 1,  "skipped" : 0, "failed" : 0 },  "hits" : {   "total" : {  "value" : 0, "relation" : "eq"  },  "max_score" : null, "hits" : [ ] } }'
        empty_response_json = json.loads(empty_response_str)

        Elasticsearch.bulk = MagicMock(return_value = es_test_response)
        Elasticsearch.search = MagicMock(return_value = empty_response_json)
        test_api.search_term_to_es('Rate limit', es_handle = es, debug = True)

        Elasticsearch.bulk.assert_called()
        self.assertEqual(test_api.latest_since, '-1')

    def test_search_term_push_es_normal(self):
        test_api = MockTweepy()

        # test_file_path = './create_search_test.p'
        es = Elasticsearch()
        es_test_response = {}
        es_test_response['errors'] = False
        latest_tweet_dummy_str = '{   "took" : 5,   "timed_out" : false,   "_shards" : {     "total" : 1,     "successful" : 1,     "skipped" : 0,     "failed" : 0   },   "hits" : {     "total" : {       "value" : 10000,       "relation" : "gte"     },     "max_score" : null,     "hits" : [       {         "_index" : "tweets",         "_type" : "_doc",         "_id" : "1316241040941232128",         "_score" : null,         "_source" : {           "id" : 1316241040941232128,           "id_str" : "1316241040941232128",           "full_text" : "@NewThor If I was worried I’d say something",           "truncated" : false,           "display_text_range" : [             9,             43           ],           "entities" : {             "hashtags" : [ ],             "symbols" : [ ],             "user_mentions" : [               {                 "screen_name" : "NewThor",                 "name" : "The King of America",                 "id" : 42721534,                 "id_str" : "42721534",                 "indices" : [                   0,                   8                 ]               }             ],             "urls" : [ ]           },           "source" : "Twitter for iPhone",           "in_reply_to_status_id" : 1316240204244623360,           "in_reply_to_status_id_str" : "1316240204244623360",           "in_reply_to_user_id" : 42721534,           "in_reply_to_user_id_str" : "42721534",           "in_reply_to_screen_name" : "NewThor",           "user" : {             "id_str" : "207617689",             "name" : "Lesley Carhart",             "screen_name" : "hacks4pancakes",             "location" : "Chicago, IL",             "description" : "ICS DFIR @dragosinc, martial artist, gamer, marksman, humanist, L14 Neutral Good rogue. I write & tweet *very serious* things about infosec. Thoughts mine. She.",             "protected" : false,             "followers_count" : 121969,             "utc_offset" : null,             "created_at" : "2010-10-25T17:46:44"           },           "geo" : null,           "coordinates" : null,           "place" : null,           "contributors" : null,           "is_quote_status" : false,           "retweet_count" : 0,           "favorite_count" : 0,           "favorited" : false,           "retweeted" : false,           "lang" : "en",           "@timestamp" : "2020-10-14T04:54:53",           "is_retweet_status" : false         },         "sort" : [           1602651293000         ]       }     ]   } }'
        latest_tweet_json = json.loads(latest_tweet_dummy_str)

        Elasticsearch.bulk = MagicMock(return_value = es_test_response)
        Elasticsearch.search = MagicMock(return_value = latest_tweet_json)
        test_api.search_term_to_es('Unit testing', es_handle = es)

        Elasticsearch.bulk.assert_called()
        self.assertEqual(test_api.latest_since, '1316241040941232128')


if __name__ == "__main__":
    unittest.main()
