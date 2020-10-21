import unittest
import json

import twitter_es_schema

class TestPopulateWithTweet(unittest.TestCase):
    def test_empty_schema(self):
         schema = twitter_es_schema.TwitterEsSchema()
         with self.assertRaises(Exception) as context:
            tweet_json = schema.get_json()

    def test_tweet_user_mentions(self):
        with open('./test_data/tweet_user_mentions.json', 'r') as handle:
            content = handle.read()

        test_tweet = json.loads(content)
        schema = twitter_es_schema.TwitterEsSchema()
        schema.populate(test_tweet)

        self.assertFalse(schema.empty)
        self.assertEqual(schema.tweet['id'], 1301026162362195971)
        self.assertEqual(schema.tweet['entities']['user_mentions'][1]['screen_name'], 'TeamYouTube')
        self.assertFalse(schema.tweet['is_retweet_status'])
        self.assertFalse(schema.tweet['is_quote_status'])

        schema_json = schema.get_json()
        self.assertTrue("2020-09-02T05:16:23" in schema_json)
        self.assertTrue("@timestamp" in schema_json)
        self.assertFalse("a href" in schema_json)

    def test_retweet_media(self):
        with open('./test_data/retweet_media.json', 'r') as handle:
            content = handle.read()
        test_tweet = json.loads(content)
        schema = twitter_es_schema.TwitterEsSchema()
        schema.populate(test_tweet)

        self.assertFalse(schema.empty)
        self.assertEqual(schema.tweet['id'], 1287635516226248706)
        self.assertTrue(schema.tweet['is_retweet_status'])
        self.assertFalse(schema.tweet['is_quote_status'])
        with self.assertRaises(KeyError) as contex:
            schema.tweet['retweet_status']['id']

        schema_json = schema.get_json()
        self.assertFalse('profile_background_image' in schema_json)
        self.assertFalse('sizes' in schema_json)
        self.assertTrue("1287489225982652418" in schema_json)

    def test_quote_tweet_retweet(self):
        with open('./test_data/quote_tweet.json', 'r') as handle:
            content = handle.read()
        test_tweet = json.loads(content)
        schema = twitter_es_schema.TwitterEsSchema()
        schema.populate(test_tweet)

        self.assertFalse(schema.empty)
        self.assertEqual(schema.tweet['id'], 1293677087669321734)
        self.assertTrue(schema.tweet['is_retweet_status'])
        self.assertTrue(schema.tweet['is_quote_status'])

        schema_json = schema.get_json()
        self.assertFalse('media_url' in schema_json)
        self.assertFalse('profile_image' in schema_json)
        self.assertTrue('2020-08-12T22:33:47' in schema_json)
        self.assertTrue('2020-08-12T22:18:02' in schema_json)

    def test_quote_tweet(self):
        with open('./test_data/quote_tweet_mikko.json', 'r') as handle:
            content = handle.read()

        test_tweet = json.loads(content)
        schema = twitter_es_schema.TwitterEsSchema()
        schema.populate(test_tweet)

        self.assertFalse(schema.empty)
        self.assertEqual(schema.tweet['id'], 1302696994704678913)
        self.assertFalse(schema.tweet['is_retweet_status'])
        self.assertTrue(schema.tweet['is_quote_status'])

        schema_json = schema.get_json()
        self.assertFalse('media_url' in schema_json)
        self.assertFalse('profile_image' in schema_json)
        self.assertTrue('1302507328642543617' in schema_json)
        self.assertTrue('2009-03-10T06:53:11' in schema_json)
        self.assertTrue('2020-09-06T19:55:41' in schema_json)

if __name__ == "__main__":
    unittest.main()
