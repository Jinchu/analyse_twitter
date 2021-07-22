#!/usr/bin/python3
""" Program for fetching documents using twitter API and pushing them to ElasticSearch. """
import sys
import os
import argparse
from configparser import ConfigParser
import tweepy
from elasticsearch_tweepy import ElasticSearchTweepy
from elasticsearch import Elasticsearch


def set_arguments():
    """ Function for argument parser """
    parser = argparse.ArgumentParser(
        description = 'Fetch tweets Twitter´s developer API. ' +
                        'Push the tweets to elastic search.\n' +
                        'Networking debuging can be done useing curl: ' +
                        'curl http://localhost:9200/_cluster/health\?pretty\n\n' +
                        'To promote safe development practises pass the elasticsearch password ' +
                        'as an enviromental variable ELASTICSEARCH_PASS.')
    parser.add_argument('-c', dest = 'config', type = str,
                        help = 'Path to the configuration file')
    parser.add_argument('-t', dest = 'target', type = str,
                        help = 'The twitter handle of the target user')
    parser.add_argument('-v', dest = 'debug', action = 'store_true',
                        help = 'Enable verbose output. Optional')
    parser.add_argument('-s', dest = 'term', type = str,
                        help = 'Search tweets with this term.')
    parser.add_argument('-m', dest = 'mode', type = str,
                        help = 'Mode of operation: user, term, list, generate.')
    parser.add_argument('-j', dest = 'proc_count', type = int,
                        help = 'Number of parallel processes used in list mode.')
    parser.add_argument('-i', dest = 'index', type = str,
                        help = 'Name of the index to be used.')
    parser.add_argument('-p', dest = 'path', type = str,
                        help = 'Path to file where the timeline will be stored. Used with _to_file')
    parser.add_argument('-q', dest = 'time_path', type = str,
                        help = 'Path to timestamp file')
    parser.set_defaults(debug = False, mode = 'user', proc_count = 4)

    arguments = parser.parse_args()
    if arguments.config is None:
        if arguments.debug:
            print('Using default path to config')
        arguments.config = '/etc/tweepy/twitter.conf'

    return arguments, parser


def register_tweepy_to_twitter(api_conf):
    """ Give Twitter Api keys so that Tweepy library can fetch tweets. """
    t_auth = tweepy.OAuthHandler(api_conf['api_key'], api_conf['api_secret'])
    t_auth.set_access_token(api_conf['acc_token'], api_conf['acc_secret'])
    return ElasticSearchTweepy(t_auth)


def main():
    args, parser = set_arguments()
    if args is None:
        return -1

    config = ConfigParser()
    try:
        config.read(args.config)
    except:
        print('ERROR: File %s in not a valid configuration.' % args.config)
        return -1

    if args.index is None:
        try:
            index_name = config['Local Storage']['index_name']
        except KeyError:
            print("No name for index defined. Put it in configuration or use -i handle.\n" + 
                  "Display the usage by -h.")
            return -1
    else:
        index_name = args.index

    if not args.mode == "term_to_file" and not args.mode == "user_to_file":
        try:
            elastic_pass = os.environ['ELASTICSEARCH_PASS']
        except KeyError:
            print('Please give the password of your ElasticSearch service as environmental ' +
                'parameter: ELASTICSEARCH_PASS')
            return -1

    try:
        elasitc_url = config['ElasticSearch']['url']
    except KeyError:
        print('Incomplete or broken configuration file [%s]. Missing:' % args.config)
        print('    [ElasticSearch]')
        print('    url = https://xxxxxxxxxx.xxx')
        return -1

    twitter_api_keys_tokens = {}
    try:
        twitter_api_keys_tokens['acc_token'] = os.environ['TWITTER_ACC_TOKEN']
        twitter_api_keys_tokens['acc_secret'] = os.environ['TWITTER_ACC_SECRET']
        twitter_api_keys_tokens['api_secret'] = os.environ['TWITTER_API_SECRET']
        twitter_api_keys_tokens['api_key'] = os.environ['TWITTER_API_KEY']
    except KeyError:
        # Use the api keys and tokens from the configuration file only when no env are defined.
        twitter_api_keys_tokens = config['Twitter API']

    twitter_api = register_tweepy_to_twitter(twitter_api_keys_tokens)

    if args.debug:
        print(twitter_api.me().name)

    if args.mode == "user":
        if args.target is None:
            print('When using this mode a target user must be specified.\n')
            parser.print_help()
            return -1
        es = Elasticsearch(
            [elasitc_url],
            http_auth=(config['ElasticSearch']['auth_user'], elastic_pass),
            use_ssl = (config['ElasticSearch']['use_ssl'] == 'True'),
            verify_certs = (config['ElasticSearch']['verify_certs'] == 'True')
        )
        twitter_api.set_es_index(index_name, es, debug = args.debug)
        twitter_api.user_timeline_to_es(args.target, es_handle = es, debug = args.debug)
    elif args.mode == "user_to_file":
        if args.target is None:
            print('When using this mode a target user must be specified.\n')
            parser.print_help()
            return -1
        if args.path is None:
            print("In this mode a path to storage file needs to be defined.")
            parser.print_help()
            return -1
        twitter_api.index = index_name
        twitter_api.user_timeline_to_file(args.target, file_path=args.path)

    elif args.mode == "list":
        es = Elasticsearch(
            [elasitc_url],
            http_auth=(config['ElasticSearch']['auth_user'], elastic_pass),
            use_ssl = (config['ElasticSearch']['use_ssl'] == 'True'),
            verify_certs = (config['ElasticSearch']['verify_certs'] == 'True')
        )
        twitter_api.set_es_index(index_name, es, args.debug)
        storage_path = config['Local Storage']['users_path']
        twitter_api.list_timeline_to_es(storage_path, args.proc_count, es_handle = es,
                                        debug = args.debug)
    elif args.mode == "term":
        if args.term is None:
            print("When using this mode a search term is required!\n")
            parser.print_help()
            return -1
        es = Elasticsearch(
            [elasitc_url],
            http_auth=(config['ElasticSearch']['auth_user'], elastic_pass),
            use_ssl = (config['ElasticSearch']['use_ssl'] == 'True'),
            verify_certs = (config['ElasticSearch']['verify_certs'] == 'True')
        )
        twitter_api.set_es_index(index_name, es, args.debug)
        twitter_api.search_term_to_es(args.term, es_handle = es, debug = args.debug)

    elif args.mode == "generate":
        if args.target is None:
            print("When using this mode a target user must be specified.\n")
            parser.print_help()
            return -1
        storage_path = config['Local Storage']['users_path']
        twitter_api.save_friends_file(args.target, storage_path, args.debug)
        twitter_api.clean_up_friends_file(storage_path, args.debug)

    elif args.mode == "term_to_file":
        if args.term is None:
            print("When using this mode a search term is required!\n")
            parser.print_help()
            return -1
        if args.path is None:
            print("In this mode a path to storage file needs to be defined.")
            parser.print_help()
            return -1
        twitter_api.index = index_name
        twitter_api.search_term_to_file(args.term, file_path = args.path,
                                        time_stamp = args.time_path, debug = args.debug)

    elif args.mode == "analyse_file":
        if args.path is None:
            print("Deprecated: In this mode a path to storage file (pickle) needs to be defined.")
            parser.print_help()
            return -1
        return -1

    elif args.mode == "clean":
        storage_path = config['Local Storage']['users_path']
        twitter_api.clean_up_friends_file(storage_path, args.debug)
    else:
        print("ERROR: unknown mode")
        return -1

    return 0


if __name__ == "__main__":
    sys.exit(main())

