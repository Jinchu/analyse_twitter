#%%
from json import dump
import sys
import os
import argparse
from configparser import ConfigParser
from elasticsearch import Elasticsearch

from elasticsearch_index_conf import set_es_index


#%%
def set_arguments():
    """ Function for argument parser """

    parser = argparse.ArgumentParser(
        description = 'Upload tweets from file. Helper script')
    parser.add_argument('-c', dest = 'config', type = str,
                        help = 'Path to the configuration file')
    parser.add_argument('-v', dest = 'debug', action = 'store_true',
                        help = 'Enable verbose output. Optional')
    parser.add_argument('-i', dest = 'index', type = str,
                        help = 'Name of the index to be used.')
    parser.add_argument('-p', dest = 'path', type = str,
                        help = 'Path to file where the timeline was stored')
    parser.set_defaults(debug = False)

    arguments = parser.parse_args()
    if arguments.config is None:
        if arguments.debug:
            print('Using default path to config')
        arguments.config = '/etc/tweepy/twitter.conf'

    return arguments, parser


#%%
def upload_records(file_path, es_handle, index, debug = False):
    if debug:
        print("Handling file [{}] for the index [{}]".format(file_path, index))

    with open (file_path, 'r') as handle:
        record_json = handle.read()

    res = es_handle.bulk(record_json, index=index)
    if res['errors']:
        if debug:
            print("At least some ingests FAILED!")
        return False
    else:
        if debug:
            print("Clean run!")
    return True


#%%
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

    try:
        elasitc_url = config['ElasticSearch']['url']
    except KeyError:
        print('Incomplete or broken configuration file [%s]. Missing:' % args.config)
        print('    [ElasticSearch]')
        print('    url = https://xxxxxxxxxx.xxx')
        return -1

    es = Elasticsearch(
            [elasitc_url],
            http_auth=(config['ElasticSearch']['auth_user'], elastic_pass),
            use_ssl = (config['ElasticSearch']['use_ssl'] == 'True'),
            verify_certs = (config['ElasticSearch']['verify_certs'] == 'True')
        )

    set_es_index(index_name, es_handle=es, debug=args.debug)

    records = os.listdir(args.path)
    for rec in records:
        if rec.endswith('txt'):
            full_path = args.path + '/' + rec
            upload_records(full_path, es_handle = es, index = index_name, debug=args.debug)
        else:
            print('Skipping file [{}] as irrelevant'. format(rec))


#%%
if __name__ == "__main__":
    elastic_pass = os.environ['ELASTICSEARCH_PASS']
    sys.exit(main())

