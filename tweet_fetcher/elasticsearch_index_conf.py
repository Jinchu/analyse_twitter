"""
Functions for setting up an ElasticSearch index for tweets
"""

def set_es_index(index_name, es_handle, debug = False):
    """ Set the index to be used. """

    if es_handle.indices.exists(index=index_name):
        if debug:
            print("index %s exists" % index_name)
    else:
        if debug:
            print("index %s must be created" % index_name)
        create_index(index_name, es_handle)

def create_index(index_name, es_handle):
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
                "time_of_day": {
                    "type": "long"
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

    return es_handle.indices.create(index=index_name, body = request_body)
