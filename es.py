import requests
import json
import datetime

ES = 'https://es.dc9.dev:9200'
TWEETS_INDEX = 'tweets'
HATE_CATEGORIES_INDEX = 'hate_categories'
PHRASES_INDEX = "stream_phrases"
STATS_INDEX = 'stats'
HEADERS = {'content-type': 'application/json'}
AUTH = ('dc9', 'hohC2wix')


def add_to_stats(data, id):
    r = requests.put(f'{ES}/{STATS_INDEX}/_doc/{id}',
                     data=json.dumps(data), headers=HEADERS, auth=AUTH)
    print(r.json())


def get_categories():
    hate_categories = []
    r = requests.get(f'{ES}/{HATE_CATEGORIES_INDEX}/_search',
                     auth=AUTH, headers=HEADERS)
    for hit in r.json()["hits"]["hits"]:
        hate_categories.append(hit["_source"])
    return hate_categories


def count_tweets(date=None, category=None, word=None) -> int:
    query = {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        'lang': "pl",
                    },
                }, {
                    'match': {
                        'is_retweet': False,
                    },

                }],

            },
        }
    }

    if (category):
        # convert category array to string
        hate_words_string = ""
        for w in category['words']:
            hate_words_string += f'{w}, '
        query['query']['bool']['must'].append({
            'match': {
                'keywords': hate_words_string,
            },
        })

    if (word):
        query['query']['bool']['must'].append({
            'match': {
                'content': word,
            },
        })

    if (date):

        query['query']['bool']['must'].append({
            'range': {
                'posted_utime': {
                    'lte': int(datetime.datetime(date.year, date.month, date.day, 0, 0).timestamp()),
                }
            },
        })

    r = requests.post(f'{ES}/{TWEETS_INDEX}/_count',
                      data=json.dumps(query), auth=AUTH, headers=HEADERS)
    return r.json()['count']


def count_tweets_for_category(category, date) -> int:
    hate_words_string = ""
    for w in category['words']:
        hate_words_string += f'{w}, '
    query = {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        'lang': "pl",
                    },

                }, {
                    'match': {
                        'is_retweet': False,
                    },

                }, {
                    'match': {
                        'keywords': hate_words_string,
                    },
                }, {
                    'range': {
                        'posted_utime': {
                            'lte': int(date.timestamp())
                        }
                    }}],

            },
        }
    }
    r = requests.post(f'{ES}/{TWEETS_INDEX}/_count',
                      auth=AUTH, data=json.dumps(query), headers=HEADERS)
    return r.json()['count']


def get_authors_from_es(category, date):
    words_string = ""
    for index, word in enumerate(category["words"]):
        if index == len(category["words"])-1:
            words_string += f"{word}"
            continue
        words_string += f"{word}, "
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "lang": "pl"
                        }
                    },
                    {
                        "match": {
                            "is_retweet": False
                        }
                    },
                    {
                        "match": {
                            "keywords": words_string
                        }
                    }, {
                        'range': {
                            'posted_utime': {
                                'lte': int(date.timestamp())
                            }
                        }}
                ]
            }
        },
        "size": 0,
        "aggs": {
            "authors": {
                "terms": {
                    "field": "author_username.keyword",
                    "size": 10000
                }
            }
        }
    }
    r = requests.get(f'{ES}/{TWEETS_INDEX}/_search',
                     data=json.dumps(query), headers=HEADERS, auth=AUTH)
    authors = []
    for author in r.json()['aggregations']['authors']['buckets']:
        authors.append({"author": author['key'], "count": author["doc_count"]})
    return authors


def get_words_from_es(category):
    words_string = ""
    for index, word in enumerate(category["words"]):
        if index == len(category["words"])-1:
            words_string += f"{word}"
            continue
        words_string += f"{word}, "
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "lang": "pl"
                        }
                    },
                    {
                        "match": {
                            "is_retweet": False
                        }
                    },
                    {
                        "match": {
                            "keywords": words_string
                        }
                    }
                ]
            }
        },
        "size": 0,
        "aggs": {
            "words": {
                "terms": {
                    "field": "keywords.keyword",
                    "size": 10000
                }
            }
        }
    }
    r = requests.get(f'{ES}/{TWEETS_INDEX}/_search',
                     data=json.dumps(query), headers=HEADERS, auth=AUTH)
    authors = []
    for word in r.json()['aggregations']['words']['buckets']:
        authors.append({"word": word['key'], "count": word["doc_count"]})
    return authors
