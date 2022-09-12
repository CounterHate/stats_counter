import requests
import json
import datetime

ES = 'https://es.dc9.dev:9200'
TWEETS_INDEX = 'tweets'
HATE_CATEGORIES_INDEX = 'hate_categories'
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


def count_tweets_for_category(category) -> int:
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

                }],

            },
        }
    }
    r = requests.post(f'{ES}/{TWEETS_INDEX}/_count',
                      auth=AUTH, data=json.dumps(query), headers=HEADERS)
    return r.json()['count']
