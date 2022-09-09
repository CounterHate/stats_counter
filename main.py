from datetime import timedelta, datetime
import requests
import json

ES = 'https://es.dc9.dev:9200'
TWEETS_INDEX = 'tweets'
HATE_CATEGORIES_INDEX = 'hate_categories'
STATS_INDEX = 'stats'
HEADERS = {'content-type': 'application/json'}
AUTH = ('dc9', 'hohC2wix')


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
                    'lte': int(datetime(date.year, date.month, date.day, 0, 0).timestamp()),
                }
            },
        })

    r = requests.post(f'{ES}/{TWEETS_INDEX}/_count',
                      data=json.dumps(query), auth=AUTH, headers=HEADERS)
    return r.json()['count']


def add_to_stats(data, id):
    r = requests.put(f'{ES}/{STATS_INDEX}/_doc/{id}',
                      data=json.dumps(data), headers=HEADERS, auth=AUTH)
    print(r.json())


def process_date(curret_date):
    # curret_date = datetime.now()
    week_ago = curret_date - timedelta(days=6)

    categories = get_categories()
    for category in categories:
        print(f'Processing {category["category"]}')
        category_week_ago = count_tweets(date=week_ago, category=category)
        category_today = count_tweets(date=curret_date, category=category)
        data = {
            'date': int(curret_date.timestamp()),
            'count': category_today,
            'growth': category_today - category_week_ago,
            'entity': 'categories',
            'entity_value': category['category']
        }
        add_to_stats(data=data, id=f'{curret_date.day}-{curret_date.month}-{curret_date.year}')

        # for word in category["words"]:
        #     word_week_ago = count_tweets(date=week_ago, word=word)
        #     word_today = count_tweets(word=word)
        #     data = {
        #         'date': int(curret_date.timestamp()),
        #         'count': word_today,
        #         'growth': word_today - word_week_ago,
        #         'entity': 'words',
        #         'entity_value': word
        #     }
        #     add_to_stats(data=data)

def main():
    today = datetime.now()
    for i in range(54):
        print(f'Week #{i}')
        current_date = today - timedelta(days=5 + 7 * i)
        process_date(curret_date=current_date)

if __name__ == "__main__":
    main()
