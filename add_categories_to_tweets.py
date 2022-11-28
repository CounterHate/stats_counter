import es
import requests
import json
import datetime
import time
import random


def save_wrong_tweets_to_file(tweet_id):
    with open('wrong_tweets.txt', 'a') as f:
        f.write(f"{tweet_id}\n")


def update_tweet_in_index(tweet, data):
    r = requests.put(f"{es.ES}/{es.TWEETS_INDEX}/_doc/{tweet['_id']}", data=json.dumps(
        data), auth=es.AUTH, headers=es.HEADERS)
    print(r.json())
    time.sleep(r.elapsed.total_seconds())


def add_data_to_tweet(tweet, categories, phrases):
    date = ""
    hate_categoriess = []

    if "keywords" not in tweet.keys() or tweet["keywords"] == []:
        content_array = tweet["content"].replace(',', ' ').replace(
            '.', ' ').replace('?', ' ').replace('!', ' ').replace('"', ' ').lower().split(' ')
        keywords = list(set(content_array).intersection(phrases))
        tweet["keywords"] = keywords

    if "posted_utime" not in tweet.keys():
        print(json.dumps(tweet, indent=2))
        raise Exception

    for k in tweet["keywords"]:
        for category in categories:
            if k in category["words"] and category["category"] not in hate_categoriess:
                hate_categoriess.append(category["category"])

    datetime_time = datetime.datetime.fromtimestamp(
        int(tweet["posted_utime"])/1000)
    day = datetime_time.day
    month = datetime_time.month
    year = datetime_time.year
    date = f"{day}-{month}-{year}"

    tweet["date"] = date
    tweet["hate_category"] = hate_categoriess

    return tweet


def get_phrases():
    query = {"size": 10000}
    r = requests.get(
        f"{es.ES}/{es.PHRASES_INDEX}/_search",
        data=json.dumps(query),
        auth=es.AUTH,
        headers=es.HEADERS,
    )
    phrases = []
    for p in r.json()["hits"]["hits"]:
        phrases.append(p["_source"]["phrase"].lower())
    return phrases


def get_tweets_for_category(category):
    words = ""
    for index, word in enumerate(category["words"]):
        if index < len(category["words"]) - 1:
            words += f"{word},"
        else:
            words += f"{word}"
    query = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [{"match": {
                    "content": words
                }}, {"match": {
                    "is_retweet": False
                }}],
                "must_not": [{
                    "exists": {
                        "field": "date"
                    }
                }],
            }

        }
    }
    r = requests.get(f"{es.ES}/{es.TWEETS_INDEX}/_search",
                     data=json.dumps(query), headers=es.HEADERS, auth=es.AUTH)
    tweets = []
    for hit in r.json()["hits"]["hits"]:
        tweets.append(hit)
    return tweets


def get_categories():
    r = requests.get(f"{es.ES}/{es.HATE_CATEGORIES_INDEX}/_search",
                     headers=es.HEADERS, auth=es.AUTH)
    categories = []
    for hit in r.json()["hits"]["hits"]:
        categories.append(hit["_source"])
    return random.sample(categories, len(categories))


def main():
    categories = get_categories()
    phrases = get_phrases()
    for cat in categories:
        tweets = get_tweets_for_category(cat)
        for tweet in tweets:
            # print(json.dumps(tweet["_source"], indent=2))
            if "date" in tweet.keys() and "hate_category" in tweet.keys():
                continue
            try:
                new_tweet = add_data_to_tweet(
                    tweet["_source"], categories, phrases)
                update_tweet_in_index(tweet, new_tweet)
            except Exception as e:
                save_wrong_tweets_to_file(tweet["_id"])


if __name__ == '__main__':
    main()
