from datetime import datetime, timedelta
import es
import requests
import json
import time


def add_to_databse(data):
    r = requests.post('http://hatify.test/api/stats',
                      data=json.dumps(data), headers=es.HEADERS)
    print(r)
    time.sleep(r.elapsed.total_seconds())


def main():
    categories = es.get_categories()
    for i in range(365):
        for category in categories:
            print(f'Processing {category["category"]}')
            curret_date = datetime.now()
            curret_date = curret_date.replace(hour=23, minute=59, second=59)
            curret_date = curret_date - timedelta(days=i+1)

            month = curret_date.month
            if month < 10:
                month = f'0{month}'

            day = curret_date.day
            if day < 10:
                day = f'0{day}'

            count = es.count_tweets_for_category(category, curret_date)
            data = {
                'short_date': f'{curret_date.year}-{month}-{day}',
                'count': count,
                'growth': 0,
                'entity': 'categories',
                'entity_value': f'{category["category"]}'
            }
            # print(data)
            add_to_databse(data=data)


if __name__ == "__main__":
    main()
