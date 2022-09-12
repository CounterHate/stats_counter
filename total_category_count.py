from datetime import datetime
import es


def main():
    categories = es.get_categories()
    for category in categories:
        curret_date = datetime.now()
        count = es.count_tweets_for_category(category)
        data = {
            'date': int(curret_date.timestamp()),
            'count': count,
            'growth': 0,
            'entity': 'categories',
            'entity_value': f'total-{category["category"]}'
        }
        es.add_to_stats(
            data=data,  id=f'{curret_date.day}-{curret_date.month}-{curret_date.year}-total-{category["category"]}')


if __name__ == "__main__":
    main()
