from datetime import datetime, timedelta
import es


def main():
    categories = es.get_categories()
    # for i in range(365):
    for category in categories:
        curret_date = datetime.now()
        curret_date = curret_date.replace(hour=23, minute=59, second=59)
        curret_date = curret_date - timedelta(days=i+1)
        count = es.count_tweets_for_category(category, curret_date)
        data = {
            'date': int(curret_date.timestamp()),
            'short_date': f'{curret_date.day}-{curret_date.month}-{curret_date.year}',
            'count': count,
            'growth': 0,
            'total': True,
            'entity': 'categories',
            'entity_value': f'total-{category["category"]}'
        }
        es.add_to_stats(
            data=data,  id=f'{curret_date.day}-{curret_date.month}-{curret_date.year}-total-{category["category"]}')


if __name__ == "__main__":
    main()
