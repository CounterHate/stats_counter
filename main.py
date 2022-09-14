from datetime import timedelta, datetime
import es


def process_date(curret_date):
    # curret_date = datetime.now()
    week_ago = curret_date - timedelta(days=6)

    categories = es.get_categories()
    for category in categories:
        print(f'Processing {category["category"]}')
        category_week_ago = es.count_tweets(date=week_ago, category=category)
        category_today = es.count_tweets(date=curret_date, category=category)
        data = {
            'date': int(curret_date.timestamp()),
            'short_date': f'{curret_date.day}-{curret_date.month}-{curret_date.year}',
            'count': category_today,
            'total': False,
            'growth': category_today - category_week_ago,
            'entity': 'categories',
            'entity_value': category['category']
        }
        # print(json.dumps(data, indent=2))
        es.add_to_stats(
            data=data, id=f'{curret_date.day}-{curret_date.month}-{curret_date.year}-{category["category"]}')


def main():
    today = datetime.now()
    for i in range(54):
        print(f'Week #{i}')
        current_date = today - timedelta(days=1 + 7 * i)
        print(current_date)
        process_date(curret_date=current_date)


if __name__ == "__main__":
    main()
