import es
from datetime import datetime, timedelta
import time
from save_data import save_to_databse


def main():
    categories = es.get_categories()
    start = time.time()
    num_of_days = 1
    for i in range(num_of_days):
        curret_date = datetime.now()
        curret_date = curret_date.replace(hour=23, minute=59, second=59)
        curret_date = curret_date - timedelta(days=i)
        for category in categories:
            print(
                f'Processing {category["category"]} for date - {curret_date}')
            authors = es.get_authors_from_es(
                category=category, date=curret_date)

            month = curret_date.month
            if month < 10:
                month = f'0{month}'

            day = curret_date.day
            if day < 10:
                day = f'0{day}'

            for author in authors:
                data = {
                    'short_date': f'{curret_date.year}-{month}-{day}',
                    'count': author["count"],
                    'growth': 0,
                    'entity': 'authors',
                    'entity_value': author["author"],
                    'hate_speech_category': category["category"],
                    'author': None
                }
                print(data)
                save_to_databse(data=data)
    print(f"{num_of_days} processed in {time.time()-start}")


if __name__ == "__main__":
    main()
