import json
import requests

ES = 'https://es.dc9.dev:9200'
INDEX = 'stats'
AUTH = ('dc9', 'hohC2wix')
HEADERS = {"content-type": "application/json"}


def create_index():
    with open('mapping.json', 'r') as f:
        mapping = json.loads(f.read())
    r = requests.put(
        url=f'{ES}/{INDEX}', data=json.dumps(mapping), auth=AUTH, headers=HEADERS)
    print(r.json())


def main():
    create_index()


if __name__ == "__main__":
    main()
