import csv
import requests
import json
import es
import time

URL = "http://hatify.test/api/stats"


def save_to_csv(filename, data):
    # open the file in the write mode
    with open(filename, 'a') as f:
        # # create the csv writer
        writer = csv.writer(f)

        # # write a row to the csv file
        writer.writerow(data)


def save_to_databse(data):
    r = requests.post(URL,
                      data=json.dumps(data), headers=es.HEADERS)
    print(r)
    time.sleep(2)
