import json

from elasticsearch import Elasticsearch
import pprint
from constants import PURCHASE_INDEX


def request():
    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    searchBody = {
        "query": {
            "range": {
                "purchase_date": {
                    "gte": "now-2M/M",
                    "lte": "now/M"
                }
            }
        },
        "aggregations": {
            "total_price": {
                "sum": {
                    "field": "price"
                }
            }
        }
    }
    response = client.search(index=PURCHASE_INDEX, body=searchBody)
    pprint.pprint(response)
    with open('response2.json', 'w') as file:
        json.dump(response, file, indent=4)


if __name__ == '__main__':
    request()
