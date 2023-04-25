import json

from elasticsearch import Elasticsearch
import pprint
from constants import PRODUCT_INDEX


def request():
    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    searchBody = {
        "aggregations": {
            "sales_per_month": {
                "date_histogram": {
                    "field": "batch_receipt_date",
                    "calendar_interval": "month"
                },
                "aggregations": {
                    "product_name": {
                        "terms": {
                            "field": "product_name"
                        },
                        "aggregations": {
                            "products_sold": {
                                "terms": {
                                    "field": "amount_of_sold"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    response = client.search(index=PRODUCT_INDEX, body=searchBody, size=0)
    pprint.pprint(response)
    with open('response1.json', 'w') as file:
        json.dump(response, file, indent=4)


if __name__ == '__main__':
    request()
