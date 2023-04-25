import json
from elasticsearch import Elasticsearch
from constants import PRODUCT_INDEX, PURCHASE_INDEX


def init_es_db():
    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])

    if client.indices.exists(index=PRODUCT_INDEX):
        client.indices.delete(index=PRODUCT_INDEX)
    client.indices.create(index=PRODUCT_INDEX)

    if client.indices.exists(index=PURCHASE_INDEX):
        client.indices.delete(index=PURCHASE_INDEX)
    client.indices.create(index=PURCHASE_INDEX)

    client.indices.close(index=PRODUCT_INDEX)
    client.indices.close(index=PURCHASE_INDEX)

    db_analyser_settings = {
        'analysis': {
            'filter': {
                'stop_words': {
                    'type': 'stop',
                    'stopwords': '_russian_'
                },
                'filter_settings': {
                    'type': 'snowball',
                    'language': 'Russian'
                }
            },
            'analyzer':
            {
                'db_analyzer':
                {
                    'type': 'custom',
                    'tokenizer': 'standard',
                    'filter': [
                        'stop_words',
                        'filter_settings',
                        'lowercase'
                    ]
                }
            }
        }
    }

    client.indices.put_settings(index=PRODUCT_INDEX, body=db_analyser_settings)
    client.indices.put_settings(index=PURCHASE_INDEX, body=db_analyser_settings)

    productMapping = {
        'properties': {
            'product_name': {
                'type': 'text',
                'fielddata': True
            },
            'batch_receipt_date': {
                'type': 'date',
                'format': 'yyyy-MM-dd'
            },
            'amount_in_stock': {
                'type': 'integer'
            },
            'amount_of_sold': {
                'type': 'integer'
            },
            'price': {
                'type': 'integer'
            },
            'description': {
                'type': 'text',
                'analyzer': 'db_analyzer',
                'fielddata': True
            }
        }
    }

    purchaseMapping = {
        'properties': {
            'customer_id': {
                'type': 'keyword'
            },
            'personal_data': {
                'type': 'text',
                'analyzer': 'db_analyzer',
                'fielddata': True
            },
            'purchase_date': {
                'type': 'date',
                'format': 'yyyy-MM-dd'
            },
            'product_id': {
                'type': 'keyword'
            },
            'product_name': {
                'type': 'text',
                'fielddata': True
            },
            'amount': {
                'type': 'integer'
            },
            'price': {
                'type': 'integer'
            }
        }
    }

    client.indices.put_mapping(
        index=PRODUCT_INDEX,
        doc_type=PRODUCT_INDEX,
        include_type_name='true',
        body=productMapping
    )
    client.indices.put_mapping(
        index=PURCHASE_INDEX,
        doc_type=PURCHASE_INDEX,
        include_type_name='true',
        body=purchaseMapping
    )

    client.indices.open(index=PRODUCT_INDEX)
    client.indices.open(index=PURCHASE_INDEX)

    with open('products.json') as file:
        products = json.load(file)

    for product in products:
        try:
            client.index(
                index=product['index'],
                doc_type=product['doc_type'],
                id=product['id'],
                body=product['body']
            )
        except Exception:
            pass

    with open('purchases.json') as file:
        purchases = json.load(file)

    for purchase in purchases:
        try:
            client.index(
                index=purchase['index'],
                doc_type=purchase['doc_type'],
                id=purchase['id'],
                body=purchase['body']
            )
        except Exception:
            pass
    print('База данных elasticsearch проинициализирована')


if __name__ == '__main__':
    init_es_db()
