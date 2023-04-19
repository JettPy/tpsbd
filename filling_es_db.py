import json
from elasticsearch import Elasticsearch
from tqdm import tqdm

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
product_index = "product"
purchase_index = "purchase"

# Create exercises index
if client.indices.exists(index=product_index):
    client.indices.delete(index=product_index)
client.indices.create(index=product_index)
# Create trainers index
if client.indices.exists(index=purchase_index):
    client.indices.delete(index=purchase_index)
client.indices.create(index=purchase_index)

client.indices.close(index=product_index)
client.indices.close(index=purchase_index)
print("Indexes created")

# Analysis
online_store_analysis = {
    "analysis": {
        "filter": {
            "russian_stop_words": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "filter_ru_sn": {
                "type": "snowball",
                "language": "Russian"
            }
        },
        "analyzer":
        {
            "ru_analyzer":
            {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "russian_stop_words",
                    "filter_ru_sn"
                ]
            }
        }
    }
}
client.indices.put_settings(index=product_index, body=online_store_analysis)
client.indices.put_settings(index=purchase_index, body=online_store_analysis)
print("Settings putted")

# Mapping
product_mapping = {
    "properties": {
        "product_name": {
            "type": "text",
            "analyzer": "ru_analyzer",
            "fielddata": True
        },
        "batch_receipt_date": {
            "type": "date",
            "format": "yyyy-MM-dd"
        },
        "amount_in_stock": {
            "type": "integer"
        },
        "amount_of_sold": {
            "type": "integer"
        },
        "price": {
            "type": "integer"
        },
        "description": {
            "type": "text",
            "analyzer": "ru_analyzer",
            "fielddata": True
        }
    }
}

purchase_mapping = {
    "properties": {
        "customer_id": {
            "type": "keyword"
        },
        "personal_data": {
            "type": "text",
            "analyzer": "ru_analyzer",
            "fielddata": True
        },
        "purchase_date": {
            "type": "date",
            "format": "yyyy-MM-dd"
        },
        "product_id": {
            "type": "keyword"
        },
        "product_name": {
            "type": "text",
            "analyzer": "ru_analyzer",
            "fielddata": True
        },
        "amount": {
            "type": "integer"
        },
        "price": {
            "type": "integer"
        }
    }
}
client.indices.put_mapping(
    index=product_index,
    doc_type=product_index,
    include_type_name="true",
    body=product_mapping
)
client.indices.put_mapping(
    index=purchase_index,
    doc_type=purchase_index,
    include_type_name="true",
    body=product_mapping
)
print("Mapping putted")

# Filling indexes up
client.indices.open(index=product_index)
client.indices.open(index=purchase_index)

with open("products.json") as file:
    data = json.load(file)

for product in tqdm(data):
    try:
        client.index(
            index=product["index"],
            doc_type=product["doc_type"],
            id=product["id"],
            body=product["body"]
        )
    except Exception as e:
        print(e)

print("Products loaded")

with open("purchases.json") as file:
    data = json.load(file)

for purchase in tqdm(data):
    try:
        client.index(
            index=purchase["index"],
            doc_type=purchase["doc_type"],
            id=purchase["id"],
            body=purchase["body"]
        )
    except Exception as e:
        print(e)

print("Purchase loaded")