from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.types import *
from datetime import datetime
PRODUCT_INDEX = 'product'
PURCHASE_INDEX = 'purchase'
client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
spark_session = SparkSession.builder.appName("csv").getOrCreate()
search_body = {
    "size": 500,
    "query": {
        "match_all": {}
    }
}
products = client.search(index=PRODUCT_INDEX, body=search_body)['hits']['hits']
purchases = client.search(index=PURCHASE_INDEX, body=search_body)['hits']['hits']

customer_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("personal_data", StringType(), False)
])

purchase_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("purchase_date", DateType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("amount", IntegerType(), False),
    StructField("price", IntegerType(), False),
])

product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("batch_receipt_date", DateType(), False),
    StructField("amount_in_stock", IntegerType(), False),
    StructField("amount_of_sold", IntegerType(), False),
    StructField("price", IntegerType(), False),
    StructField("description", StringType(), False),
    StructField("image_link", StringType(), False)
])

customer_table = []
product_table = []
purchase_table = []
for purchase in purchases:
    customer_table.append((
        int(purchase['_source']['customer_id']),
        purchase['_source']['personal_data']
    ))
    purchase_table.append((
        int(purchase['_id']),
        int(purchase['_source']['customer_id']),
        datetime.strptime(purchase['_source']['purchase_date'], "%Y-%m-%d"),
        int(purchase['_source']['product_id']),
        int(purchase['_source']['amount']),
        int(purchase['_source']['price'])
    ))

for product in products:
    product_table.append((
        int(product['_id']),
        product['_source']['product_name'],
        datetime.strptime(product['_source']['batch_receipt_date'], "%Y-%m-%d"),
        int(product['_source']['amount_in_stock']),
        int(product['_source']['amount_of_sold']),
        int(product['_source']['price']),
        product['_source']['description'],
        product['_source']['image_link']
    ))

customer_df = spark_session.createDataFrame(customer_table, customer_schema)
purchase_df = spark_session.createDataFrame(purchase_table, purchase_schema)
product_df = spark_session.createDataFrame(product_table, product_schema)

customer_df.write.csv(path='hdfs://localhost:9000/data/customers.csv', mode='overwrite', header=True)
purchase_df.write.csv(path='hdfs://localhost:9000/data/purchases.csv', mode='overwrite', header=True)
product_df.write.csv(path='hdfs://localhost:9000/data/products.csv', mode='overwrite', header=True)
