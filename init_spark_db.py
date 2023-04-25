from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
spark = SparkSession.builder.appName('Online store').getOrCreate()

searchBody = {
    'size': 1000,
    'query': {
        'match_all': {}
    }
}

products = client.search(index='product', body=searchBody)['hits']['hits']
purchases = client.search(index='purchase', body=searchBody)['hits']['hits']

customer_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('personal_data', StringType(), False)
])

purchase_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('customer_id', IntegerType(), False),
    StructField('purchase_date', DateType(), False),
    StructField('product_id', IntegerType(), False),
    StructField('amount', IntegerType(), False),
    StructField('price', IntegerType(), False),
])

product_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('product_name', StringType(), False),
    StructField('batch_receipt_date', DateType(), False),
    StructField('amount_in_stock', IntegerType(), False),
    StructField('amount_of_sold', IntegerType(), False),
    StructField('price', IntegerType(), False),
    StructField('description', StringType(), False),
    StructField('image_link', StringType(), False)
])

customer_table = []
product_table = []
purchase_table = []
for purchase in purchases:
    customer_table.append((
        purchase['_source']['customer_id'],
        purchase['_source']['personal_data']
    ))
    purchase_table.append((
        purchase['_id'],
        purchase['_source']['customer_id'],
        datetime.strptime(purchase['_source']['purchase_date'], '%Y-%m-%d'),
        purchase['_source']['product_id'],
        purchase['_source']['amount'],
        purchase['_source']['price']
    ))

for product in products:
    product_table.append((
        product['_id'],
        product['_source']['product_name'],
        datetime.strptime(product['_source']['batch_receipt_date'], '%Y-%m-%d'),
        product['_source']['amount_in_stock'],
        product['_source']['amount_of_sold'],
        product['_source']['price'],
        product['_source']['description'],
        product['_source']['image_link']
    ))

customer_df = spark.createDataFrame(customer_table, customer_schema)
purchase_df = spark.createDataFrame(purchase_table, purchase_schema)
product_df = spark.createDataFrame(product_table, product_schema)

customer_df.write.csv(path='hdfs://localhost:9000/data/customers.csv', mode='overwrite', header=True)
purchase_df.write.csv(path='hdfs://localhost:9000/data/purchases.csv', mode='overwrite', header=True)
product_df.write.csv(path='hdfs://localhost:9000/data/products.csv', mode='overwrite', header=True)
