from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Online store').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
data = spark.read.load('hdfs://localhost:9000/data/purchases.csv', format='csv', sep=',',
                       inferSchema='true', header='true')
data.registerTempTable('purchases')
data = spark.read.load('hdfs://localhost:9000/data/products.csv', format='csv', sep=',',
                       inferSchema='true', header='true')
data.registerTempTable('products')
data = spark.read.load('hdfs://localhost:9000/data/customers.csv', format='csv', sep=',',
                       inferSchema='true', header='true')
data.registerTempTable('customers')
spark.sql('''
    SELECT DISTINCT customers.personal_data, products.product_name
    FROM customers, products, purchases
    WHERE customers.id = purchases.customer_id
        AND products.id = purchases.product_id
        AND purchases.price = (
            SELECT MAX(price)
            FROM purchases
        )
''').show(truncate=False)

input('Enter Ctrl+C')
spark.stop()
