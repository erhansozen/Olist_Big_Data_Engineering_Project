import os
import zipfile
from zipfile import ZipFile as zf

# Unzip dataset which I downloaded from S3
with zf('Olist.zip', 'r') as zipObj:
    zipObj.extractall()


import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
conf = SparkConf().setMaster("local").setAppName("Missed_Deadlines")
spark = SparkSession.builder.getOrCreate()
print(spark)


from pyspark.sql import SQLContext
sqlContext = SQLContext(spark)


spark.conf.set("spark.sql.execution.arrow.enabled", "true")


spark = SparkSession.builder.getOrCreate()

# Spark data frames convert into SQL tables
spark_items = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("olist_order_items_dataset.csv")

spark_orders = spark.read.format("csv") \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .load("olist_orders_dataset.csv")

spark_products = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("olist_products_dataset.csv")

# SQL Tables to spark querries for late orders
spark_items.createOrReplaceTempView('items')
spark_orders.createOrReplaceTempView('orders')
spark_products.createOrReplaceTempView('products')

# SQL Query to pull late orders
spark_late_orders = spark.sql("""
SELECT i.order_id, i.seller_id, i.shipping_limit_date, i.price, i.freight_value,
       p.product_id, p.product_category_name,
       o.customer_id, o.order_status, o.order_purchase_timestamp, o.order_delivered_carrier_date,
       o.order_delivered_customer_date, o.order_estimated_delivery_date
FROM items AS i
JOIN orders AS o
ON i.order_id = o.order_id
JOIN products AS p
ON i.product_id = p.product_id
WHERE i.shipping_limit_date < o.order_delivered_carrier_date
""")

# Get csv file to upload S3
spark_late_orders.coalesce(1) \
                       .write \
                       .option("header", "true") \
                       .csv("/erho2/late_orders.csv")