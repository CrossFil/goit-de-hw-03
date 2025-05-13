from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Step 1").getOrCreate()

users_df = spark.read.csv("/Users/admin/Downloads/users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("/Users/admin/Downloads/purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("/Users/admin/Downloads/products.csv", header=True, inferSchema=True)

users_df.show(3)
purchases_df.show(3)
products_df.show(3)
