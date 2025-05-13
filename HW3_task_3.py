from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("Step 3 - Total Purchases by Category").getOrCreate()

# Завантаження очищених даних
users_df = spark.read.option("header", True).csv("/Users/admin/Downloads/users.csv").na.drop()
purchases_df = spark.read.option("header", True).csv("/Users/admin/Downloads/purchases.csv").na.drop()
products_df = spark.read.option("header", True).csv("/Users/admin/Downloads/products.csv").na.drop()

# Конвертація типів
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
products_df = products_df.withColumn("price", col("price").cast("double"))

# Об'єднання датафреймів по product_id
joined_df = purchases_df.join(products_df, on="product_id")

# Обчислення суми покупок
result_df = joined_df.withColumn("total", col("quantity") * col("price")) \
    .groupBy("category") \
    .sum("total") \
    .withColumnRenamed("sum(total)", "total_spent") \
    .orderBy(col("total_spent").desc())

# Вивід результатів
result_df.show()
