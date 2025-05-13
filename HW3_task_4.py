from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Step 4 - Purchases by Category for Age 18-25").getOrCreate()

# Завантаження очищених даних
users_df = spark.read.option("header", True).csv("/Users/admin/Downloads/users.csv").na.drop()
purchases_df = spark.read.option("header", True).csv("/Users/admin/Downloads/purchases.csv").na.drop()
products_df = spark.read.option("header", True).csv("/Users/admin/Downloads/products.csv").na.drop()

# Конвертація типів
users_df = users_df.withColumn("age", col("age").cast("int"))
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
products_df = products_df.withColumn("price", col("price").cast("double"))

# Об'єднання таблиць
df = purchases_df.join(users_df, on="user_id").join(products_df, on="product_id")

# Фільтрація за віком 18–25
filtered_df = df.filter((col("age") >= 18) & (col("age") <= 25))

# Обчислення витрат і агрегація за категорією
result_df = filtered_df.withColumn("total", col("quantity") * col("price")) \
    .groupBy("category") \
    .sum("total") \
    .withColumnRenamed("sum(total)", "total_spent_18_25") \
    .orderBy(col("total_spent_18_25").desc())

# Вивід результатів
result_df.show()
