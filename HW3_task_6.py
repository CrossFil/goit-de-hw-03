from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round

spark = SparkSession.builder.appName("Step 6 - Top 3 Categories").getOrCreate()

# Завантаження очищених даних
users_df = spark.read.option("header", True).csv("/Users/admin/Downloads/users.csv").na.drop()
purchases_df = spark.read.option("header", True).csv("/Users/admin/Downloads/purchases.csv").na.drop()
products_df = spark.read.option("header", True).csv("/Users/admin/Downloads/products.csv").na.drop()

# Приведення типів
users_df = users_df.withColumn("age", col("age").cast("int"))
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
products_df = products_df.withColumn("price", col("price").cast("double"))

# Об'єднання таблиць
df = purchases_df.join(users_df, on="user_id").join(products_df, on="product_id")

# Фільтрація за віком 18–25
filtered_df = df.filter((col("age") >= 18) & (col("age") <= 25))

# Обчислення витрат по кожному рядку
with_total = filtered_df.withColumn("total", col("quantity") * col("price"))

# Підрахунок сумарних витрат по кожній категорії
category_totals = with_total.groupBy("category").agg(_sum("total").alias("category_total"))

# Загальна сума витрат
total_sum = category_totals.agg(_sum("category_total").alias("total_all")).collect()[0]["total_all"]

# Обчислення частки
result_df = category_totals.withColumn("share", round(col("category_total") / total_sum * 100, 2))

# Виведення топ-3 категорій за витратами
result_df.select("category", "category_total", "share").orderBy(col("share").desc()).limit(3).show()
