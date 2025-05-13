from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Step 2 - Drop Nulls").getOrCreate()

# Завантаження CSV-файлів
users_df = spark.read.option("header", True).csv("/Users/admin/Downloads/users.csv")
purchases_df = spark.read.option("header", True).csv("/Users/admin/Downloads/purchases.csv")
products_df = spark.read.option("header", True).csv("/Users/admin/Downloads/products.csv")

# Видалення рядків з пропущеними значеннями
users_cleaned = users_df.na.drop()
purchases_cleaned = purchases_df.na.drop()
products_cleaned = products_df.na.drop()

# Вивід результатів для перевірки
print("Users (cleaned):")
users_cleaned.show()

print("Purchases (cleaned):")
purchases_cleaned.show()

print("Products (cleaned):")
products_cleaned.show()
