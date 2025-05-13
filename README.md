# Apache Spark Data Analysis Project

Project Description

This project demonstrates data analysis using Apache Spark with Python (PySpark). The task involves working with three CSV files representing user data, product purchases, and product information. The goal is to clean the data, perform aggregation, and generate insights, particularly focused on product categories and user age groups.

Input Data
- users.csv
  Contains info (user_id, name, age, email)
- purchases.csv
Contains info (purchase_id, user_id, product_id, date, quantity)
- products.csv
Contains info (product_id, product_name, category, price)

## Task Steps
1. Load Data
Load all three CSV files into separate Spark DataFrames.
2. Clean Data
Remove any rows with missing (null) values in all three datasets.
3. Total Purchases by Category
Calculate the total monetary value of purchases for each product category.
4. Purchases by Category for Users Aged 18–25
Calculate the total amount spent per category by users aged between 18 and 25 (inclusive).
5. Percentage of Spending by Category for Age Group 18–25
For users aged 18–25, determine the percentage contribution of each product category to their total spending.
6. Top 3 Categories by Spending Share (Ages 18–25)
Identify the top three product categories with the highest percentage of total spending among users aged 18–25.
