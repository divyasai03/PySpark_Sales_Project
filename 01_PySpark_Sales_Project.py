# Databricks notebook source
# MAGIC %md
# MAGIC 1. IMPORT REQUIRED LIBRARIES

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

from pyspark.sql.functions import (
    col,
    trim,
    try_to_date,
    when,
    sum as spark_sum,
    count,
    avg,
    round
)

print("PySpark Sales Data Processing Project Started!")
print("Spark Version:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. DEFINE THE DATA SCHEMA

# COMMAND ----------

sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("city", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC 3. CREATE SAMPLE SALES DATA

# COMMAND ----------

sales_data = [
    ("1001", "C001", "Laptop",   2, 50000.0, "2026-10-01", "Hyderabad"),
    ("1002", "C002", "Mouse",    3,   500.0, "2026-10-01", "Chennai"),
    ("1003", "C003", "Keyboard", 1,  1500.0, "2026-10-02", "Hyderabad"),
    ("1004", "C004", "Mouse",    2,   500.0, "2026-10-02", "Bengaluru"),
    ("1005", "C005", "Laptop",   1, 50000.0, "2026-10-03", "Chennai"),
    ("1002", "C002", "Mouse",    3,   500.0, "2026-10-01", "Chennai"),
    ("1006", None,   "Monitor",  1, 12000.0, "2026-10-03", "Hyderabad"),
    ("1007", "C007", "Chair",   -2,  3000.0, "2026-10-04", "Chennai"),
    ("1008", "C008", "Desk",     1,  -100.0, "2026-10-04", "Bengaluru"),
    ("1009", "C009", "Headset",  2,   800.0, "not-a-date",  "Hyderabad"),
    ("1010", "C010", "Webcam",   1,  2500.0, "2026-10-05", "Bengaluru")
]

sales_df = spark.createDataFrame(
    sales_data,
    schema=sales_schema
)

print("\n--- SECTION 3: INPUT DATA ---")
print("Total input records:", sales_df.count())
sales_df.printSchema()
display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. CLEAN TEXT COLUMNS AND VALIDATE DATES

# COMMAND ----------

checked_df = (
    sales_df
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product", trim(col("product")))
    .withColumn("city", trim(col("city")))
    .withColumn(
        "parsed_date",
        try_to_date(col("order_date"), "yyyy-MM-dd")
    )
)

print("\n--- SECTION 4: DATA AFTER BASIC CLEANING ---")
display(checked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. IDENTIFY INVALID RECORDS

# COMMAND ----------

checked_df = checked_df.withColumn(
    "rejection_reason",
    when(
        col("customer_id").isNull() |
        (col("customer_id") == ""),
        "MISSING_CUSTOMER_ID"
    )
    .when(
        col("quantity").isNull() |
        (col("quantity") <= 0),
        "INVALID_QUANTITY"
    )
    .when(
        col("price").isNull() |
        (col("price") <= 0),
        "INVALID_PRICE"
    )
    .when(
        col("parsed_date").isNull(),
        "INVALID_DATE"
    )
    .otherwise(None)
)

rejected_df = checked_df.filter(
    col("rejection_reason").isNotNull()
)

valid_before_dedup_df = checked_df.filter(
    col("rejection_reason").isNull()
)

print("\n--- SECTION 5: VALIDATION RESULTS ---")
print("Input records:", sales_df.count())
print("Rejected records:", rejected_df.count())
print("Valid records before deduplication:",
      valid_before_dedup_df.count())

print("\nRejected records:")
display(rejected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. REMOVE DUPLICATE ORDERS

# COMMAND ----------

valid_df = valid_before_dedup_df.dropDuplicates(
    ["order_id"]
)

duplicates_removed = (
    valid_before_dedup_df.count() - valid_df.count()
)

print("\n--- SECTION 6: DEDUPLICATION RESULTS ---")
print("Duplicate records removed:", duplicates_removed)
print("Final valid records:", valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 7. CALCULATE REVENUE FOR EACH ORDER

# COMMAND ----------

valid_df = valid_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

print("\n--- SECTION 7: CLEANED SALES DATA ---")
display(
    valid_df.select(
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "parsed_date",
        "city",
        "revenue"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 8. PRODUCT-WISE REVENUE ANALYSIS

# COMMAND ----------

product_sales_df = (
    valid_df
    .groupBy("product")
    .agg(
        spark_sum("revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

print("\n--- SECTION 8: PRODUCT-WISE REVENUE ---")
display(product_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 9. CITY-WISE REVENUE ANALYSIS

# COMMAND ----------

city_sales_df = (
    valid_df
    .groupBy("city")
    .agg(
        spark_sum("revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

print("\n--- SECTION 9: CITY-WISE REVENUE ---")
display(city_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 10. PRODUCT-WISE QUANTITY AND REVENUE

# COMMAND ----------

product_summary_df = (
    valid_df
    .groupBy("product")
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

print("\n--- SECTION 10: PRODUCT SALES SUMMARY ---")
display(product_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 11. OVERALL SALES METRICS

# COMMAND ----------

sales_summary_df = valid_df.agg(
    count("order_id").alias("total_valid_orders"),
    spark_sum("revenue").alias("total_revenue"),
    round(
        avg("revenue"), 2
    ).alias("average_order_revenue")
)

print("\n--- SECTION 11: OVERALL SALES SUMMARY ---")
display(sales_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 12. VERIFY TOTAL REVENUE

# COMMAND ----------

print("\n--- SECTION 12: REVENUE VERIFICATION ---")

valid_df.selectExpr(
    "COUNT(*) AS valid_records",
    "SUM(revenue) AS verified_total_revenue"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 13. SAVE RESULTS AS DATABRICKS TABLES

# COMMAND ----------

current_catalog = spark.sql(
    "SELECT current_catalog()"
).first()[0]

current_schema = spark.sql(
    "SELECT current_schema()"
).first()[0]

print("\n--- SECTION 13: TABLE DESTINATION ---")
print("Catalog:", current_catalog)
print("Schema:", current_schema)

# Use quoted identifiers for catalog, schema and table names.
target_prefix = (
    f"`{current_catalog}`.`{current_schema}`"
)

valid_table = f"{target_prefix}.`cleaned_sales`"
rejected_table = f"{target_prefix}.`rejected_sales`"
product_table = f"{target_prefix}.`product_sales_summary`"
city_table = f"{target_prefix}.`city_sales_summary`"

valid_df.write.mode("overwrite").saveAsTable(valid_table)
rejected_df.write.mode("overwrite").saveAsTable(rejected_table)
product_sales_df.write.mode("overwrite").saveAsTable(product_table)
city_sales_df.write.mode("overwrite").saveAsTable(city_table)

print("Tables saved successfully:")
print(valid_table)
print(rejected_table)
print(product_table)
print(city_table)

# COMMAND ----------

# MAGIC %md
# MAGIC 14. READ BACK THE SAVED RESULTS

# COMMAND ----------

print("\n--- SECTION 14: VERIFY SAVED TABLES ---")

display(spark.table(valid_table))
display(spark.table(rejected_table))
display(spark.table(product_table))
display(spark.table(city_table))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 15. PROJECT COMPLETION SUMMARY

# COMMAND ----------

print("\n==========================================")
print("PYSPARK PROJECT SUMMARY")
print("==========================================")
print("Input records:", sales_df.count())
print("Rejected records:", rejected_df.count())
print("Duplicates removed:", duplicates_removed)
print("Final valid records:", valid_df.count())

print("Total revenue:",
      valid_df.agg(spark_sum("revenue")).first()[0])

print("Project processing and analysis completed!")


# COMMAND ----------

# MAGIC %md
# MAGIC STEP: CHECK YOUR EXISTING DATAFRAMES

# COMMAND ----------

print("Input records:", sales_df.count())
print("Rejected records:", rejected_df.count())
print("Valid records:", valid_df.count())

print("\nProduct-wise sales:")
display(product_sales_df)

print("\nCity-wise sales:")
display(city_sales_df)

print("\nOverall sales summary:")
display(sales_summary_df)


# COMMAND ----------

# MAGIC %md
# MAGIC DATA QUALITY CHECKS
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

input_count = sales_df.count()
rejected_count = rejected_df.count()
valid_count = valid_df.count()

# Check for duplicate order IDs in the final valid data
duplicate_count = (
    valid_df.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

# Check for invalid quantities and prices in the valid data
invalid_quantity_count = valid_df.filter(
    col("quantity").isNull() | (col("quantity") <= 0)
).count()

invalid_price_count = valid_df.filter(
    col("price").isNull() | (col("price") <= 0)
).count()

# Check for invalid dates in the valid data
invalid_date_count = valid_df.filter(
    col("parsed_date").isNull()
).count()

# Verify revenue total
revenue_total = valid_df.agg(
    {"revenue": "sum"}
).first()[0]

print("========== DATA QUALITY REPORT ==========")
print("Input records:", input_count)
print("Rejected records:", rejected_count)
print("Valid records:", valid_count)
print("Duplicate order IDs in valid data:", duplicate_count)
print("Invalid quantities in valid data:", invalid_quantity_count)
print("Invalid prices in valid data:", invalid_price_count)
print("Invalid dates in valid data:", invalid_date_count)
print("Total revenue:", revenue_total)

# Overall checks
assert input_count == 11, "Unexpected input count"
assert rejected_count == 4, "Unexpected rejected count"
assert valid_count == 6, "Unexpected valid count"
assert duplicate_count == 0, "Duplicate order IDs remain"
assert invalid_quantity_count == 0, "Invalid quantities remain"
assert invalid_price_count == 0, "Invalid prices remain"
assert invalid_date_count == 0, "Invalid dates remain"
assert revenue_total == 156500.0, "Revenue total does not match"

print("\nALL DATA QUALITY CHECKS PASSED!")


# COMMAND ----------

# MAGIC %md
# MAGIC Check Databricks storage

# COMMAND ----------

print("Current catalog:")
spark.sql("SELECT current_catalog()").show(truncate=False)

print("Current schema:")
spark.sql("SELECT current_schema()").show(truncate=False)

print("Available schemas:")
spark.sql("SHOW SCHEMAS").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Save Cleaned Data

# COMMAND ----------

# Save the existing cleaned DataFrame as a Delta table
valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.cleaned_sales")

print("Cleaned sales data saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the saved table

# COMMAND ----------

# Step 3: Verify the saved Delta table

saved_df = spark.table("workspace.default.cleaned_sales")

print("Saved record count:", saved_df.count())

print("Saved sales records:")
saved_df.orderBy("order_id").show(truncate=False)

print("Total revenue in saved table:")
saved_df.selectExpr("SUM(revenue) AS total_revenue").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Save the rejected records

# COMMAND ----------

# Step 4: Save rejected records for investigation

rejected_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rejected_sales")

print("Rejected sales records saved successfully!")

# COMMAND ----------

saved_rejected_df = spark.table("workspace.default.rejected_sales")

print("Rejected record count:", saved_rejected_df.count())
saved_rejected_df.select(
    "order_id", "customer_id", "quantity",
    "price", "order_date", "rejection_reason"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a final sales summary table

# COMMAND ----------

# Step 5: Create product-wise sales summary
from pyspark.sql.functions import sum as spark_sum

# Reuse the existing valid_df — do not recreate the dataset
product_summary_df = (
    valid_df
    .groupBy("product")
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

# Explicitly allow the saved table's schema to be replaced
spark.sql("""
    DROP TABLE IF EXISTS workspace.default.product_sales_summary
""")

product_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.product_sales_summary")

print("Product sales summary saved successfully!")

# Verify the saved summary
spark.table("workspace.default.product_sales_summary") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify all three saved tables

# COMMAND ----------

# Step 6: Verify all saved project tables

tables_to_check = {
    "cleaned_sales": 6,
    "rejected_sales": 4,
    "product_sales_summary": 4
}

for table_name, expected_count in tables_to_check.items():
    full_table_name = f"workspace.default.{table_name}"
    actual_count = spark.table(full_table_name).count()

    print(
        f"{table_name}: {actual_count} records "
        f"(expected {expected_count})"
    )

    assert actual_count == expected_count, (
        f"Record count mismatch in {table_name}"
    )

print("\nALL THREE SAVED TABLES VERIFIED SUCCESSFULLY!")

# COMMAND ----------

# MAGIC %md
# MAGIC Check whether RDD operations are supported

# COMMAND ----------

# Check whether Spark RDD operations are supported

try:
    test_rdd = spark.sparkContext.parallelize([1, 2, 3])
    print("RDD operations are supported.")
    print("RDD values:", test_rdd.collect())
except Exception as e:
    print("RDD operations are not available on this compute.")
    print("Reason:", str(e)[:500])