# PySpark Sales Data Processing Project

## Project Objective

Build data-processing workflows using PySpark to clean, validate, transform, and analyze sales data using DataFrames and RDDs.

## Technologies

- Python
- PySpark DataFrames
- PySpark RDDs
- Databricks Serverless compute
- Delta tables
- Prepare.sh Spark workspace for RDD demonstrations

## Project Structure

```text
pyspark-dataframes-rdd-project/
├── README.md
├── 01_PySpark_Sales_Project.py
├── rdd_sales_demo.py
└── PySpark_DataFrames_RDD_Project_Report.pdf
```

## Part 1: DataFrame Workflow

1. Create the sample sales DataFrame with an explicit schema.
2. Clean text columns and validate dates.
3. Separate valid and rejected records.
4. Identify invalid customer IDs, quantities, prices, and dates.
5. Remove duplicate orders.
6. Calculate revenue for each order.
7. Analyze sales by product and city.
8. Calculate overall sales metrics.
9. Run data-quality checks using assertions.
10. Save cleaned data, rejected records, and sales summaries as Delta tables.
11. Read the saved tables and verify the results.

## DataFrame Final Results

- Input records: 11
- Rejected records: 4
- Valid records before deduplication: 7
- Duplicate records removed: 1
- Valid records retained: 6
- Total revenue: 156,500.00

## Saved Delta Tables

- `workspace.default.cleaned_sales`
- `workspace.default.rejected_sales`
- `workspace.default.product_sales_summary`
- `workspace.default.city_sales_summary`

## Part 2: RDD Distributed Processing

The `rdd_sales_demo.py` file demonstrates the following RDD concepts:

- `parallelize()` — create an RDD from a Python collection.
- `map()` — transform each element, such as squaring numbers.
- `filter()` — select elements that satisfy a condition.
- `reduce()` — combine elements into one result.
- `flatMap()` — transform sentences into individual words.
- `collect()`, `count()`, and `first()` — retrieve results and inspect an RDD.

### RDD Demo Results

- Squared numbers: `[1, 4, 9, 16, 25, 36]`
- Even numbers: `[2, 4, 6]`
- Sum of even numbers: `12`
- Words: `['Apache', 'Spark', 'PySpark', 'RDD']`
- Sales records: 5
- High-value sales: 3
- Total revenue in the RDD sample: 102,500

The RDD sample uses a separate dataset from the DataFrame workflow, so the revenue totals differ.

## How to Run

### DataFrame Project

1. Open Databricks.
2. Import or open `01_PySpark_Sales_Project.py` as a notebook.
3. Run the notebook using compatible Databricks compute.
4. Verify the data-quality checks and saved Delta tables.

### RDD Project

1. Open a regular PySpark environment, such as Prepare.sh.
2. Open `rdd_sales_demo.py`.
3. Run the script using the environment's supported PySpark execution method.
4. Compare the output with the expected results.

**Note:** Databricks Serverless does not support direct `SparkContext` access. The RDD demonstration therefore runs separately from the DataFrame workflow.

## Project Scope and Limitations

- The project uses small sample datasets for learning and demonstration.
- The code demonstrates distributed-processing concepts but does not benchmark performance on large datasets.
- The DataFrame and RDD components were demonstrated in separate environments.
- Creating Delta tables requires appropriate Databricks catalog, schema, and table permissions.

## Resume Summary

Created PySpark data-processing workflows using DataFrames and RDDs to clean, validate, transform, and analyze sales data. Implemented duplicate removal, data-quality validation, revenue calculations, product and city aggregations, Delta-table storage, and RDD transformations using `map()`, `filter()`, `reduce()`, and `flatMap()`.
