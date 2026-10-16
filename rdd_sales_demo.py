"""
PySpark RDD Demo: Distributed Processing Fundamentals

Topics:
- Creating an RDD with parallelize()
- map()
- filter()
- reduce()
- flatMap()
- Basic RDD actions: collect(), count(), first()

Run in a regular PySpark environment that supports SparkContext,
such as the Prepare.sh Spark workspace. Databricks Serverless does not
support direct SparkContext access.
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("RDD-Sales-Demo").getOrCreate()
    sc = spark.sparkContext

    try:
        # 1. Create an RDD from a Python list.
        numbers = sc.parallelize([1, 2, 3, 4, 5, 6])
        print("Original numbers:", numbers.collect())

        # 2. map(): apply a function to each element.
        squared = numbers.map(lambda x: x * x)
        print("Squared numbers:", squared.collect())
        # Expected: [1, 4, 9, 16, 25, 36]

        # 3. filter(): keep elements that satisfy a condition.
        even_numbers = numbers.filter(lambda x: x % 2 == 0)
        print("Even numbers:", even_numbers.collect())
        # Expected: [2, 4, 6]

        # 4. reduce(): combine elements into one result.
        even_total = even_numbers.reduce(lambda a, b: a + b)
        print("Sum of even numbers:", even_total)
        # Expected: 12

        # 5. flatMap(): turn each sentence into multiple words.
        sentences = sc.parallelize(["Apache Spark", "PySpark RDD"])
        words = sentences.flatMap(lambda sentence: sentence.split(" "))
        print("Words:", words.collect())
        # Expected: ['Apache', 'Spark', 'PySpark', 'RDD']

        # 6. Basic RDD actions.
        print("Number of values:", numbers.count())  # Expected: 6
        print("First value:", numbers.first())      # Expected: 1

        # 7. Small sales example: (product, sale_amount).
        sales = sc.parallelize([
            ("Laptop", 50000),
            ("Mouse", 500),
            ("Keyboard", 1500),
            ("Laptop", 50000),
            ("Mouse", 500),
        ])

        high_value_sales = sales.filter(lambda item: item[1] >= 1000)
        print("High-value sales:", high_value_sales.collect())
        # Expected:
        # [('Laptop', 50000), ('Keyboard', 1500), ('Laptop', 50000)]

        total_revenue = (
            sales
            .map(lambda item: item[1])
            .reduce(lambda a, b: a + b)
        )
        print("Total sales revenue:", total_revenue)
        # Expected: 102500

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
