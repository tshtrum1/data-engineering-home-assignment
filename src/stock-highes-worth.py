from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockHighesWorth") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Load CSV data
df = spark.read.csv("s3://data-engineer-assignment-tomersht/input/stocks_data.csv", header=True)
df.createOrReplaceTempView("stocks_data")

# Step 1: calculate the average stock worth
avg_worth_df = spark.sql("""
SELECT
    ticker,
    AVG(close * volume) AS avg_stock_worth
FROM
    stocks_data
GROUP BY
    ticker
""")

avg_worth_df.createOrReplaceTempView("avg_worth")

# Step 2: Find the stock with the maximum average worth
result_df = spark.sql("""
SELECT
    ticker,
    avg_stock_worth AS value
FROM
    avg_worth
WHERE
    avg_stock_worth = (SELECT MAX(avg_stock_worth) FROM avg_worth)
""")

result_df.write.parquet("s3://data-engineer-assignment-tomersht/output/stock-highes-worth")

spark.stop()