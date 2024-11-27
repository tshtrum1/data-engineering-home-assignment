from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockHighesWorth").getOrCreate()

"""
spark = SparkSession.builder \
    .appName("StocksAverageReturn") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Specify the S3 path to the CSV file
s3_path = "s3a://your-bucket-name/path/to/your-file.csv"
"""

# Load CSV data (Spark infers the schema automatically)
df = spark.read.csv("/Users/tomershtrum/Desktop/intellij/data-engineering-home-assignment/stocks_data.csv", header=True)
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
    avg_stock_worth
FROM
    avg_worth
WHERE
    avg_stock_worth = (SELECT MAX(avg_stock_worth) FROM avg_worth)
""")

result_df.show()

#result_df.write.parquet("s3a://<your-bucket-name>/path/to/save/final_data.parquet")

spark.stop()