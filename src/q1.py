from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StocksAverageReturn").getOrCreate()
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

# Load CSV data
df = spark.read.csv("/Users/tomershtrum/Desktop/intellij/data-engineering-home-assignment/stocks_data.csv", header=True)
df.createOrReplaceTempView("stocks_data")

# Step 1: Find the closest dates using self-join and compute the date difference
closest_dates_df = spark.sql("""
SELECT
    a.Date AS current_date,
    a.ticker,
    a.close AS current_close,
    b.Date AS closest_date,
    b.close AS closest_close,
    ABS(DATEDIFF(a.Date, b.Date)) AS date_diff
FROM stocks_data a
JOIN stocks_data b
    ON a.ticker = b.ticker
WHERE a.Date != b.Date
""")
closest_dates_df.createOrReplaceTempView("closest_dates")
# Step 2: Use a window function to find the closest date based on the minimum date difference
ranked_closest_dates_df = spark.sql("""
SELECT
    current_date,
    ticker,
    current_close,
    closest_date,
    closest_close,
    date_diff,
    RANK() OVER (PARTITION BY ticker, current_date ORDER BY date_diff) AS rank
FROM closest_dates
""")
ranked_closest_dates_df.createOrReplaceTempView("ranked_closest_dates")
# Step 3: Filter to keep only the closest date (rank = 1)
filtered_closest_dates_df = spark.sql("""
SELECT
    current_date,
    ticker,
    current_close,
    closest_date,
    closest_close
FROM ranked_closest_dates
WHERE rank = 1
""")
filtered_closest_dates_df.createOrReplaceTempView("filtered_closest_dates")

# Step 4: Calculate the daily return using the closest date
daily_returns_df = spark.sql("""
SELECT
    current_date,
    ticker,
    (current_close - closest_close) / closest_close * 100 AS daily_return
FROM filtered_closest_dates
""")
daily_returns_df.createOrReplaceTempView("daily_returns")

# Step 5: Compute the average daily return for all stocks for each date
result_df = spark.sql("""
SELECT
    current_date AS date,
    AVG(daily_return) AS average_return
FROM daily_returns
GROUP BY current_date
ORDER BY current_date ASC
""")

result_df.show()

#result_df.write.parquet("s3a://<your-bucket-name>/path/to/save/final_data.parquet")

spark.stop()
