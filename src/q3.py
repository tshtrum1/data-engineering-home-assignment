from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockMostVolatile").getOrCreate()

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

df = spark.read.csv("/Users/tomershtrum/Desktop/intellij/data-engineering-home-assignment/stocks_data.csv", header=True)
df.createOrReplaceTempView("stocks_data")

# Step 1: Fill missing closing prices
filled_prices_df = spark.sql("""
SELECT
    ticker,
    Date,
    close,
    COALESCE(close,
        FIRST_VALUE(close) OVER (PARTITION BY ticker ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        FIRST_VALUE(close) OVER (PARTITION BY ticker ORDER BY Date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS filled_close
FROM stocks_data
""")

filled_prices_df.createOrReplaceTempView("filled_prices")

# Step 2: Calculate daily returns
daily_returns_df = spark.sql("""
SELECT
    ticker,
    Date,
    filled_close AS close,
    LAG(filled_close) OVER (PARTITION BY ticker ORDER BY Date) AS prev_close,
    (filled_close - LAG(filled_close) OVER (PARTITION BY ticker ORDER BY Date)) / LAG(filled_close) OVER (PARTITION BY ticker ORDER BY Date) AS daily_return
FROM filled_prices
""")

daily_returns_df = daily_returns_df.filter("daily_return IS NOT NULL")
daily_returns_df.createOrReplaceTempView("daily_returns")

# Step 3: Calculate annualized volatility
volatility_df = spark.sql("""
SELECT
    ticker,
    STDDEV(daily_return) AS daily_stddev,
    STDDEV(daily_return) * SQRT(252) AS annualized_volatility
FROM daily_returns
GROUP BY ticker
""")

volatility_df.createOrReplaceTempView("volatility_table")

# Step 4: Find the most volatile stock
most_volatile_df = spark.sql("""
SELECT
    ticker,
    annualized_volatility
FROM volatility_table
ORDER BY annualized_volatility DESC
LIMIT 1
""")

most_volatile_df.show()

#most_volatile_df.write.parquet("s3a://<your-bucket-name>/path/to/save/final_data.parquet")

spark.stop()