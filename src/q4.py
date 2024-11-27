from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TopThreeTickerAndDate").getOrCreate()

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
# Register as temp table for SQL queries
df.createOrReplaceTempView("stocks_data")

# Step 2: Create a query to calculate the date differences between the pairs
query_closest_prices = """
SELECT 
    a.Date AS current_date,
    a.ticker,
    a.close AS current_close,
    b.Date AS prior_date,
    b.close AS prior_close,
    ABS(DATEDIFF(a.Date, b.Date)) AS date_diff
FROM stocks_data a
JOIN stocks_data b 
    ON a.ticker = b.ticker 
    AND a.Date != b.Date
"""

# Execute the SQL query to get the date differences
closest_prices_df = spark.sql(query_closest_prices)

# Register the closest prices result as a temporary view for further queries
closest_prices_df.createOrReplaceTempView("closest_prices")

# Step 3: Filter for the closest prior date within a 30-day window and calculate the 30-day return
query_30_day_returns = """
SELECT 
    current_date,
    ticker,
    (current_close - prior_close) / prior_close * 100 AS return_30_day
FROM closest_prices
WHERE ABS(date_diff - 30) <= 1  -- Filter for dates with a 30-day difference
AND prior_close IS NOT NULL
"""

# Execute the SQL query for 30-day returns
returns_df = spark.sql(query_30_day_returns)

# Register the returns DataFrame as a temporary view for ranking
returns_df.createOrReplaceTempView("returns_data")

# Step 4: Rank the results by return_30_day and select the top 3 returns
query_top_returns = """
SELECT 
    current_date,
    ticker,
    return_30_day
FROM (
    SELECT 
        current_date,
        ticker,
        return_30_day,
        RANK() OVER (PARTITION BY ticker ORDER BY return_30_day DESC) AS rank
    FROM returns_data
) ranked
WHERE rank <= 3
"""

# Execute the SQL query to get top 3 returns
top_returns_df = spark.sql(query_top_returns)

# Show the final result
top_returns_df.show()

#top_returns_df.write.parquet("s3a://<your-bucket-name>/path/to/save/final_data.parquet")

spark.stop()