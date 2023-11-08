import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

silver_schema = StructType([
    StructField("marketplace", StringType(), nullable=False)
    ,StructField("customer_id", StringType(), nullable=False)
    ,StructField("review_id", StringType(), nullable=False)
    ,StructField("product_id", StringType(), nullable=False)
    ,StructField("product_parent", StringType(), nullable=False)
    ,StructField("product_title", StringType(), nullable=False)
    ,StructField("product_category", StringType(), nullable=False)
    ,StructField("star_rating", IntegerType(), nullable=False)
    ,StructField("helpful_votes", IntegerType(), nullable=False)
    ,StructField("total_votes", IntegerType(), nullable=False)
    ,StructField("vine", StringType(), nullable=False)
    ,StructField("verified_purchase", StringType(), nullable=False)
    ,StructField("review_headline", StringType(), nullable=False)
    ,StructField("review_body", StringType(), nullable=False)
    ,StructField("purchase_date", DateType(), nullable=False)
    ,StructField("review_timestamp", TimestampType(), nullable=False)
    ,StructField("customer_name", StringType(), nullable=False)
    ,StructField("gender", StringType(), nullable=False)
    ,StructField("date_of_birth", DateType(), nullable=False)
    ,StructField("city", StringType(), nullable=False)
    ,StructField("state", StringType(), nullable=False)
])

# Define a streaming dataframe using readStream on top of the bronze reviews directory on S3
silver_data = spark.read \
                .schema(silver_schema) \
                .parquet("s3a://hwe-fall-2023/sstalcup/silver/reviews") \
                .createOrReplaceTempView("silver_data")

# Product Dimension
product_dim = spark.sql("SELECT DISTINCT\
                            product_id, \
                            product_parent, \
                            product_title, \
                            product_category \
                        FROM \
                            silver_data")
#product_dim.write.mode("overwrite").parquet("s3a://hwe-fall-2023/sstalcup/gold/product_dim")

# Customer Dimension
customer_dim = spark.sql("SELECT DISTINCT \
                            customer_id, \
                            customer_name, \
                            gender, \
                            date_of_birth, \
                            city, \
                            state \
                        FROM \
                            silver_data")
customer_dim.write.mode("overwrite").parquet("s3a://hwe-fall-2023/sstalcup/gold/customer_dim")


# Date Dimension
date_dim = spark.sql("SELECT DISTINCT\
                            purchase_date, \
                            YEAR(purchase_date) AS year, \
                            MONTH(purchase_date) AS month, \
                            DATE_FORMAT(purchase_date, 'MMMM') AS long_month, \
                            CONCAT(MONTH(purchase_date), ' ', YEAR(purchase_date)) AS year_month \
                        FROM \
                            silver_data")
date_dim.write.mode("overwrite").parquet("s3a://hwe-fall-2023/sstalcup/gold/date_dim")

## Stop the SparkSession
spark.stop()