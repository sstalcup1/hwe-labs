import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load()

# Process the received data
query = df.withColumn("value", split("value", "\t")) \
            .selectExpr("value[0] as  marketplace", 
                        "value[1] as customer_id", 
                        "value[2] as review_id",
                        "value[3] as product_id",
                        "value[4] as product_parent",
                        "value[5] as product_title",
                        "value[6] as product_category",
                        "cast(value[7] as int) as star_rating",
                        "cast(value[8] as int) as helpful_votes",
                        "cast(value[9] as int) as total_votes",
                        "value[10] as vine",
                        "value[11] as verified_purchase",
                        "value[12] as review_headline",
                        "value[13] as review_body",
                        "cast(value[14] as date) as purchase_date", 
                        "current_timestamp() as review_timestamp") \
            .writeStream \
            .format("parquet") \
            .option("path", "s3a://hwe-fall-2023/sstalcup/bronze/reviews") \
            .option("checkpointLocation", "/tmp/kafka-checkpoint") \
            .start()

# Wait for the streaming query to finish
query.awaitTermination()

# Read written data
# read_written_df = spark.read.parquet("s3a://hwe-fall-2023/sstalcup/bronze/reviews")
# read_written_df.show(n=10)
# read_written_df.printSchema()

# Stop the SparkSession
spark.stop()