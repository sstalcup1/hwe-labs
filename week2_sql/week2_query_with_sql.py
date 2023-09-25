from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_replace

### Setup: Create a SparkSession
spark = SparkSession.builder \
        .appName("week2App") \
        .master("local[1]") \
        .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("review_view")

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
reviews = reviews.withColumn("review_timestamp", current_timestamp())

# Question 4: How many records are in the reviews dataframe? 
row_count = reviews.count()
#print(row_count) #returns 145431

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
reviews.show(n=5, truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
# Create new dataframe
product_category = reviews.select("product_category")

# Look at first 50 rows
#product_category.show(n=50) # I notice that every value is 'Digital_Video_Games'

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
result = spark.sql("WITH helpful_rank_cte AS ( \
                        SELECT \
                            product_title, \
                            helpful_votes, \
                            RANK() OVER (ORDER BY helpful_votes DESC) AS review_rank\
                        FROM \
                            review_view) \
                   SELECT \
                        product_title, \
                        helpful_votes \
                   FROM \
                        helpful_rank_cte \
                   WHERE \
                        review_rank = 1")
#result.show()

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
result = spark.sql("SELECT \
                        COUNT(review_id) AS 5_star_count \
                   FROM \
                        review_view WHERE star_rating = 5")
#result.show()

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
cast_df = spark.sql("SELECT \
                        CAST(star_rating AS INT), \
                        CAST(helpful_votes AS INT), \
                        CAST(total_votes AS INT) \
                    FROM \
                        review_view")
#cast_df.show(n=10)

# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
result = spark.sql("WITH date_rank_cte AS ( \
                        SELECT \
                            purchase_date, \
                            COUNT(*) AS purchases, \
                            RANK() OVER (ORDER BY COUNT(*) DESC) AS date_rank\
                        FROM \
                            review_view \
                        GROUP BY \
                            purchase_date) \
                   SELECT \
                        purchase_date, \
                        purchases \
                   FROM \
                       date_rank_cte \
                   WHERE \
                        date_rank = 1")
#result.show()

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
reviews.write.mode("overwrite").csv(r"C:\Users\scouts\Documents\HWE\lab-writes", header=True)

### Teardown
# Stop the SparkSession
spark.stop
