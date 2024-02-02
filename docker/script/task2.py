from pyspark.sql import SparkSession
from pyspark.sql.functions import when, to_date, count, sum
from pyspark.sql.functions import explode, col, udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
import re
def extract_domain(string):
        dom_regex = r"https?://(.*?)/"
        if string is not None:
            return re.findall(dom_regex, string)
        
if __name__ == "__main__":
    # Initialize the spark job
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://admin:password@mongo/qa_db?authSource=admin') \
        .config('spark.mongodb.output.uri', 'mongodb://admin:password@mongo/qa_db?authSource=admin') \
        .getOrCreate()
    # Read data from Q&A collections 
    question_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("collection","questions")\
        .load()

    answer_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("collection","answers")\
        .load()

    #  Convert the datatype
    question_df = question_df.withColumn("CreationDate", to_date(question_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
       .withColumn("ClosedDate", to_date(question_df.ClosedDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
        .withColumn("OwnerUserId", question_df.OwnerUserId.cast(IntegerType()))
    answer_df = answer_df.withColumn("CreationDate", to_date(answer_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
        .withColumn("OwnerUserId", answer_df.OwnerUserId.cast(IntegerType()))
    # Filter outliers
    question_df = question_df.withColumn("OwnerUserId", when(question_df.OwnerUserId == "NA", None).otherwise(question_df.OwnerUserId))
    answer_df = answer_df.withColumn("OwnerUserId", when(answer_df.OwnerUserId == "NA", None).otherwise(answer_df.OwnerUserId))
    # Most used domain
    extracter = udf(lambda z: extract_domain(z), ArrayType(StringType()))
    question_filter = question_df.select("Title", extracter("Body").alias("Domain"))
    question_exploded = question_filter.withColumn("ExplodedDomain", explode(col("Domain")))
    question_grouped = question_exploded.groupBy("ExplodedDomain").count().orderBy("count", ascending=False)
    question_grouped.show() # Result

    # Filter the question_exploded DataFrame for validation
    question_grouped.where(
        (col("ExplodedDomain") == "img7.imageshack.us") |
        (col("ExplodedDomain") == "www.cs.bham.ac.uk") |
        (col("ExplodedDomain") == "groups.csail.mit.edu") |
        (col("ExplodedDomain") == "fiddlertool.com") |
        (col("ExplodedDomain") == "www.dynagraph.org") |
        (col("ExplodedDomain") == "images.mydomain.com")
    ).show()





