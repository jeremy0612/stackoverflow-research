from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, to_date, udf, explode
from pyspark.sql.types import ArrayType, StringType
import re
def extract_languages(string):
        lang_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
        if string is not None:
            return re.findall(lang_regex, string)
        
if __name__ == "__main__":
    # Initialize the spark job
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://admin:password@172.21.0.5/qa_db?authSource=admin') \
        .config('spark.mongodb.output.uri', 'mongodb://admin:password@172.21.0.5/qa_db?authSource=admin') \
        .getOrCreate()
    # Read data from Q&A collections 
    question_df = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("collection","question")\
    .load()

    answer_df = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("collection","answer")\
    .load()

    #  Convert the datatype
    question_df = question_df.withColumn("CreationDate", to_date(question_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
       .withColumn("ClosedDate", to_date(question_df.ClosedDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    answer_df = answer_df.withColumn("CreationDate", to_date(answer_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    # Filter outliers
    question_df = question_df.withColumn("OwnerUserId", when(question_df.OwnerUserId == "NA", None).otherwise(question_df.OwnerUserId))
    answer_df = answer_df.withColumn("OwnerUserId", when(answer_df.OwnerUserId == "NA", None).otherwise(answer_df.OwnerUserId))

    # Programming languages appearance frequency in questions
    extracter = udf(lambda z: extract_languages(z), ArrayType(StringType()))
    question_filter = question_df.select("Title", extracter("Body").alias("Languages"))
    question_exploded = question_filter.withColumn("ExplodedLanguages", explode(col("Languages")))
    question_grouped = question_exploded.groupBy("ExplodedLanguages").count()
    question_grouped.show()



