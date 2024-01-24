from pyspark.sql import SparkSession
from pyspark.sql.functions import when, to_date, count, sum
from pyspark.sql.window import Window
import re
def extract_domain(string):
        dom_regex = r"(?!String\.Format|support\.html|my\.ini|feedparser\.parse)[\w\-\.]+\.[\w\-]{2,}"
        if string is not None:
            return re.findall(dom_regex, string)
        
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

    join_expr = question_df.OwnerUserId  == answer_df.OwnerUserId
    answer_renamed_df = answer_df \
                        .withColumnRenamed("CreationDate", "reorder_CreationDate") \
                        .withColumnRenamed("Score","reorder_Score")
    question_df.join(answer_renamed_df, join_expr, "inner")\
                .drop(answer_renamed_df.OwnerUserId)\
                .select("OwnerUserId", "CreationDate", "Score")
    score_total_window = Window.partitionBy("OwnerUserId")\
                .orderBy("CreationDate")
    question_df.withColumn("ScoreTotal", sum("Score").over(score_total_window))\
                .filter(question_df.OwnerUserId.isNotNull())\
                .groupBy("OwnerUserId","CreationDate")\
                .agg(sum("Score").alias("ScoreTotal"))\
                .sort("CreationDate")\
                .show(50)

    

    # question_score_df = question_df.select("Id", "CreationDate")
    # answer_score_df = answer_df.select("ParentId", "CreationDate", "OwnerUserId", "Score").where(answer_df.OwnerUserId == 26)
    # score_df = question_score_df.join(answer_score_df, question_score_df.Id == answer_score_df.ParentId, "inner")
    # score_df = score_df.groupBy("OwnerUserId").sum("Score")
    # score_df.show(50)

    # # Most used domain
    # extracter = udf(lambda z: extract_domain(z), ArrayType(StringType()))
    # question_filter = question_df.select("Title", extracter("Body").alias("Domain"))
    # question_exploded = question_filter.withColumn("ExplodedDomain", explode(col("Domain")))
    # question_grouped = question_exploded.groupBy("ExplodedDomain").count().orderBy("count", ascending=False)
    # question_grouped.show()



