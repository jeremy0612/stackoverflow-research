from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, to_date, sum
from pyspark.sql.functions import  col,  date_format, window
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.window import Window
        
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
    # # Valid userID
    # question_df = question_df.filter(question_df.OwnerUserId.isNotNull())
    # answer_df = answer_df.filter(answer_df.OwnerUserId.isNotNull())
    
    # Score
    question_score = question_df.select("Score", "CreationDate","OwnerUserId")
    answer_score = answer_df.select("Score", "CreationDate","OwnerUserId")
    union_score = question_score.union(answer_score)
    union_score.show()
    # Tumbling Window
    window_score = union_score \
        .groupBy(window(col("CreationDate"), "1 day"),"OwnerUserId") \
        .agg(sum("Score").alias("TotalScore"))\
        .orderBy("window.start","OwnerUserId")\
        .select("OwnerUserId",col("window.start").alias("Date"),"TotalScore")  
    window_score.show()

    # window_score.where((col("OwnerUserId") == 26) |
    #                    (col("OwnerUserId") == 83) ).show() 
    
    





