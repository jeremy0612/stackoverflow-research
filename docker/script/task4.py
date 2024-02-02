from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, to_date, sum
from pyspark.sql.functions import  col , window
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from datetime import datetime

START = '01-01-2008'
END = '01-01-2009'

start_date = datetime.strptime(START, '%d-%m-%Y')
end_date = datetime.strptime(END, '%d-%m-%Y')
# Convert to YYYY-MM-DD format
START = start_date.strftime('%Y-%m-%d')
END = end_date.strftime('%Y-%m-%d')

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

    #  Convert the datatype
    question_df = question_df.withColumn("CreationDate", to_date(question_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
       .withColumn("ClosedDate", to_date(question_df.ClosedDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
        .withColumn("OwnerUserId", question_df.OwnerUserId.cast(IntegerType()))
    # Filter outliers
    question_df = question_df.withColumn("OwnerUserId", when(question_df.OwnerUserId == "NA", None).otherwise(question_df.OwnerUserId))
    
    # # Valid userID
    # question_df = question_df.filter(question_df.OwnerUserId.isNotNull())
    # answer_df = answer_df.filter(answer_df.OwnerUserId.isNotNull())
    
    # Score
    question_score = question_df.select("Score", "CreationDate","OwnerUserId")
    
    # Tumbling Window
    window_score = question_score \
        .groupBy(window(col("CreationDate"), "1 day"),"OwnerUserId") \
        .agg(sum("Score").alias("TotalScore"))\
        .orderBy("window.start","OwnerUserId")\
        .select("OwnerUserId",col("window.start").alias("Date"),"TotalScore")\
        .filter((col("window.start") >= START) & (col("window.start") <= END))

    result = window_score.groupBy("OwnerUserId").agg({"TotalScore": "sum"}).orderBy("OwnerUserId",ascending=True)
    result.where((col("OwnerUserId") == 1580) |
                       (col("OwnerUserId") == 4101) |
                       (col("OwnerUserId") == 18051) |
                       (col("OwnerUserId") == 18866) |
                       (col("OwnerUserId") == 2376109) ).orderBy("OwnerUserId").show() 
    
    





