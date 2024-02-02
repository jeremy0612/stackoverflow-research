from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, to_date, sum
from pyspark.sql.functions import  col , window
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
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
    question_df = question_df.withColumn("OwnerUserId", when(question_df.OwnerUserId == "NA", None).otherwise(question_df.OwnerUserId)).alias("question")
    answer_df = answer_df.withColumn("OwnerUserId", when(answer_df.OwnerUserId == "NA", None).otherwise(answer_df.OwnerUserId)).alias("answer")
    
    # # Valid userID
    # question_df = question_df.filter(question_df.OwnerUserId.isNotNull())
    # answer_df = answer_df.filter(answer_df.OwnerUserId.isNotNull())
    
    # --------- Query all active users  ---------
    # Register the DataFrame as a temporary SQL table
    answer_df.createOrReplaceTempView("answer") 
    metric1_df = spark.sql("""
                                    SELECT OwnerUserId as UserId, COUNT(*) as TotalAnswer, SUM(score) as TotalScore
                                    FROM answer 
                                    GROUP BY OwnerUserId 
                                    HAVING TotalAnswer > 50 or TotalScore > 500
                                    ORDER BY OwnerUserId ASC;
                                """)
    metric1_df = metric1_df.select("UserId")

    metric2_df = question_df.join(answer_df, (question_df.Id == answer_df.ParentId) \
                                  & (question_df.CreationDate == answer_df.CreationDate), 'inner')\
                            .select(col("answer.Id").alias("answerId"),\
                                    col("answer.OwnerUserId").alias("UserId"),\
                                    col("question.Id").alias("questionId"))\
                            .groupBy("UserId","questionId")\
                            .count()\
                            .orderBy("count", ascending=False)
    metric2_df = metric2_df.select("UserId").where(col("count") > 5)
    
    result_df = metric1_df.union(metric2_df)\
                            .where("UserId is not null")\
                            .orderBy("UserId")
    result_df.show()
    


    
    





