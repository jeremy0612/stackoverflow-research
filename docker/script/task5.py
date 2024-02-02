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
    '''
    spark.sql("Create database if not exists qa_db")
    spark.sql("use qa_db")
    # Bucketing for question_df
    question_df.write.bucketBy(2, "Id").saveAsTable("question_bucketed")
    # Bucketing for answer_df
    answer_df.write.bucketBy(2, "Id").saveAsTable("answer_bucketed")
    '''
    # spark.sql("use qa_db")
    # Read the bucketed tables
    # question_df = spark.read.table("qa_db.question_bucketed")
    # answer_df = spark.read.table("qa_db.answer_bucketed")

    # spark.config.set("spark.sql.autoBroadcastJoinThreshold", -1)


    # Join dataframe0
    combine_df = question_df.join(answer_df, question_df.Id == answer_df.ParentId, 'inner')\
                            .select("question.Id","answer.Id")
    # Group by Question ID
    grouped_df = combine_df.groupBy("question.Id").count()
    result_df = grouped_df\
                .select("ID",col("count").alias("TotalAnswer"))\
                .where(col("count") > 5).orderBy("ID")
    # result_df.show()
    print("Number of good questions: ", result_df.count())
    
    





