# Write your code here

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from pyspark.sql.functions import from_json, col

BOOTSTRAP_SERVERS = "confluent-local-broker-1:64574"
TOPIC = "wikimedia"

schema = StructType([
    StructField("timestamp", IntegerType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("user", StringType()),
    StructField("length", StructType([
        StructField("old", IntegerType()),
        StructField("new", IntegerType()),  
    ])),
    StructField("meta", StructType([
        StructField("domain", StringType())  
    ])),
])  

def main():
    #Create spark session
    spark = SparkSession.builder \
                        .appName('WikimediaEvents') \
                        .getOrCreate()
    
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .load()

    #Parsed the incoming data and calculated new columns
    kafka_stream_df = kafka_stream_df.select(F.from_json(F.col('value').cast('string'),schema).alias('data'))\
    .select('data.timestamp', 'data.bot', "data.minor", "data.user", F.col("data.length.old").alias('old_length'), F.col("data.length.new").alias('new_length'), F.col("data.meta.domain")).alias('domain').withColumn('length_diff' , F.col('new_length') - F.col("old_length")).withColumn('length_diff_percent', ((F.col('new_length') - F.col("old_length")) / F.col("old_length")) *100)                                                                

    # Writing data to csv
    
    # kafka_stream_df.writeStream \
    # .outputMode("append")\
    # .option("checkpointLocation","output")\
    # .format("csv")\
    # .option("path","./output")\
    # .option("header",True)\
    # .trigger(processingTime="10 seconds")\
    # .start()\
    # .awaitTermination() 

    #Output the top five domains, along with counts for each.
    df_top_domain = kafka_stream_df.groupBy(F.col('domain')).count().orderBy(F.desc('count')).limit(5)
    
    # query = df_top_domain\
    # .writeStream \
    # .outputMode("complete")\
    # .format("console") \
    # .start() \
    # .awaitTermination()

    #Output the top five users, based on the length of content they have added (sum of length_diff).
    df_top_users = kafka_stream_df.groupBy('user').agg(F.sum('length_diff')).alias('sum(length_diff)').orderBy(F.desc('sum(length_diff)')).limit(5)

    # query = df_top_users\
    # .writeStream \
    # .outputMode("complete")\
    # .format("console") \
    # .start() \
    # .awaitTermination()

    #Output the total number of events, the percent of events by bots, the average length_diff, the minimum length_diff, and the maximum length_diff. (In this example, percent_bot is represented as 0-1, rather than 0-100.
    df_total_events = kafka_stream_df.agg(F.count('timestamp').alias('total_count'),\
    (F.count('bot') / F.count('timestamp')).alias('percent_bot'),\ #this calculation is wrong
    F.avg('length_diff').alias('average_length_diff'),\
    F.min('length_diff').alias('min_length_diff'),\
    F.max('length_diff').alias('max_length_diff')
    )
    # query = df_total_events\
    # .writeStream \
    # .outputMode("complete")\
    # .format("console") \
    # .start() \
    # .awaitTermination()


if __name__ == "__main__":
    main()