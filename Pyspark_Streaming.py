import findspark
findspark.init("/home/ubuntu/tools/spark/spark-3.2.1-bin-hadoop3.2")


from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *

#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = '''org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1'''


spark = SparkSession.builder.appName("Spark_Kafka_StockData").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


bootstrap_server = 'localhost:9092'
kafka_topic = 'StockStream'


stock_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", kafka_topic ) \
        .option("startingOffsets", "latest") \
        .load()


stock_df.printSchema()
print(type(stock_df))

df1 = stock_df.selectExpr("CAST(value AS STRING)")

print(type(df1))


# Define a schema for the stock data


stock_schema_str = "Time String,Open Double,High Double,Low Double,Close Double,Volume Double"


stock_df2 = df1.select(from_csv(col("value"),stock_schema_str).alias("stock"))

stock_df3 = stock_df2.select("stock.*")
                       
stock_df3.printSchema()

stock_agg_write_stream = stock_df3 \
        .writeStream \
        .outputMode("append") \
        .option("truncate", "false") \
        .format("memory") \
        .queryName("testedTable") \
        .start()

print("Please wait 100 seconds while spark is processing the incoming data!!")
stock_agg_write_stream.awaitTermination(100)

stream_df = spark.sql("select * from testedTable")
stream_df.show(5,truncate = False)
stream_df.count()



feature_cols = ["Open", "High", "Low"]
vect_assembler = VectorAssembler(inputCols = feature_cols, outputCol="features")

data_w_features = vect_assembler.transform(stream_df)

final_df = data_w_features.select('features','Close','Time')
final_df.count()
final_df.show(10, truncate=False)

#save/write csv file to hdfs----
final_df.write.parquet("Stock_Test_Data")
print("Testing data is saved to Stock_Test_Data in parquet file format!!")


