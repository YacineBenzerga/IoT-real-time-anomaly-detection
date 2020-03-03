from pyspark.sql.functions import col, udf, window
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads
from pyspark.sql import SparkSession, SQLContext
import pgConnector
import sys
sys.path.append("./postgres/")


class Min_streamer:

    def __init__(self):
        """
        Initialize Spark, Spark streaming context
        """
        self.sc_cfg = SparkConf()
        self.sc_cfg.setAppName("timescaleWrite")
        self.sc_cfg.set("spark.executor.memory", "1100m")
        self.sc_cfg.set("spark.executor.cores", "2")
        self.sc_cfg.set("spark.executor.instances", "9")
        self.sc = SparkContext(conf=self.sc_cfg).getOrCreate("timescaleWrite")
        self.ssc = StreamingContext(self.sc, 10)
        self.spark = SparkSession(self.sc)
        self.kafka_topic = 'temp_topic'
        self.kfk_brokers_ip = "ec2-3-210-59-51.compute-1.amazonaws.com:9092, \
				ec2-52-2-252-109.compute-1.amazonaws.com:9092,ec2-52-86-201-163.compute-1.amazonaws.com:9092"
        self.sc.setLogLevel("ERROR")

    def process_stream(self, rdd):
        """
         Args rdd: rdd
        :rtype: None
        """
        if rdd.isEmpty():
            print("RDD is empty")
        else:
            df = rdd.toDF()
            # downsample data
            df2 = df.withColumn("timestamp", df.ts.cast("timestamp"))
            downsampled_df = df2.groupBy('id', window("timestamp", "1 second").alias("ds_ts")).agg(F.round(F.avg("val"), 2).alias(
                'downsample_avg'))
            final_df = downsampled_df.select("id", downsampled_df['ds_ts'].start.alias(
                "start_ts"), "downsample_avg").orderBy('start_ts', ascending=True)

            # write to timescale
            try:
                connector = pgConnector.PostgresConnector(
                    "ec2-3-94-71-208.compute-1.amazonaws.com", "datanodedb", "datanode", "password")
                connector.write(final_df, "downsampled_table", "append")

            except Exception as e:
                print(e)
                pass

    def init_stream(self):
        self.kafkaStream = KafkaUtils.createDirectStream(
            self.ssc, [self.kafka_topic],  {"metadata.broker.list": self.kfk_brokers_ip})
        self.rdd = self.kafkaStream.repartition(9).map(
            lambda x: loads(x[1].decode('utf-8')))
        self.rdd.foreachRDD(lambda rdd: self.process_stream(rdd))

    def start_stream(self):
        self.init_stream()
        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == "__main__":
    streamer = Min_streamer()
    streamer.start_stream()
