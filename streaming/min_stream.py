from pgConnector import PostgresConnector
from pyspark.sql import SparkSession, SQLContext
from json import loads
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
import time
from pyspark.sql.functions import col, udf, window


class Streamer:

    def __init__(self):
        """
        Initialize Spark, Spark streaming context
        """
        self.sc_cfg = SparkConf()
        self.sc_cfg.setAppName("timescaleWrite")
        self.sc_cfg.set("spark.executor.memory", "1000m")
        self.sc_cfg.set("spark.executor.cores", "2")
        self.sc_cfg.set("spark.executor.instances", "15")
        self.sc_cfg.set("spark.driver.memory", "5000m")
        self.sc_cfg.set("spark.locality.wait", 100)
        self.sc_cfg.set("spark.executor.extraJavaOptions",
                        "-XX:+UseConcMarkSweepGC")

        #self.sc_cfg.set("spark.streaming.backpressure.enabled", 'True')
        self.sc = SparkContext(conf=self.sc_cfg).getOrCreate("timescaleWrite")
        self.ssc = StreamingContext(self.sc, 10)
        self.spark = SparkSession(self.sc)
        self.kafka_topic = 'all_topic'
        self.kfk_brokers_ip = "ec2-3-210-59-51.compute-1.amazonaws.com:9092, \
				ec2-52-2-252-109.compute-1.amazonaws.com:9092,ec2-52-86-201-163.compute-1.amazonaws.com:9092"
        self.sc.setLogLevel("ERROR")

    def process_stream(self, rdd):
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
                connector = PostgresConnector(
                    "ec2-3-94-71-208.compute-1.amazonaws.com", "datanodedb", "datanode", "password")
                connector.write(final_df, "downsampled_table", "append")

            except Exception as e:
                print(e)
                pass

    def quiet_logs(self, sc):
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    def init_stream(self):
        self.kafkaStream = KafkaUtils.createDirectStream(
            self.ssc, [self.kafka_topic],  {"metadata.broker.list": self.kfk_brokers_ip})
        self.rdd = self.kafkaStream.map(lambda x: loads(x[1].decode('utf-8')))
        self.rdd.foreachRDD(lambda rdd: self.process_stream(rdd))

    def start_stream(self):
        self.init_stream()
        self.quiet_logs(self.sc)
        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == "__main__":
    streamer = Streamer()
    streamer.start_stream()
