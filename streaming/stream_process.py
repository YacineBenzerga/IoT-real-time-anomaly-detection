from pgConnector import PostgresConnector
from pyspark.sql import SparkSession, SQLContext, Row, DataFrame, SQLContext, functions as F
from json import loads
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, udf, desc, first, last
from pyspark.sql.types import FloatType, BooleanType, IntegerType
from pyspark.sql.window import Window
import time


class Streamer:

    def __init__(self):
        """
        Initialize Spark, Spark streaming context
        """
        self.sc_cfg = SparkConf()
        self.sc_cfg.setAppName("IoTAnomalyDetect")
        self.sc_cfg.set("spark.executor.memory", "1000m")
        self.sc_cfg.set("spark.executor.cores", "2")
        self.sc_cfg.set("spark.executor.instances", "15")
        self.sc_cfg.set("spark.driver.memory", "5000m")
        self.sc_cfg.set("spark.locality.wait", 100)
        self.sc_cfg.set("spark.executor.extraJavaOptions",
                        "-XX:+UseConcMarkSweepGC")

        #self.sc_cfg.set("spark.streaming.backpressure.enabled", 'True')
        self.sc = SparkContext(conf=self.sc_cfg).getOrCreate("anomaly_detect")
        self.ssc = StreamingContext(self.sc, 2.5)
        self.spark = SparkSession(self.sc)
        self.kafka_topic = 'confluent_topic'
        self.kfk_brokers_ip = "ec2-52-45-231-52.compute-1.amazonaws.com:9092, \
				ec2-3-222-199-83.compute-1.amazonaws.com:9092,ec2-3-212-207-21.compute-1.amazonaws.com:9092"
        self.sc.setLogLevel("ERROR")

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
