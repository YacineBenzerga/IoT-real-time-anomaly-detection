import time
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, BooleanType, IntegerType
from pyspark.sql.functions import col, udf, desc, first, last
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads
from pyspark.sql import SparkSession, SQLContext, Row, DataFrame, SQLContext, functions as F
import pgConnector
import sys
sys.path.append("./postgres/")


class Streamer:

    def __init__(self):
        """
        Initialize Spark, Spark streaming context
        """
        self.sc_cfg = SparkConf()
        self.sc_cfg.setAppName("IoTAnomalyDetect")
        self.sc_cfg.set("spark.executor.memory", "2700m")
        self.sc_cfg.set("spark.executor.cores", "2")
        self.sc_cfg.set("spark.executor.instances", "9")
        self.sc_cfg.set("spark.driver.memory", "5000m")
        self.sc = SparkContext(conf=self.sc_cfg).getOrCreate("anomaly_detect")
        self.ssc = StreamingContext(self.sc, 5)
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
        def detect_anomaly(sensor_readings, running_avg, std_dev):
            """
            Args:
                sensor_readings: List(float)
                running_avg: float
                std_dev: float
            :rtype: int
            """
            anomalies = []
            for x, (i, y) in zip(sensor_readings, enumerate(running_avg)):
                upper_limit = running_avg[i-1] + 3*std_dev
                lower_limit = running_avg[i-1] - 3*std_dev
                if (x > upper_limit) or (x < lower_limit):
                    anomalies.append(x)
            return len(anomalies)

        if rdd.isEmpty():

            print("RDD is empty")
        else:
            df = rdd.toDF().cache()
            w = (Window().partitionBy(col("id")).rowsBetween(-1, 1))
            df = df.withColumn('rolling_average', F.avg("val").over(w))
            agg_df = df.groupBy(['id']).agg(F.collect_list("val").alias("sensor_reading"), first("ts").cast('timestamp').alias("start_ts"), last(
                "ts").cast('timestamp').alias("end_ts"), F.round(F.stddev("val"), 3).alias("std_temp"), F.collect_list("rolling_average").alias("rol_avg"))
            agg_df.show()
            anomaly_udf = udf(detect_anomaly, IntegerType())
            processed_df = agg_df.withColumn("num_anomaly", anomaly_udf(
                "sensor_reading", "rol_avg", "std_temp")).sort(desc("num_anomaly"))
            final_df = processed_df.withColumn("anomaly", F.when(
                F.col("num_anomaly") > 1, True).otherwise(False))
            final_df = final_df.select(
                "id", "start_ts", "end_ts", "std_temp", "num_anomaly", "anomaly")
            try:
                connector = pgConnector.PostgresConnector(
                    "ec2-3-94-71-208.compute-1.amazonaws.com", "datanodedb", "datanode", "password")
                connector.write(final_df, "anomaly_window_tbl", "append")

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
    streamer = Streamer()
    streamer.start_stream()
