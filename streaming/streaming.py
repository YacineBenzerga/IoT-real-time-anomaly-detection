import pyspark
import json
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition


class Streamer:

    def __init__(self):
        """
        Initialize Spark, Spark streaming context
        """

        self.sc = pyspark.SparkContext().getOrCreate()
        # ToDo: add interval from config
        self.ssc = pyspark.streaming.StreamingContext(self.sc, 1)
        self.kafka_topic = "my_topic"
        self.sc.setLogLevel("ERROR")
        self.kfk_brokers_ip = "$SPARK-CLUSTER_0:9092,$SPARK-CLUSTER_1:9092,$SPARK-CLUSTER_2:9092",

    def init_stream(self):
        """
        Initialize stream from a Kafka topic
        """
        self.dataStream = KafkaUtils.createDirectStream(
            self.ssc, [self.kafka_topic],  {"metadata.broker.list": self.kfk_brokers_ip})

    def process_stream(self):
        """
        Clean,Filter,Enrich streamed data
        """
        self.init_stream()

        # Test stream
        test_output = self.dataStream.map(lambda x: x[1])
        test_output.pprint()
        # Do processing here in functional programming way:
        # self.dataStream = (self.dataStream.repartition.map.filter.map...etc)

    def start_stream(self):
        self.process_stream()
        self.ssc.start()
        self.ssc.awaitTermination()
