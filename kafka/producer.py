import sys
import json
import boto3
from helper_fns import helpers
from kafka.producer import KafkaProducer
import lazyreader
import time


class Producer(object):
    """
    Class: Kafka producer ingesting datafrom S3 bucket
    """

    def __init__(self, addr):
        """
        Initialize Kafka producer with s3,kafka config files
        """
        # ToDO: Add parse_config helper method

        self.producer = KafkaProducer(bootstrap_servers=addr)
        self.s3_bucket = 'insight.de.yacine.data'
        self.s3_folder = 'sep2019'
        self.raw_data = 'data.csv'
        self.kafka_topic = 'my_topic'
        self.schema = {
            "DELIMITER":  ",",
            "FIELDS":
            {
                "timestamp": {"index": 0, "type": "str"},
                "node_id":   {"index": 1, "type": "str"},
                "sensor": {"index": 3, "type": "str"},
                "parameter":  {"index": 4, "type": "str"},
                "value_hrf":   {"index": 6, "type": "float"}
            }
        }

    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.s3_bucket,
                                Key="{}/{}".format(self.s3_folder,
                                                   self.raw_data))

            for line in lazyreader.lazyread(obj['Body'], delimiter='\n'):

                message_info = line.strip()
                msg = "{}".format(helpers.map_schema(
                    message_info, self.schema))
                print(msg)
                if msg is not None:
                    # ToDo: send kafka msg in encoded(key),value pair to reduce bandwith
                    self.producer.send(self.kafka_topic, msg)
                    msg_cnt += 1

                time.sleep(0.001)


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
