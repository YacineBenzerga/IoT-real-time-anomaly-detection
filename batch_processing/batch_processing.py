import sys
#import postgres
import pyspark
from helper_fns import helpers


class BatchProcess:

    def __init__(self, s3_config, schema_config, postgres_config):

        # self.postgres_config =
        self.s3_bucket = 'insight.de.yacine.data'
        self.s3_folder = 'sep2019'
        self.s3_raw_data = 'data.csv'
        self.data_schema = {
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

        self.node_schema = {
            "DELIMITER":  ",",
            "FIELDS":
            {
                "address": {"index": 3, "type": "str"},
                "lat":   {"index": 4, "type": "float"},
                "lon": {"index": 5, "type": "float"},
                "start_timestamp":  {"index": 7, "type": "str"}
            }
        }

        self.sc = pyspark.SparkContext.getOrCreate()
        self.sc.setLogLevel("ERROR")

    def read_from_s3(self):
        filenames = "s3a://{}/{}/{}".format(self.s3_bucket,
                                            self.s3_folder,
                                            self.s3_raw_data)

        self.data = self.sc.textFile(filenames)

    def spark_process(self):
        self.sc.broadcast(self.data_schema)
        # ToDo: Add cleaning, processing here
        # Add lat, lon, address, uptime of nodes from nodes.csv
        self.data = (self.data.map(
            lambda line: helpers.map_schema(line, self.data_schema)))

    # def write_to_db(self):

    def start(self):
        self.read_from_s3()
        self.spark_process()
        # self.load_to_db()
