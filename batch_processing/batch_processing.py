from pyspark.sql import SparkSession


class Batch_process:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Batch Process anomaly detect") \
            .config('spark.executor.memory', '6gb') \
            .getOrCreate()

        # .config("spark.jars", "/home/ubuntu/newnode/postgresql-42.2.2.jar") \

    def read_from_timescale(self):
        # 1.Read data from timescale
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://ec2-3-94-71-208.compute-1.amazonaws.com:5432/datanodedb") \
            .option("dbtable", "downsampled_table") \
            .option("user", "datanode") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.show()

    # def process_df(self):

        # 2.group by sensor id
        # 3.
        # 3.1. UDF to Decompose to seasonality, trend, residual
        # 3.2. If residual distribution is normal: detect anomaly (s-esd)
        #      If residual distribution is not normal --> turn to normal: detect anomaly (s-esd)
        # 4.Write table: id,start_ts,end_ts,num_anomalies to seasonal_anomlies_table


if __name__ == "__main__":
    batch = Batch_process()
    batch.read_from_timescale()
