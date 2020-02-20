import sys
sys.path.append("./postgres/")
import pgConnector
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, udf, desc, first, last
from pyspark.sql.types import FloatType, BooleanType, IntegerType

import numpy as np
import scipy.stats as stats
from stldecompose import decompose


def calculate_zscore(ts, hybrid=False):
    if hybrid:
        median = np.ma.median(ts)
        mad = np.ma.median(np.abs(ts - median))
        return (ts - median) / mad
    else:
        return stats.zscore(ts, ddof=1)


def calculate_test_statistic(ts, test_statistics, hybrid=False):

    corrected_ts = np.ma.array(ts, mask=False)
    for anomalous_index in test_statistics:
        corrected_ts.mask[anomalous_index] = True
    z_scores = abs(calculate_zscore(corrected_ts, hybrid=hybrid))
    max_idx = np.argmax(z_scores)
    return max_idx, z_scores[max_idx]


def calculate_critical_value(size, alpha):

    t_dist = stats.t.ppf(1 - alpha / (2 * size), size - 2)

    numerator = (size - 1) * t_dist
    denominator = np.sqrt(size ** 2 - size * 2 + size * t_dist ** 2)

    return numerator / denominator


def seasonal_esd(ts, seasonality=None, hybrid=False, max_anomalies=10, alpha=0.05):

    ts = np.array(ts)
    # Seasonality is 20% of the ts if not given.
    seasonal = seasonality or int(0.2 * len(ts))
    decomposition = decompose(ts, period=seasonal)
    residual = ts - decomposition.seasonal - np.median(ts)
    outliers = esd(residual, max_anomalies=max_anomalies,
                   alpha=alpha, hybrid=hybrid)
    return outliers


def esd(ts, max_anomalies=10, alpha=0.05, hybrid=False):

    ts = np.copy(np.array(ts))
    test_statistics = []
    total_anomalies = 0
    for curr in range(max_anomalies):
        test_idx, test_val = calculate_test_statistic(
            ts, test_statistics, hybrid=hybrid)
        critical_value = calculate_critical_value(
            len(ts) - len(test_statistics), alpha)
        if test_val > critical_value:
            total_anomalies = curr
        test_statistics.append(test_idx)
    anomalous_indices = test_statistics[:total_anomalies +
                                        1] if total_anomalies else []
    return anomalous_indices


class Batch_process:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Batch Process anomaly detect") \
            .config('spark.executor.memory', '6gb') \
            .getOrCreate()

    def read_from_timescale(self):
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://ec2-3-94-71-208.compute-1.amazonaws.com:5432/datanodedb") \
            .option("dbtable", "downsampled_table") \
            .option("user", "datanode") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        self.process_df(df)

    def process_df(self, df):
        def detect_anomaly(ts):
            outliers_indices = seasonal_esd(
                ts, hybrid=True, max_anomalies=10)
            return len(outliers_indices)

        grouped_df = df.groupBy(["id"]).agg(F.collect_list("downsample_avg").alias(
            "downsampled_ts"), first("start_ts").alias("start_ts"), last("end_ts").alias("end_ts"))
        anomaly_udf = udf(detect_anomaly, IntegerType())
        processed_df = grouped_df.withColumn("num_anomaly", anomaly_udf(
            "downsampled_avg")).sort(desc("num_anomaly"))
        final_df = processed_df.select(
            "id", "start_ts", "end_ts", "num_anomaly")
        try:
            connector = pgConnector.PostgresConnector(
                "ec2-3-94-71-208.compute-1.amazonaws.com", "datanodedb", "datanode", "password")
            connector.write(final_df, "global_anomalies_table", "append")
        except Exception as e:
            print(e)
            pass


if __name__ == "__main__":
    batch = Batch_process()
    batch.read_from_timescale()
