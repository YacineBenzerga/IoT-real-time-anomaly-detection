# Real-time anomaly detection in Industrial IoT sensors
### Insight Data Engineering (New York, Fall 2020)


The goal of this project is to leverage IoT sensors to allow industrial companies the identification of malfunctioning assets by detecting usage anomalies in real time and enable predictive maintenance by detecting global usage anomalies in 24 hours time windows.


## Approach
To simulate an Industrial IoT environement. A kafka producer is streaming temperature recordings of 90 sensors at a rate of 27000 events/sec. the generated data follows a normal distribution for the assets functioning properly, and an exponential distribution as noise for the malfunctioing assets.   



***Streaming***: real-time sensor observations are streamed by Kafka into Spark Streaming, which handles two tasks: 
 1.detecting anomalies using an offline High-low pass filter algorithm and write the results to  anomaly_window_tbl in Timescale.
 
2.Preprocess & downsample generated data and writes to downsampled_table in Timescale.


***Batch***: downsampled data is then ingested every 24 hours using Airflow from Timescale to Spark, where global anomlies are detected using Twitter hybrid Seasonal ESD, and results are saved back to Timescale in global_anomalies_table

## Instructions 

Pegasus is a tool that allows you to quickly deploy a number of distributed technologies.

Install and configure [AWS CLI](https://aws.amazon.com/cli/) and [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine, and clone this repository using
`git clone https://github.com/YacineBenzerga/IoT-real-time-anomaly-detection`.

#### SET UP CLUSTER:
- (4 nodes) Spark-Cluster 
- (3 nodes) Kafka-Cluster
- (1 node) PostgreSQL(Timescale)
- (1 node) Dash 

-Follow the instructions in docs/pegasus.txt to create required clusters, install and start technologies

>Required technologies per cluster
-(Spark): aws, hadoop, spark
-(Kafka): aws, hadoop, zookeeper, kafka

-Install Postgres with Timescale extension using(https://docs.timescale.com/latest/getting-started/installation/ubuntu/installation-apt-ubuntu)

-Airflow scheduler can be installed on the master node of Spark-Cluster.Follow the instructions in `docs/airflow_install.txt` to install and launch the Airflow server.

