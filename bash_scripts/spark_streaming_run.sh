#!/bin/bash
spark-submit --master spark://ip-10-0-0-14:7077 \
             --jars /home/ubuntu/Datanode/postgresql-42.2.2.jar \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
               streaming/stream_process.py & spark-submit --master spark://ip-10-0-0-14:7077 \
             --jars /home/ubuntu/Datanode/postgresql-42.2.2.jar \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
               streaming/min_stream.py