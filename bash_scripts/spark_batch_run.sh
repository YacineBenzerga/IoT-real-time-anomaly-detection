#!/bin/bash
spark-submit --master spark://ip-10-0-0-14:7077 \
             --jars /home/ubuntu/Datanode/postgresql-42.2.2.jar \
             --py-files /home/ubuntu/Datanode/mypkg.zip \
               batch_processing/batch_process.py