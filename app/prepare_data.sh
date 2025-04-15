#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    echo "BIG_DATA_APP: Putting data to hdfs" && \
    hdfs dfs -put data / && \
    spark-submit prepare_index_rdd.py && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "BIG_DATA_APP: done data preparation!"
