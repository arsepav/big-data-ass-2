#!/bin/bash


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --master yarn --archives /app/.venv.tar.gz#.venv query.py  $1  2>&1 | grep "BIG_DATA_APP:"
