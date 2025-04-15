#!/bin/bash

echo "Start indexingg!!"
INPUT_PATH=${1:-hdfs:///index/data}

if [[ $INPUT_PATH == hdfs://* ]]; then
    echo "BIG_DATA_APP: Using HDFS path: $INPUT_PATH"
else
    echo "BIG_DATA_APP: Copying local file to HDFS"
    hdfs dfs -put -f $INPUT_PATH /tmp/local_index_data
    INPUT_PATH="hdfs:///tmp/local_index_data"
fi

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input $INPUT_PATH \
    -output /tmp/index/pipeline1 \
    -mapper /app/mapreduce/mapper1.py \
    -reducer /app/mapreduce/reducer1.py \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input /tmp/index/pipeline1 \
    -output /tmp/index/pipeline2 \
    -mapper /app/mapreduce/mapper2.py \
    -reducer /app/mapreduce/reducer2.py \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py

echo "BIG_DATA_APP: load to cassandra..."
source .venv/bin/activate
python3 upload_to_cassandra.py

echo "BIG_DATA_APP: Indexing completed successfully!"