#!/bin/bash

echo "BIG_DATA_APP: start indexingg!!..."

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input /data \
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

python3 app.py

echo "BIG_DATA_APP: Indexing completed successfully!"
