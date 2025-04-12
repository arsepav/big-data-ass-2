#!/usr/bin/env python3
import subprocess
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': '1'
    }
""")
session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        df int
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        tf int,
        PRIMARY KEY (term, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        title text,
        doc_id text,
        PRIMARY KEY (title, doc_id)
    )
""")

hdfs_path = "/tmp/index/pipeline1"
command = ["hdfs", "dfs", "-cat", f"{hdfs_path}/part-*"]

proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

for line in proc.stdout:
    line = line.strip()
    print(line)
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    term, doc_id, tf_str = parts[0], parts[1], parts[2]
    try:
        tf = int(tf_str)
    except ValueError:
        continue

    session.execute("""
        INSERT INTO inverted_index (term, doc_id, tf)
        VALUES (%s, %s, %s)
    """, (term, doc_id, tf))

proc.wait()

hdfs_path_pipline2 = "/tmp/index/pipeline2"
command = ["hdfs", "dfs", "-cat", f"{hdfs_path_pipline2}/part-*"]

proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

for line in proc.stdout:
    line = line.strip()
    print(line)
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 2:
        continue
    term, df = parts[0], int(parts[1])

    session.execute("""
        INSERT INTO vocabulary (term, df)
        VALUES (%s, %s)
    """, (term, df))

proc.wait()


hdfs_path_pipline3 = "/data"
command = ["hdfs", "dfs", "-ls", f"{hdfs_path_pipline3}/"]

proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

for line in proc.stdout:
    if not line:
        continue
    line = line[line.find("/data/") + 6:].replace(".txt\n", "")
    parts = line.split("_", maxsplit=1)
    print(parts)
    if len(parts) < 2:
        continue
    doc_id, title = parts[0], parts[1]

    print(doc_id, title)

    session.execute("""
        INSERT INTO documents (doc_id, title)
        VALUES (%s, %s)
    """, (doc_id, title))

proc.wait()

print("BIG_DATA_APP: loaded to cassandra!!!!!!!")
