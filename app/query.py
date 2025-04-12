#!/usr/bin/env python3
import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log as spark_log

def main():
    query = sys.argv[1].strip() if len(sys.argv) >= 2 else input("Enter your search query: ").strip()
    query_terms = query.lower().split()
    
    spark = SparkSession.builder \
        .appName("SearchEngineQuery") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()

    total_docs = 4000

    vocabulary = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="vocabulary") \
        .load() \
        .filter(col("term").isin(query_terms))
    
    inverted_index = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="inverted_index") \
        .load() \
        .filter(col("term").isin(query_terms))
    
    idx = inverted_index.join(vocabulary.select("term", "df"), on="term")
    k1 = 1.0
    b = 0.75
    bm25_expr = spark_log(total_docs / col("df")) * (((k1 + 1) * col("tf")) / (k1 + col("tf")))
    idx = idx.withColumn("bm25", bm25_expr)
    scores = idx.groupBy("doc_id").sum("bm25").withColumnRenamed("sum(bm25)", "score")
    
    documents = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="documents") \
        .load() \
        .select("doc_id", "title")

    results = scores.join(documents, on="doc_id")
    
    top_docs = scores.orderBy(col("score").desc()).limit(10)
    
    for row in top_docs.select("doc_id").collect():
        print("doc: ", row["doc_id"])
        

    top_docs = results.orderBy(col("score").desc()).limit(10)

    print("Search result:")

    for row in top_docs.select("doc_id", "title").collect():
        print(f"doc: {row['doc_id']}, title: {row['title']}")
    
    spark.stop()

if __name__ == "__main__":
    main()
