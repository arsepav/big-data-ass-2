#!/usr/bin/env python3
import sys
import math
from pyspark.sql import SparkSession

def calculate_bm25(term_data, total_docs, avg_doc_len, k1=1.0, b=0.75):
    term, doc_id, tf, df, doc_len = term_data
    idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)
    numerator = tf * (k1 + 1)
    denominator = tf + k1 * (1 - b + b * (doc_len / avg_doc_len))
    bm25 = idf * (numerator / denominator)
    return doc_id, bm25

def main():
    query = sys.argv[1].strip() if len(sys.argv) >= 2 else input("Enter your search query: ").strip()
    query_terms = query.lower().split()
    
    spark = SparkSession.builder \
        .appName("SearchEngineQuery") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    total_docs = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="documents") \
        .load() \
        .count()

    doc_lengths_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="documents") \
        .load() \
        .rdd.map(lambda row: (row['doc_id'], len(row['title'].split()) + (len(row['text'].split()) if 'text' in row and row['text'] else 0)))
    
    avg_doc_len = doc_lengths_rdd.map(lambda x: x[1]).mean()

    vocabulary_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="vocabulary") \
        .load() \
        .rdd.filter(lambda row: row['term'] in query_terms) \
        .map(lambda row: (row['term'], row['df']))

    inverted_index_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="inverted_index") \
        .load() \
        .rdd.filter(lambda row: row['term'] in query_terms) \
        .map(lambda row: (row['term'], row['doc_id'], row['tf']))


    joined_rdd = inverted_index_rdd \
        .keyBy(lambda x: x[0]) \
        .join(vocabulary_rdd.keyBy(lambda x: x[0])) \
        .map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][1])) \
        .keyBy(lambda x: x[1]) \
        .join(doc_lengths_rdd.keyBy(lambda x: x[0])) \
        .map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1][1]))

    bm25_rdd = joined_rdd.map(lambda x: calculate_bm25((x[0], x[1], x[2], x[3], x[4]), total_docs, avg_doc_len))

    scores_rdd = bm25_rdd.reduceByKey(lambda x, y: x + y)

    top_docs = scores_rdd.takeOrdered(10, key=lambda x: -x[1])

    documents_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="documents") \
        .load() \
        .rdd.map(lambda row: (row['doc_id'], row['title']))

    results = spark.sparkContext.parallelize(top_docs) \
        .map(lambda x: (x[0], x[1])) \
        .join(documents_rdd) \
        .map(lambda x: (x[0], x[1][1], x[1][0]))
    scores_rdd = bm25_rdd.reduceByKey(lambda x, y: x + y)
    top_docs = scores_rdd.takeOrdered(10, key=lambda x: -x[1])


    documents_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="search_engine", table="documents") \
        .load() \
        .rdd.map(lambda row: (row['doc_id'], row['title']))

  
    results = spark.sparkContext.parallelize(top_docs) \
        .map(lambda x: (x[0], x[1])) \
        .join(documents_rdd) \
        .map(lambda x: (x[0], x[1][1], x[1][0])).sortBy(lambda x: -x[2])

    print("BIG_DATA_APP: Search result:")
    for doc_id, title, score in results.collect():
        print(f"BIG_DATA_APP: doc_id: {doc_id}, title: {title}, score: {score}")

    spark.stop()

if __name__ == "__main__":
    main()