from pyspark.sql import SparkSession
import os

def extract_fields(file_tuple):
    file_path, content = file_tuple
    file_name = os.path.basename(file_path)
    base_name = file_name[:-4] 
    doc_id, doc_title = base_name.split("_", 1)
    return f"{doc_id}\t{doc_title}\t{content}"

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName("IndexDataPreparation").getOrCreate()
        sc = spark.sparkContext
        data_rdd = sc.wholeTextFiles("hdfs:///data/*.txt")


        transformed_rdd = data_rdd.map(extract_fields)

        transformed_rdd = transformed_rdd.coalesce(1)
        transformed_rdd.saveAsTextFile("hdfs:///index/data")
        spark.stop()
    except Exception as e:
        print(f"BIG_DATA_APP: Error: {e}")
