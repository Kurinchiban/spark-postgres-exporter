import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def analyze_file_type(file_path):
    _, file_extension = os.path.splitext(file_path)
    return file_extension

def write_to_postgres(data_frame, table_name):
    postgres_url = "jdbc:postgresql://localhost:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    data_frame.write.jdbc(url=postgres_url, table=table_name, mode="overwrite", properties=properties)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Loading data into PostgreSQL')
    parser.add_argument('-p', '--file_path', required=True, help='The file path for loading into PostgreSQL')
    parser.add_argument('-d', '--table_name', required=False, help='table name')

    args = parser.parse_args()

    # Logic to analyse the file type
    file_extension = analyze_file_type(args.file_path)

    # Create a spark session 
    try:
        spark = SparkSession.builder.appName("FileProcessing")\
            .config("spark.jars", "/home/kurinchiban/Desktop/Pyspark/Pyspark/write_spark_to_postgres/postgresql-42.6.0.jar")\
            .getOrCreate()
        
        if file_extension == ".csv":
            data_df = spark.read.csv(args.file_path, header=True, inferSchema=True) 
        elif file_extension == ".json":
            data_df = spark.read.json(args.file_path)
        
        # Transform the data 
        
        if isinstance(data_df, DataFrame):
            # load the data in the postgres using spark
            write_to_postgres(data_df, args.table_name)

        spark.stop()

    except Exception as error:
        print(error)
