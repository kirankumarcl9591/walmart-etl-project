from pyspark.sql import SparkSession
import json
import os


def read_json(config_file_path):
    with open(config_file_path, "r") as f:
        config = json.load(f)
    return config


def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    return spark


def read_csv(spark, file_path):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    return df


def write_delta_csv(df, output_path, name):
    df.write.format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(f"{output_path}/{name}")


def main():

    config_path = "A_RAW_DATA/c_pyspark_script/raw_config.json"
    config = read_json(config_path)

    spark = get_spark_session(config["spark_app_name"])

    input_path = config["input_path"]
    output_path = config["output_path"]

    for file in config["files"]:
        file_path = os.path.join(input_path, file)

        print(f"Processing {file}")

        df = read_csv(spark, file_path)

        name = file.replace(".csv", "")
        write_delta_csv(df, output_path, name)

    spark.stop()


if __name__ == "__main__":
    main()
