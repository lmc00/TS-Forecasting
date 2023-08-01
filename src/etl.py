## Imports
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import copy
import time
import statsmodels
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Spark dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel, SparkConf
from pyspark.ml.feature import StandardScaler, VectorAssembler, PCA
from pyspark.mllib.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.ml.classification import LogisticRegression

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, expr, udf, sequence

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Generate_100_columns").getOrCreate()
    sc = spark.sparkContext
    # Other configs

    # Other configs
    pd.options.display.float_format = "{:.2f}".format

    # Useful directory variables
    src_path = os.getcwd()
    root_path = os.path.dirname(src_path)
    data_path = root_path + "/datasets"
    visualization_path = root_path + "/data_visualization"

    # Start counting time
    start_t = time.time()
    # Reading the original file
    df = spark.read.parquet(
        "output_final.parquet"
    )  # Functional programming. Reading the raw data file with the Structured API
    # df.printSchema()
    df.createOrReplaceTempView("df")

    # Generating a dataFrame with the times (every 30 minutes from start_timestamp to end_timestamp)
    dates = pd.date_range(
        start=datetime(2018, 1, 1, 0, 0, 0),
        end=datetime(2021, 6, 30, 0, 0, 0),
        freq="30min",
    )
    datetimes = [date.to_pydatetime() for date in dates]
    time_df = (
        spark.createDataFrame(datetimes, TimestampType())
        .withColumnRenamed("value", "time")
        .sort(F.asc("time"))
    )

    # Obtaning each consumption node in a list
    node_list = (
        spark.sql("SELECT node from df").rdd.flatMap(lambda x: x).collect()
    )  # Getting the list with all the node names


    # Obtaning each consumption node:
    # We are having two time related Spark Dataframes that originally prior to iterate will be identical
    # time_df: will remain unchanged during the whole execution, just a reference to ensure all the times are met and if not a null is given for the corresponding electrical consumption column
    # consumption_df: this will suffer a left join at the end of each iteration and will be the container for all the consumption columns
    consumption_df = spark.createDataFrame(time_df.rdd, time_df.schema)
    consumption_df = consumption_df.withColumn("total_average_power_consumption_W", lit(0))
    for node in node_list[:100]:  # All the consumption related cluster nodes
        sql_query_node_consumption = """
                        SELECT 
                            EXPLODE(power) as (time, node_{}_power_consumption) 
                        FROM df
                        WHERE 
                            node LIKE "{}"
                    """.format(
            node, node
        )
        node_consumption = spark.sql(sql_query_node_consumption)
        node_consumption = node_consumption.withColumn(
            "time", F.to_timestamp(node_consumption.time, "yyyy-MM-dd HH:MM:SS")
        )
        node_consumption = node_consumption.groupBy(
            "time", F.window("time", "30 minutes")
        ).agg(
            avg("node_{}_power_consumption".format(node)).alias(
                "node_{}_power_consumption".format(node)
            ),
        )
        node_consumption = node_consumption.select(
            "time", "window.*", "node_{}_power_consumption".format(node)
        ).sort(F.asc("time"))
        node_consumption = node_consumption.select(
            col("end").alias("time"), col("node_{}_power_consumption".format(node))
        )
        node_consumption = node_consumption.groupBy("time").agg(
            avg("node_{}_power_consumption".format(node)).alias(
                "node_{}_average_power_consumption".format(node)
            )
        )
        node_consumption = node_consumption.select(
            "time", "node_{}_average_power_consumption".format(node)
        ).sort(F.asc("time"))
        consumption_df = consumption_df.join(node_consumption, ["time"], how="left").sort(
            F.asc("time")
        )
        consumption_df = consumption_df.fillna(0, subset=["node_{}_average_power_consumption".format(node)])
        consumption_df = consumption_df.withColumn("total_average_power_consumption_W", col("total_average_power_consumption_W")+col("node_{}_average_power_consumption".format(node)))
        consumption_df = consumption_df.drop("node_{}_average_power_consumption".format(node))
    # consumption_df.cache()
    consumption_df.write.repartittion(1).parquet("javier_example")
    end_t = time.time()
    print("Time in seconds " + str(end_t - start_t))
    spark.stop()
