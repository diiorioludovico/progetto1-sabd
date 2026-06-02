from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

from time import time

import logging
import sys


class Lambda:
    @staticmethod
    def set_tupla(x):
        # x è una lista di campi, ci interessano solo alcune colonne
        return (
            str(x[1]) + "," +
            str(x[3]) + "," +
            str(x[13]) + "," +
            str(x[16]) + "," +
            str(x[17]) + "," +
            str(x[19]) + "," +
            str(x[22]) + "," +
            str(x[23]) + "," +
            str(x[24]) + "," +
            str(x[25]) + "," +
            str(x[26])
        )


class Optimizer:

    column_list = [
        "MONTH", "OP_UNIQUE_CARRIER", "DEP_DELAY",
        "ARR_DELAY", "CANCELLED", "DIVERTED",
        "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
        "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
    ]

    def __init__(self, spark, hdfs_path, rdd_dataframe: str, num_partitions: int):
        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        # ---- calcolo num_partitions ----
        if num_partitions > 0:
            self.num_partitions = num_partitions
        else:
            cores = 8  
            self.num_partitions = cores * 2 

        logging.info(f"Numero di partizioni scelto: {self.num_partitions}")

        # ---- pipeline pre-processing ----
        raw_dataset = self.carica_dataset(spark, hdfs_path, rdd_dataframe)
        dataset = self.column_filter(raw_dataset, rdd_dataframe)
        self.save_filtered_dataset(dataset, rdd_dataframe, hdfs_path)

    def carica_dataset(self, spark, hdfs_path, rdd_dataframe: str):
        logging.info("Caricamento del dataset")
        start = time()

        if rdd_dataframe == "Dataframe":
            # DataFrame: Spark parallelizza già la lettura
            dataset = spark.read.csv(hdfs_path, header="true")
            logging.info(f"Numero partizioni DataFrame (prima): {dataset.rdd.getNumPartitions()}")
        else:
            # RDD: leggo tutti i CSV, tolgo header e splitto
            dataset_temp = spark.sparkContext.textFile(hdfs_path + "*.csv")
            header = dataset_temp.first()
            dataset = dataset_temp.filter(lambda line: line != header) \
                                  .map(lambda line: line.split(","))
            logging.info(f"Numero partizioni RDD (prima): {dataset.getNumPartitions()}")

        end = time()
        logging.info(f"Caricamento completato in {end - start:.3f} s")
        return dataset

    def column_filter(self, data, rdd_dataframe: str):
        logging.info("Filtraggio del dataset e allineamento delle partizioni")
        start = time()

        if rdd_dataframe == "Dataframe":
            dataset = data.select(self.column_list)
            logging.info(f"Partizioni DF dopo select: {dataset.rdd.getNumPartitions()}")
            # Una sola repartition per allineare al livello di parallelismo desiderato
            dataset = dataset.repartition(self.num_partitions)
            logging.info(f"Partizioni DF dopo repartition: {dataset.rdd.getNumPartitions()}")
        else:
            dataset = data.map(Lambda.set_tupla)
            logging.info(f"Partizioni RDD dopo map: {dataset.getNumPartitions()}")
            dataset = dataset.repartition(self.num_partitions)
            logging.info(f"Partizioni RDD dopo repartition: {dataset.getNumPartitions()}")

        end = time()
        logging.info(f"Filtraggio completato in {end - start:.3f} s")
        return dataset

    def save_filtered_dataset(self, dataset, rdd_dataframe: str, hdfs_path: str):
        logging.info("Salvataggio del dataset filtrato")
        start = time()

        if rdd_dataframe == "Dataframe":
            path = hdfs_path + "dataset/parquet_data"
            logging.info(f"Partizioni DF in scrittura: {dataset.rdd.getNumPartitions()}")
            dataset.write.mode("overwrite").parquet(path)
        else:
            path = hdfs_path + "dataset/dataset.csv"
            logging.info(f"Partizioni RDD in scrittura: {dataset.getNumPartitions()}")
            dataset.saveAsTextFile(path)

        end = time()
        logging.info(f"Dataset salvato ({path}) in {end - start:.3f} s")


def main():
    logging.basicConfig(
        filename="optimizer.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Inizializzazione pre-processing PySpark")

    config = {
        "host": sys.argv[1],
        "port": sys.argv[2],
        "directory": sys.argv[3],
        "RDD_Dataframe": sys.argv[4],
        "num_partitions": int(sys.argv[5])
    }

    hdfs_file_path = config["host"] + config["port"] + config["directory"]
    logging.info(f"URI base dataset: {hdfs_file_path}")

    rdd_dataframe = config["RDD_Dataframe"]
    logging.info(f"Sessione di lavoro con {rdd_dataframe}")

    # NB: qui puoi mettere master="local[*]" se giri tutto su un host singolo
    spark = (
        SparkSession.builder
        .appName("Progetto-1-preprocessing")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    logging.info("Creazione oggetto Optimizer")
    Optimizer(spark, hdfs_file_path, rdd_dataframe, config["num_partitions"])


if __name__ == "__main__":
    main()