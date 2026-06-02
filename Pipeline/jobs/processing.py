from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

from time import time

import logging
import sys


class Lambda:
    # ===== QUERY 1 (RDD) =====

    @staticmethod
    def filter_carrier(x):
        # x: [month, carrier, dep_delay, arr_delay, cancelled, diverted, ...]
        return x[1] in ("AA", "DL")

    @staticmethod
    def seq_func1(acc, v):
        # v: (dep_delay, arr_delay, cancelled, diverted, ...)
        cancelled = float(v[2])

        sum_delay, min_delay, max_delay, cancelled_count, total_count = acc
        total_count += 1

        if cancelled == 1.0:
            cancelled_count += 1
        else:
            dep_delay = float(v[0])
            sum_delay += dep_delay
            min_delay = min(min_delay, dep_delay)
            max_delay = max(max_delay, dep_delay)

        return (
            sum_delay,
            min_delay,
            max_delay,
            cancelled_count,
            total_count,
        )

    @staticmethod
    def comb_func1(x, y):
        return (
            x[0] + y[0],
            min(x[1], y[1]),
            max(x[2], y[2]),
            x[3] + y[3],
            x[4] + y[4],
        )

    @staticmethod
    def tupla_finale1(x):
        # x: ((month, carrier), (sum_delay, min_delay, max_delay, cancelled_count, total_count))
        sum_delay, min_delay, max_delay, cancelled_count, total_count = x[1]
        voli_non_cancellati = total_count - cancelled_count
        mean = sum_delay / voli_non_cancellati if voli_non_cancellati > 0 else 0.0
        cancellation_rate = 100 * (cancelled_count / total_count) if total_count > 0 else 0.0

        return (
            f"{x[0][1]},{x[0][0]},{mean},{min_delay},{max_delay},{cancellation_rate}"
        )

    # ===== QUERY 2 (RDD) =====

    @staticmethod
    def seq_func2(acc, v):
        # v: (arr_delay, cancelled, diverted, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay)
        (
            tot_flights,
            n_canc_flights,
            n_div_flights,
            sum_arr_delay,
            n_delayed_flights,
            sum_carrier_delay,
            sum_weather_delay,
            sum_nas_delay,
            sum_security_delay,
            sum_late_aircraft_delay,
        ) = acc

        tot_flights += 1

        if float(v[2]) == 1.0:
            n_canc_flights += 1
        elif float(v[3]) == 1.0:
            n_div_flights += 1
        else:
            # voli non cancellati o deviati
            sum_arr_delay += float(v[1])

            # se ci sono le componenti di delay, le sommo
            if str(v[5]) != "":
                sum_carrier_delay += float(v[4])
                sum_weather_delay += float(v[5])
                sum_nas_delay += float(v[6])
                sum_security_delay += float(v[7])
                sum_late_aircraft_delay += float(v[8])
                n_delayed_flights += 1

        return (
            tot_flights,
            n_canc_flights,
            n_div_flights,
            sum_arr_delay,
            n_delayed_flights,
            sum_carrier_delay,
            sum_weather_delay,
            sum_nas_delay,
            sum_security_delay,
            sum_late_aircraft_delay,
        )

    @staticmethod
    def comb_func2(x, y):
        return tuple(x[i] + y[i] for i in range(10))

    @staticmethod
    def tupla_query2(x):
        # x: (carrier, (tot_flights, n_canc_flights, n_div_flights, sum_arr_delay, n_delayed_flights, ...))
        tot_flights, n_canc, n_div, sum_arr_delay, n_delayed, scd, swd, snd, ssd, slad = x[1]
        n_flights = tot_flights - n_canc - n_div

        if n_flights > 0 and n_delayed > 0:
            arr_delay_mean = sum_arr_delay / n_flights
            carrier_delay_mean = scd / n_delayed
            weather_delay_mean = swd / n_delayed
            nas_delay_mean = snd / n_delayed
            security_delay_mean = ssd / n_delayed
            late_aircraft_delay_mean = slad / n_delayed
        else:
            arr_delay_mean = 0.0
            carrier_delay_mean = 0.0
            weather_delay_mean = 0.0
            nas_delay_mean = 0.0
            security_delay_mean = 0.0
            late_aircraft_delay_mean = 0.0

        return (
            x[0],
            n_flights,
            arr_delay_mean,
            carrier_delay_mean,
            weather_delay_mean,
            nas_delay_mean,
            security_delay_mean,
            late_aircraft_delay_mean,
        )

    @staticmethod
    def tupla_finale2(x):
        return ",".join(str(v) for v in x)


class Executor:
    def __init__(self, spark, hdfs_path, rdd_dataframe: str):
        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        dataset = self.carica_dataset(spark, hdfs_path, rdd_dataframe)
        # cache condiviso tra le due query
        dataset = dataset.cache()
        dataset.count()  # forza il caching

        self.query1(dataset, rdd_dataframe, hdfs_path)
        self.query2(dataset, rdd_dataframe, hdfs_path)

    def carica_dataset(self, spark, hdfs_path, rdd_dataframe: str):
        logging.info("Caricamento del dataset per il processing")
        start = time()

        if rdd_dataframe == "Dataframe":
            path = hdfs_path + "dataset/parquet_data"
            dataset = spark.read.parquet(path)
        else:
            path = hdfs_path + "dataset/dataset.csv"
            dataset = spark.sparkContext.textFile(path).map(lambda line: line.split(","))

        end = time()
        logging.info(f"Caricamento completato in {end - start:.3f} s")
        return dataset

    def query1(self, dataset, rdd_dataframe: str, hdfs_path: str):
        logging.info("Avvio query 1")
        start = time()

        if rdd_dataframe == "Dataframe":
            # DataFrame API
            filtered_df = dataset.filter(dataset.OP_UNIQUE_CARRIER.isin("AA", "DL"))
            df = filtered_df.groupBy(["MONTH", "OP_UNIQUE_CARRIER"]).agg(
                F.avg("DEP_DELAY").alias("dep_delay_mean"),
                F.min("DEP_DELAY").alias("dep_delay_min"),
                F.max("DEP_DELAY").alias("dep_delay_max"),
                (F.sum("CANCELLED") / F.count("*") * 100).alias("cancellation_rate"),
            )

            df.write.mode("overwrite").option("header", "true").csv(hdfs_path + "output/query1.csv")
        else:
            # RDD API
            result_rdd = (
                dataset.filter(Lambda.filter_carrier)
                .map(lambda x: ((x[0], x[1]), x[2:]))  # key: (month, carrier)
                .aggregateByKey(
                    (0.0, float("inf"), float("-inf"), 0, 0),
                    Lambda.seq_func1,
                    Lambda.comb_func1,
                )
                .map(Lambda.tupla_finale1)
            )

            # coalesce(1) solo per avere un unico file in output
            result_rdd.coalesce(1).saveAsTextFile(hdfs_path + "output/query1.csv")

        end = time()
        logging.info(f"Query 1 completata in {end - start:.3f} s")

    def query2(self, dataset, rdd_dataframe: str, hdfs_path: str):
        logging.info("Avvio query 2")
        start = time()

        if rdd_dataframe == "Dataframe":
            # DataFrame API
            temp_df = dataset.groupBy("OP_UNIQUE_CARRIER").agg(
                (F.count("*") - F.sum("CANCELLED") - F.sum("DIVERTED")).alias("n_flights"),
                F.sum("ARR_DELAY").alias("sum_arr_delay"),
                F.sum("CARRIER_DELAY").alias("sum_carrier_delay"),
                F.sum("WEATHER_DELAY").alias("sum_weather_delay"),
                F.sum("NAS_DELAY").alias("sum_nas_delay"),
                F.sum("SECURITY_DELAY").alias("sum_security_delay"),
                F.sum("LATE_AIRCRAFT_DELAY").alias("sum_late_aircraft_delay"),
                F.count("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay_count"),
            )

            df = (
                temp_df.withColumn("arr_delay_mean", F.col("sum_arr_delay") / F.col("n_flights"))
                .withColumn("carrier_delay_mean", F.col("sum_carrier_delay") / F.col("late_aircraft_delay_count"))
                .withColumn("weather_delay_mean", F.col("sum_weather_delay") / F.col("late_aircraft_delay_count"))
                .withColumn("nas_delay_mean", F.col("sum_nas_delay") / F.col("late_aircraft_delay_count"))
                .withColumn("security_delay_mean", F.col("sum_security_delay") / F.col("late_aircraft_delay_count"))
                .withColumn("late_aircraft_delay_mean", F.col("sum_late_aircraft_delay") / F.col("late_aircraft_delay_count"))
                .drop("sum_arr_delay", "sum_carrier_delay", "sum_weather_delay",
                      "sum_nas_delay", "sum_security_delay", "sum_late_aircraft_delay",
                      "late_aircraft_delay_count")
            )

            # ordino e prendo i top 10
            df_top10 = df.orderBy(F.col("arr_delay_mean").desc()).limit(10)
            df_top10.write.mode("overwrite").option("header", "true").csv(hdfs_path + "output/query2.csv")
        else:
            # RDD API
            agg_rdd = (
                dataset.map(lambda x: (x[1], x[2:]))  # key: carrier
                .aggregateByKey(
                    tuple(0.0 for _ in range(10)),
                    Lambda.seq_func2,
                    Lambda.comb_func2,
                )
                .filter(lambda x: x[1][0] - x[1][1] - x[1][2] > 500)  # almeno 500 voli non canc/dev
                .map(Lambda.tupla_query2)
            )

            
            top10 = agg_rdd.takeOrdered(10, key=lambda x: -x[2])

            result_rdd = dataset.context.parallelize(top10).map(Lambda.tupla_finale2)
            result_rdd.coalesce(1).saveAsTextFile(hdfs_path + "output/query2.csv")

        end = time()
        logging.info(f"Query 2 completata in {end - start:.3f} s")


def main():
    logging.basicConfig(
        filename="executor.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logging.info("Inizializzazione processamento PySpark")

    config = {
        "host": sys.argv[1],
        "port": sys.argv[2],
        "directory": sys.argv[3],
        "RDD_Dataframe": sys.argv[4],
        "num_cores": sys.argv[5]
    }

    hdfs_file_path = config["host"] + config["port"] + config["directory"]
    logging.info(f"URI base dataset: {hdfs_file_path}")

    rdd_dataframe = config["RDD_Dataframe"]
    logging.info(f"Sessione di lavoro con {rdd_dataframe}")

    
    num_cores = config["num_cores"]
    if num_cores == "0":
        num_cores = "8"
    logging.info(f"Sessione di lavoro con {num_cores} core")

    spark = (
        SparkSession.builder
        .appName("Progetto-1-processing")
        .master("spark://spark-master:7077")
        .config("spark.executor.cores", num_cores)    
        .config("spark.cores.max", num_cores) 
        .getOrCreate()
    )

    logging.info("Creazione oggetto Executor")
    Executor(spark, hdfs_file_path, rdd_dataframe)


if __name__ == "__main__":
    main()
