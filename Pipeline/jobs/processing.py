from pyspark.sql import SparkSession # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F # pyright: ignore[reportMissingImports]

from time import time

import logging
import sys

class Lambda():
    # <===== QUERY 1 =====> 

    # funzione per filtrare le tuple e mantenere solamente quelle con carrier AA o DL
    # -> x: (month, carrier, dep_delay, arr_delay, cancelled, diverted, ...)
    @staticmethod
    def filter_carrier(x):
        return True if x[1] == "AA" or x[1] == "DL" else False

    # funzione per mappare la tupla del RDD nella tupla da restituire
    # -> v: ((month, carrier), (dep_delay, arr_delay, cancelled, diverted, ...))
    # -> acc: ((month, carrier), (dep_delay, cancelled, 1))
    @staticmethod
    def seq_func1(acc, v):
        
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
            total_count
        )

    # funzione per unire due tuple restituite dalla funzione seq_func
    # -> acc: ((month, carrier), (dep_delay, cancelled, 1))
    # -> (somma dep_delay, min dep_delay, max dep_delay, numero totale voli cancellati, numero totale voli)
    @staticmethod
    def comb_func1(x, y):

        return (
            x[0] + y[0],
            min(x[1], y[1]),
            max(x[2], y[2]),
            x[3] + y[3],
            x[4] + y[4]
        )
        
    # trasformazione finale per avere il formato da riportare su .csv
    # -> x: ((month, carrier), (somma dep_delay, min dep_delay, max dep_delay, numero totale voli cancellati, numero totale voli))
    # -> (carrier, month, dep_delay_mean, dep_delay_min, dep_delay_max, cancellation_rate)
    @staticmethod
    def tupla_finale1(x):

        voli_non_cancellati = x[1][4] - x[1][3]
        mean = str(x[1][0] / voli_non_cancellati)

        cancellation_rate = str(100 * (x[1][3] / x[1][4]))
        
        return (
            str(x[0][1]) + "," +
            str(x[0][0]) + "," +
            mean + "," +
            str(x[1][1]) + "," +
            str(x[1][2]) + "," +
            cancellation_rate
        )
    
    # <===== QUERY 2 =====>

    @staticmethod
    def seq_func2(acc, v):

        tot_flights, n_canc_flights, n_div_flights, sum_arr_delay, n_delayed_flights, \
        sum_carrier_delay, sum_weather_delay, sum_nas_delay, sum_security_delay, sum_late_aircraft_delay = acc

        # incrementiamo numero totale dei voli
        tot_flights += 1

        if float(v[2]) == 1.0:
            # incrementiamo numero totale dei voli cancellati
            n_canc_flights += 1
        elif float(v[3]) == 1.0:
            # incrementiamo numero totale dei voli deviati
            n_div_flights += 1
        else:
            # voli non cancellati o deviati -> arr_delay != Nan
            sum_arr_delay += float(v[1])

            if str(v[5]) != '':
                sum_carrier_delay += float(v[4])
                sum_weather_delay += float(v[5])
                sum_nas_delay += float(v[6])
                sum_security_delay += float(v[7])
                sum_late_aircraft_delay += float(v[8])
                n_delayed_flights += 1

        return (
            tot_flights, n_canc_flights, n_div_flights, sum_arr_delay, n_delayed_flights,
            sum_carrier_delay, sum_weather_delay, sum_nas_delay, sum_security_delay, sum_late_aircraft_delay
        )

    @staticmethod
    def comb_func2(x, y):

        return (
            x[0] + y[0],
            x[1] + y[1],
            x[2] + y[2],
            x[3] + y[3],
            x[4] + y[4],
            x[5] + y[5],
            x[6] + y[6],
            x[7] + y[7],
            x[8] + y[8],
            x[9] + y[9]
        )
    
    @staticmethod
    def tupla_query2(x):                                 
        n_flights = x[1][0]-x[1][1]-x[1][2]             # numero di voli non cancellati o deviati

        return (
            x[0],
            n_flights,
            x[1][3] / n_flights,
            x[1][5] / x[1][4],
            x[1][6] / x[1][4],
            x[1][7] / x[1][4],
            x[1][8] / x[1][4],
            x[1][9] / x[1][4]
        )
    
    #
    @staticmethod
    def tupla_finale2(x):

        return (
            str(x[0]) + "," +
            str(x[1]) + "," +
            str(x[2]) + "," +
            str(x[3]) + "," +
            str(x[4]) + "," +
            str(x[5]) + "," +
            str(x[6]) + "," +
            str(x[7]) 
        )

class Executor():

    def __init__(self, spark, hdfs_path, rdd_dataframe):
        # avvio sparkContext
        sc = spark.sparkContext

        sc.setLogLevel("WARN")  # Riduce il rumore nei log

        # caricamento del dataset da hdfs
        dataset = self.carica_dataset(spark, hdfs_path, rdd_dataframe).cache()
        # completato!

        # esecuzione query 1
        self.query1(dataset, rdd_dataframe, hdfs_path)
        # completato!

        # esecuzione query 2
        self.query2(dataset, rdd_dataframe, hdfs_path, sc)

    def carica_dataset(self, spark, hdfs_path, rdd_dataframe):
        logging.info(f"Caricamento del dataset")
        start = time()

        if rdd_dataframe == "Dataframe":
            # carichiamo il dataset su un dataframe

            #dataset = spark.read.csv(hdfs_path + "dataset/dataset.csv", inferSchema="true", header="true")
            dataset = spark.read.parquet(hdfs_path + "dataset/")

            #logging.info(f"Righe totali su Dataframe: {dataset.count()}")   
            logging.info(f"Numero partizioni del Dataframe: {dataset.rdd.getNumPartitions()}")   
        else:
            # carichiamo il dataset su un rdd
            dataset = spark.sparkContext.textFile(hdfs_path + "dataset/dataset.csv") \
                           .map(lambda line: line.split(","))

            
            logging.info(f"Prima riga RDD: {dataset.first()}")
        
        end = time()
        logging.info(f"Terminato caricamento del dataset in {end-start} s")

        return dataset
    
    def query1(self, dataset, rdd_dataframe, hdfs_path):
        logging.info(f"Avvio query 1")
        start = time()

        if rdd_dataframe == "Dataframe":
            #eseguiamo le trasformazioni sul Dataframe
            filtered_df = dataset.filter(dataset.OP_UNIQUE_CARRIER.isin('AA', 'DL')) # lasciamo solo le righe dei carrier AA e DL
            df = filtered_df.groupBy(["MONTH", "OP_UNIQUE_CARRIER"]).agg(
                    F.avg("DEP_DELAY").alias("dep_delay_mean"),
                    F.min("DEP_DELAY").alias("dep_delay_min"),
                    F.max("DEP_DELAY").alias("dep_delay_max"),
                    (F.sum("CANCELLED") / F.count("*") * 100).alias("cancellation_rate")
                )
            
            #logging.info(f"{df2.show(3)}")
            df.write.mode("overwrite").option("header", "true").csv(hdfs_path + "output/query1.csv")
        else:
            #eseguiamo le trasformazioni sul RDD : ('1', 'AA', '-3.00', '-7.00', '0.00', '0.00', '', '', '', '', '')

            # FILTER: manteniamo solamente le tuple relative ai carrier AA e DL
            # MAP: trasformiamo le tuple in coppie chiave-valore
            # key:      (mese, carrier)
            # value:    (dep_delay, arr_delay, ...) 
            # AGGREGATE_BY_KEY: mappiamo ogni tupla ad una nuova tupla (vedere lambda_func.seq_func e lambda_func.comb_func)
            # SAVE_AS_TEXT_FILE: azione che salva il file su HDFS
            dataset.filter(Lambda.filter_carrier) \
                   .map(lambda x: ((x[0], x[1]), (x[2:]))) \
                   .aggregateByKey((0.0, float("inf"), float("-inf"), 0, 0), Lambda.seq_func1, Lambda.comb_func1) \
                   .map(Lambda.tupla_finale1) \
                   .coalesce(1) \
                   .saveAsTextFile(hdfs_path + "output/query1.csv")
        
        end = time()
        logging.info(f"Query 1 completata in {end-start} s")
    
    def query2(self, dataset, rdd_dataframe, hdfs_path, sc):
        logging.info(f"Avvio query 2")
        start = time()

        if rdd_dataframe == "Dataframe":
            #eseguiamo le trasformazioni sul Dataframe

            temp_df = dataset.groupBy("OP_UNIQUE_CARRIER").agg(
                    (F.count("*") - F.sum("CANCELLED") - F.sum("DIVERTED")).alias("n_flights"),
                    F.sum("ARR_DELAY").alias("arr_delay_mean"),
                    F.sum("CARRIER_DELAY").alias("carrier_delay_mean"),
                    F.sum("WEATHER_DELAY").alias("weather_delay_mean"),
                    F.sum("NAS_DELAY").alias("nas_delay_mean"),
                    F.sum("SECURITY_DELAY").alias("security_delay_mean"),
                    F.sum("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay_mean"),
                    F.count("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay_count")
                )
            df = temp_df.withColumn("arr_delay_mean", F.col("arr_delay_mean") / F.col("n_flights")) \
                    .withColumn("carrier_delay_mean", F.col("carrier_delay_mean") / F.col("late_aircraft_delay_count")) \
                    .withColumn("weather_delay_mean", F.col("weather_delay_mean") / F.col("late_aircraft_delay_count")) \
                    .withColumn("nas_delay_mean", F.col("nas_delay_mean") / F.col("late_aircraft_delay_count")) \
                    .withColumn("security_delay_mean", F.col("security_delay_mean") / F.col("late_aircraft_delay_count")) \
                    .withColumn("late_aircraft_delay_mean", F.col("late_aircraft_delay_mean") / F.col("late_aircraft_delay_count")) \
                    .drop("late_aircraft_delay_count")
            
            df_top10 = df.orderBy(F.col("arr_delay_mean").desc()).limit(10)
            df_top10.write.mode("overwrite").option("header", "true").csv(hdfs_path + "output/query2.csv")
        else:
            #eseguiamo le trasformazioni sul RDD

            # MAP: organizziamo ogni tupla come un coppia chiave valore
            # key:      (carrier)
            # value:    (dep_delay, ...)
            # AGGREGATE_BY_KEY: mappatura di ogni tupla su una nuova
            # FILTER: rimuoviamo i carrier con meno di 500 voli non cancellati o deviati
            # MAP: trasformiamo le righe del RDD in righe di un .csv
            # SAVE_AS_TEXT_FILE: azione che salva il file su HDFS

            rdd_list = dataset.map(lambda x: ((x[1]), (x[2:])))  \
                          .aggregateByKey((tuple(0.0 for _ in range(10))), Lambda.seq_func2, Lambda.comb_func2) \
                          .filter(lambda x: x[1][0]-x[1][1]-x[1][2] > 500) \
                          .map(Lambda.tupla_query2) \
                          .sortBy(lambda x: x[2], ascending=False) \
                          .take(10) 
                          
            
            sc.parallelize(rdd_list).map(Lambda.tupla_finale2) \
                                    .coalesce(1) \
                                    .saveAsTextFile(hdfs_path + "output/query2.csv")
            
        end = time()
        logging.info(f"Query 2 completata in {end-start} s")

    

def main():
    logging.basicConfig(
        filename='executor.log',
        filemode='w',  # append (usa 'w' per sovrascrivere ogni volta)
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Inizializzazione dell'applicazione di processamento Pyspark")

    config = {
        "host": sys.argv[1],
        "port": sys.argv[2],
        "directory": sys.argv[3],
        "RDD_Dataframe": sys.argv[4]
    }

    hdfs_file_path = config.get("host") + config.get("port") + config.get("directory")# + str("202501_T_ONTIME_REPORTING.csv")
    logging.info(f"URI file su HDFS cluster: {hdfs_file_path}")
    #print(hdfs_file_path)

    rdd_dataframe = config.get("RDD_Dataframe")
    logging.info(f"Sessione di lavoro con {rdd_dataframe}")

    # Crea la SparkSession 
    spark = SparkSession.builder.appName("Progetto-1").master("spark://spark-master:7077").getOrCreate()

    logging.info("Creazione oggetto Executor")
    executor = Executor(spark, hdfs_file_path, rdd_dataframe)

if __name__ == "__main__":
    main()