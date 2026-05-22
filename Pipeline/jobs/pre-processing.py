from pyspark.sql import SparkSession # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F # pyright: ignore[reportMissingImports]

from time import time

import logging
import sys

class Lambda():

    @staticmethod
    def get_column(dataframe, column_list):
            column = []

            for index, name in column_list:
                if dataframe:
                    el = name
                else:
                    el = index
                column.append(el)
            
            return column
    
    @staticmethod
    def set_tupla(x):
        return (
            str(x[1]) + "," +
            str(x[3]) + "," +
            str(x[13])+ "," +
            str(x[16])+ "," +
            str(x[17])+ "," +
            str(x[19])+ "," +
            str(x[22])+ "," +
            str(x[23])+ "," +
            str(x[24])+ "," +
            str(x[25])+ "," +
            str(x[26])
        )


class Optimizer():

    column_list = [(1,"MONTH"), (3,"OP_UNIQUE_CARRIER"), (13,"DEP_DELAY"),
                   (16,"ARR_DELAY"), (17,"CANCELLED"), (19,"DIVERTED"),
                   (22,"CARRIER_DELAY"), (23,"WEATHER_DELAY"), (24,"NAS_DELAY"),
                   (25,"SECURITY_DELAY"), (26,"LATE_AIRCRAFT_DELAY")]
    
    def __init__(self, spark, hdfs_path, rdd_dataframe):
        # avvio sparkContext
        sc = spark.sparkContext

        sc.setLogLevel("WARN")  # Riduce il rumore nei log

        # caricamento del dataset da hdfs
        raw_dataset = self.carica_dataset(spark, hdfs_path, rdd_dataframe)

        #rimuoviamo dal modello dati le colonne che non servono
        dataset = self.column_filter(raw_dataset, rdd_dataframe)

        self.save_filtered_dataset(dataset, rdd_dataframe, hdfs_path)


    def carica_dataset(self, spark, hdfs_path, rdd_dataframe):
        logging.info(f"Caricamento del dataset")
        start = time()

        if rdd_dataframe == "Dataframe":
            # carichiamo il dataset su un dataframe

            dataset = spark.read.csv(hdfs_path, inferSchema="true", header="true")
            #logging.info(f"Righe totali su Dataframe: {dataset.count()}")   
            logging.info(f"Numero partizioni del Dataframe: {dataset.rdd.getNumPartitions()}")   
        else:
            # carichiamo il dataset su un rdd

            dataset_temp = spark.sparkContext.textFile(hdfs_path + "*.csv") 
            header = dataset_temp.first()
            dataset = dataset_temp.filter(lambda line: line != header).map(lambda line: line.split(","))
            logging.info(f"Numero partizioni del RDD: {dataset.getNumPartitions()}")  
        
        end = time()
        logging.info(f"Caricamento del dataset avvenuto con successo in {end-start} s")

        return dataset
        
    def column_filter(self, data, rdd_dataframe):
        logging.info(f"Filtrando le colonne del dataset")
        start = time()

        if rdd_dataframe == "Dataframe":
            # dataset è un dataframe

            dataset = data.select(Lambda.get_column(True, self.column_list))
            logging.info(f"Prima riga DF: {dataset.first()}")
        else:
            # dataset è un rdd

            dataset = data.map(Lambda.set_tupla)
            logging.info(f"Prima riga RDD: {dataset.first()}")
        
        end = time()
        logging.info(f"Filtraggio delle colonne del dataset avvenuto con successo in {end-start} s")

        return dataset
    
    def save_filtered_dataset(self, dataset, rdd_dataframe, hdfs_path):
        logging.info(f"Salvataggio del dataset su HDFS")
        start = time()

        if rdd_dataframe == "Dataframe":
            # salviamo il dataframe in formato parquet

            #dataset.write.mode("overwrite").option("header", "true").csv(hdfs_path + "dataset/dataset.csv")
            dataset.write.mode("overwrite").parquet("dataset/")
        else:
            # salviamo l'RDD in formato RDD

            dataset.saveAsTextFile(hdfs_path + "dataset/dataset.csv")
        
        end = time()
        logging.info(f"Salvataggio del dataset su HDFS avvenuto con successo in {end-start} s")

def main():
    logging.basicConfig(
        filename='optimizer.log',
        filemode='w',  # append (usa 'w' per sovrascrivere ogni volta)
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Inizializzazione del processo di pre-processing Pyspark")

    config = {
        "host": sys.argv[1],
        "port": sys.argv[2],
        "directory": sys.argv[3],
        "RDD_Dataframe": sys.argv[4]
    }

    hdfs_file_path = config.get("host") + config.get("port") + config.get("directory")
    logging.info(f"URI file su HDFS cluster: {hdfs_file_path}")
    #print(hdfs_file_path)

    rdd_dataframe = config.get("RDD_Dataframe")
    logging.info(f"Sessione di lavoro con {rdd_dataframe}")

    # Crea la SparkSession 
    spark = SparkSession.builder.appName("Progetto-1").master("spark://spark-master:7077").getOrCreate()

    logging.info("Creazione oggetto Optimizer")
    optimizer = Optimizer(spark, hdfs_file_path, rdd_dataframe)

if __name__ == "__main__":
    main()