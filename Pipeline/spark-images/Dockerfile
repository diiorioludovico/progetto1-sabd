# Usa l'immagine ufficiale PySpark come base
FROM apache/spark-py:v3.4.0

# Passa a root se devi installare pacchetti extra
USER root

RUN mkdir -p /opt/spark/work && chmod -R 777 /opt/spark/work

RUN apt-get update && apt-get install -y vim



     