#!/bin/sh

hdfs namenode -format

$HADOOP_HOME/sbin/start-dfs.sh

hdfs dfs -mkdir -p /user/root

hdfs dfs -mkdir /user/root/data

cd /hadoop/dfs/data

hdfs dfs -put 202501_T_ONTIME_REPORTING.csv data/
hdfs dfs -put 202502_T_ONTIME_REPORTING.csv data/
hdfs dfs -put 202503_T_ONTIME_REPORTING.csv data/
hdfs dfs -put 202504_T_ONTIME_REPORTING.csv data/

hdfs dfs -chown -R airflow data/

hdfs dfs -chown airflow data/

