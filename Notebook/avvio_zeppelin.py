import subprocess

# avvio del container del master
cmd = 'docker run -d -p 8080:8080 -v spark:/opt/spark -e SPARK_HOME=/opt/spark/spark-3.5.8-bin-hadoop3 --network=prj1 --name zeppelin apache/zeppelin:0.12.0'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

