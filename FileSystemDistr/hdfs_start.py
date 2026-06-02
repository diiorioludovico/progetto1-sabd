import subprocess


# avvio del container del worker
cmd = 'docker run -t -i -p 9864:9864 -d --network=prj1 --name=slave1 matnar/hadoop:3.3.2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

# avvio del container del master
cmd = 'docker run -t -i -p 9870:9870 -p 54310:54310 -d --network=prj1 --name=master -v master-volume:/hadoop/dfs/data datastore-hdfs-master'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

