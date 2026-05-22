import subprocess

# script da eseguire da /Datastore
#docker build -f master/Dockerfile -t hdfs-master .  

# creazione docker network se non esiste
cmd = 'docker network create --driver bridge prj1'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

#stop dei container 
cmd = 'docker stop master slave1 slave2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

# rimozione dei container
cmd = 'docker rm master slave1 slave2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

# rimozione dell'immagine
cmd = 'docker rmi master slave1 slave2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")


# avvio del container del master
cmd = 'docker run -t -i -p 9864:9864 -d --network=prj1 --name=slave1 matnar/hadoop:3.3.2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

# avvio del container del master
cmd = 'docker run -t -i -p 9863:9864 -d --network=prj1 --name=slave2 matnar/hadoop:3.3.2'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")

# avvio del container del master
cmd = 'docker run -t -i -p 9870:9870 -p 54310:54310 -d --network=prj1 --name=master -v master-volume:/hadoop/dfs/data datastore-hdfs-master'
res = subprocess.run(cmd, shell=True, capture_output=True).stdout.decode("utf-8")








#1.docker run -d --name master --network project1-network -p 9870:9870 -p 8088:8088 -p 9864:9864 -e NODETYPE=master -v master-volume:/hadoop/dfs/data matnar/hadoop
#2.docker run -d --name slave1 --network project1-network -e NODE_TYPE=worker matnar/hadoop
#3.docker run -d --name slave2 --network project1-network -e NODE_TYPE=worker matnar/hadoop


