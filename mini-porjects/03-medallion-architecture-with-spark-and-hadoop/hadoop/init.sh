hdfs dfs -mkdir -p /data/bronze/orders /data/bronze/ordershistory 
hdfs dfs -mkdir -p /data/silver /data/gold /jobs
hdfs dfs -chmod 777 /data/**
hdfs dfs -put ./pipeline/*.py /jobs/
hdfs dfs -chmod 744 /jobs/*