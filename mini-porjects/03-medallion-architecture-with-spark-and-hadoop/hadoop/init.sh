hdfs dfs -mkdir -p /data/bronze/orders /data/bronze/ordershistory /data/bronze/bookmark
hdfs dfs -mkdir -p /data/silver/allorders
hdfs dfs -mkdir -p /data/gold/
hdfs dfs -mkdir -p /jobs
hdfs dfs -chmod 777 /data/*
hdfs dfs -chmod 777 /data/*/*
hdfs dfs -put ./pipeline/*.py /jobs/
hdfs dfs -chmod 744 /jobs/*