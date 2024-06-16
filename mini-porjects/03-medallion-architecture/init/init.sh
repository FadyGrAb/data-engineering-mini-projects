docker compose exec hadoop-namenode bash -c "hdfs dfs -mkdir -p /data/bronze/orders /data/bronze/ordershistory /data/silver /data/gold /jobs && hdfs dfs -chmod 777 /data/** /jobs"
docker compose exec hadoop-namenode bash -c "hdfs dfs -put ./pipeline/* /jobs/"
docker compose exec hadoop-namenode bash -c "hdfs dfs -chmod 777 /jobs/*"