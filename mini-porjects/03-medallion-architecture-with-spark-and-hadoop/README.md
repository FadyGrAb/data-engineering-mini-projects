1. Build spark:homemade
2. run: `docker compose up -d`
3. run:
   ```bash
   docker compose exec hadoop-namenode bash -c "/opt/hadoop/init.sh"
   ```
4. Activate retailapp env and run:
   ```bash
   python retailapp.py
   ```
5. Activate orchestrator env and run:
   ```bash
   python orchestrator.py
   ```
6. To check `docker compose exec hadoop-namenode bash -c "hdfs dfs -ls /data/bronze/ordershistory"`

http://localhost:50070/webhdfs/v1/?op=LISTSTATUS
http://localhost:50070/webhdfs/v1/user/hadoop/example.txt?op=OPEN
curl -i -X PUT "http://localhost:50070/webhdfs/v1/user/hadoop/newfile.txt?op=CREATE&overwrite=true"
http://localhost:50070/webhdfs/v1/user/hadoop/example.txt?op=DELETE
http://localhost:50070/webhdfs/v1/user/hadoop/example.txt?op=GETFILESTATUS
http://localhost:50075/webhdfs/v1/jobs/ingestion.py?op=OPEN&namenoderpcaddress=localhost:8020&offset=0