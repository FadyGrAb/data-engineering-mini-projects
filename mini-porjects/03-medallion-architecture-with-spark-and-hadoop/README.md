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