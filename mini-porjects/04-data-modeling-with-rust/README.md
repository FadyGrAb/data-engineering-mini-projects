cd mini-porjects/04-data-modeling-with-rust
docker compose up postgres -d
bin/deploy
docker compose up pipeline -d

bin/get-duckdb

sudo chmod 777 pipeline-run/duckdb/pagila_sales.duckdb 

bin/duckdb pipeline-run/duckdb/pagila_sales.duckdb


## What is this project all about
In this project, I've implemented the *Star Schema* where you organise your data into *Fact* and *Dimension* tables. This format is preferable in the BI and data warehousing applications. What's new here, is that I've used *Rust* for the ETL pipeline to load the data into *DuckDB*.

## System architecture
![architecture](./diagrams/medallion-LLD.svg)
### The Retail app
I've simulated the retail app with a python script that randomly generates purchases and handle their lifecycle as NEW -> INTRANSIT -> DELIVERED. The app then stores that in a [Postgresql](https://www.postgresql.org/) database named *retail* that has two tables, *orders* and *ordershistory*. Separate users with appropriate privileges are created to the app to write the data to the database and to Spark, to read the data. The database is initiated with `postgresql/init.sql` script.

### Apache Spark Cluster
A [standalone ](https://spark.apache.org/docs/latest/spark-standalone.html) cluster with one master none (spark-master) and two worker nodes (spark-worker-1 and spark-worker-2). The REST API endpoint and history server are enabled. I've build its docker image and didn't use any image from Docker Hub.

### The Development Jupyter server
A jupyter server on the Master Node of the spark cluster running on port 8888 that is exposed to localhost in the Docker compose file. This is how I developed the pipeline's PySpark scripts. This allowed rapid and convenient development for the Spark jobs. The server is configures to have a password `pass` that can be changed from `spark/jupyter/jupyter_notebook_config.py` or via `JUPYTER_NOTEBOOK_PASSWORD` if passed to the Docker compose file.

### The Orchestrator
A python script that communicates with Apache Spark via its REST API endpoint to submit *PySpark* scripts stored on HDFS in `hdfs:<namenode>/jobs/*` and "orchestrate" the pipeline *Ingestion* -> *Transformation* -> *Serving* via Spark's drivers status API. All the PySpark files are in `spark/pipeline/`.

### Spark incremental database loading
The `ingestion.py` script implements a *bookmarking* mechanism which allows Spark to check for and sets bookmarks during the database data ingestion so it only loads new data and not to strain the app's databases with long running queries each time the pipeline is executed. The bookmark is stored in HDFS in `hdfs:<namenode>/data/bronze/bookmark` as JSON.

### Hadoop cluster
The official image on Docker Hub with minimal configuration changes on `hadoop/config` where WebHDFS is enabled.

### The dashboard app
A Flask app the read the csv file stored in the *Gold* folder in HDFS and outputs the resulting data as graphs. It meant to be the data consumer for this pipeline where business users can get their metrics.

## Spark Operations
### Bronze stage
Spark is incrementally ingesting the *orders* and *ordershistory* table partitions by date in *parquet* format in the *Bronze*.
### Silver stage
Spark joins the two table from the *Bronze* stage and filter out "INTRANSIT" records and stores them in the *Silver* directory in *parquet* format.
### Gold stage
Spark pivot the joined table to get one record per order that contains the order date and delivery date, subtracts then and get the delivery time in hours.
Then aggregates the result to get the orders count and average delivery time per branch. Finally, it stores them as CSV files in the *Gold* directory.

## Exposed ports and services:
I've configured the the majority of the the services in the Docker compose file to be accessible from localhost:
- **Dashboard App**: http://localhost:5000
- **Spark master node UI**: http://localhost:8080
- **Spark worker node 1 UI**: http://localhost:8081
- **Spark worker node 2 UI**: http://localhost:8082
- **Spark history server**: http://localhost:18080
- **Spark running driver UI**: http://localhost:4040
- **Jupyter server running on Spark cluster**: http://localhost:8888
- **Hadoop namenode UI**: http://localhost:9870

## Demo
![demo](./diagrams/demo.gif)
## Instructions
I assume that you have Python and Docker installed in your system.

- Go to the project's root directory.
- Create a Python virtual env and install the requirements in `./requirements.txt`
- Build the custom Spark docker image (this could take a while)
  ```bash
  docker build spark:homemade ./spark/
  ```
- Build the Dashboard app docker image
  ```bash
  docker build dashboard:homemade ./dashboard/
  ```
- Spin up the system
  ```bash
  docker compose up -d
  ```
- Activate the virtual env and run the retail app. You will see simulated transactions in your terminal.
  ```bash
  python ./retailapp/retailapp.py
  ```
- Open a new terminal and initiate HDFS directories and copy the pipeline PySpark scripts to HDFS
  ```bash
  docker compose exec hadoop-namenode bash -c "/opt/hadoop/init.sh"
- Go to the dashboard's app url (http://localhost:5000) and validate that there aren't any data yet.
- Wait a while for the retail app's database to populate then run the Spark pipeline withe the orchestrator
  ```bash
  python ./orchestrator/scheduler.py
  ```
- (Optional) You can check the progress from Spark's master node UI.
- After the pipeline finishes successfully (all stages are ended with FINISHED), visit the dashboard's app url again and refresh. You will get the processed data now.
- (Optional) You can rerun the pipeline and notice the dashboard's data changes.

## Stopping the project
I didn't configure any Named volumes on Docker, so once you stop the project, all the data is lost.
To stop the project just execute `docker compose down`

## Things to consider if you want to make this into real-world project
- Instead of the custom *orchestrator* I used, a proper orchestration tool should replace it like [Apache Airflow](https://airflow.apache.org/), [Dagster](https://dagster.io/), ..., etc.
- Also, instead of the custom Dashboard app, a proper BI tool like [Power BI](https://app.powerbi.com/home), [Tableau](https://www.tableau.com/), [Apache Superset](https://superset.apache.org/), ..., etc. will be more powerful and flexible.
- Bookmarking should be in the *Silver* stage as well.
- In a multi-tenant environment, controlling access to the Spark and Hadoop cluster is a must. Tool like  [Apache Ranger](https://ranger.apache.org/) are usually used to serve this purpose.
- This architecture isn't limited to ingesting from one source, multiple sources can be ingested too and enriched from other sources in the *Silver* stage.
- The Spark operation in this project are rather simple. More complex operations can be done leveraging Spark's powerful analytics engine.

## Key takeaways
- You can control almost everything in a spark cluster from the `spark-defaults.conf` file.
- Spark has a REST endpoint to submit jobs and manage the cluster but it isn't enabled by default.
- Spark history server isn't enabled by default.
- Don't use the latest Java version for Spark even if the docs says it's supported. (I had compatibility issue that appeared down the road as I was using Java 17).
- Developing Spark driver apps in a jupyter notebook will make your life way easier.
- Hadoop also has an http endpoint called WebHDFS which isn't enabled by default too.
- docker-entrypoint-initdb.d is very useful for Postgresql initiation. 
- Never use plaintext passwords in your code, instead you env vars or cloud secrets services.

## References
All the used tool official docs.