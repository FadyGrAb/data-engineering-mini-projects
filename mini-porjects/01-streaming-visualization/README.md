# Streaming Visualization
Streaming data real-time visualization using:
- Apache Kafka
- Apache Pinot
- Apache Superset

## System architecture:


## Instructions:
Move to the project's root directory and perform the following.
- Create the external docker network
  ```bash
  docker network create streaming-nt 
  ```
- Spinning up the Pinot and Kafka containers containers
  ```bash
  docker compose up -d
  ```
- Create the topic 
  ```bash
  docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic network-alarms --bootstrap-server kafka:9092
  ```
- Create a python virtual environment
  ```bash
  python -m venv venv
  # Or if you have conda installed
  conda create -p ./venv python<3.12 -y
  ```
- Activate the virtual env
  ```bash
  source venv/scripts/activate
  # Or with conda
  conda activate ./venv
  ```
- Install the pip requirements
  ```bash
  pip install -r requirements.txt
  ```
- Copy the Pinot schema and table config in the Pinot controller container and add them.
  ```bash
  # Copy the schema and config files
  docker cp pinot/. pinot-controller:/tmp/pinot/
  # Use Pinot Admin Tool to add the Schema and the Table configs
  docker exec -it pinot-controller \
    bin/pinot-admin.sh AddTable \
      -tableConfigFile /tmp/pinot/table-config-alarms.json \
      -schemaFile /tmp/pinot/schema-alarms.json \
      -controllerHost pinot-controller \
      -controllerPort 9000 \
      -exec
  ```
- Clone the Apache Superset repo
  ```bash
  git clone --depth=1  https://github.com/apache/superset.git
  ```
- Create a `docker/requirements-local.txt` inside the docker directory in the newly cloned repo folder. Write inside it `pinotdb`
- Change directory into the superset folder and spin-up the Apache Superset docker compose file
  ```bash
  docker compose -f docker-compose-image-tag.yml up
  ```
- Enter the connection string while connecting to the Pinot database through Superset UI on http://localhost:8088
  ```
  pinot://pinot-broker:8099/query?controller=http://pinot-controller:9000/
  ```

## Take away points:
