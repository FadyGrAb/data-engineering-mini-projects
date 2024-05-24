# Simple Streaming Visualization
This project uses Kafka, Pinot, and Superset to ingest and visualize network alarms simulation

## Instructions:
Move to the project's root directory and perform the following.
- Spinning up the project's containers
  ```bash
  docker compose up -d
  ```
- Create the topic 
  ```bash
  docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic network-alarms --bootstrap-server localhost:9092
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

