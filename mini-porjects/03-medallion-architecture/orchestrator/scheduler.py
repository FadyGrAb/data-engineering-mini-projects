import json
import os
from pprint import pprint

import requests
from dotenv import load_dotenv

load_dotenv()


def submit_job(master_url, app_name, args=None):
    url = f"{master_url}/v1/submissions/create"
    data = {
        "appResource": "",
        "sparkProperties": {
            # "spark.submit.deployMode": "client",
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": app_name,
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.jars": "/spark/jars/postgresql-jdbc.jar",
        },
        "clientSparkVersion": "",
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "environmentVariables": {
            "DB_USERNAME": os.environ.get("DB_USERNAME"),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
        },
        "action": "CreateSubmissionRequest",
        "appArgs": args,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    return response.json()


if __name__ == "__main__":
    py_file_path = "hdfs://hadoop-namenode:8020/jobs/ingestion.py"
    pprint(
        submit_job(
            master_url="http://localhost:6066",
            app_name="Ingestion",
            args=[
                py_file_path,
            ],
        )
    )
