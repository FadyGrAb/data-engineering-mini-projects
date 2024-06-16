import json
import os
from pprint import pprint
from time import sleep

import requests
from dotenv import load_dotenv

load_dotenv()


def submit_job(master_url, app_name, args=None):
    url = f"{master_url}/v1/submissions/create"
    data = {
        "appResource": "",
        "sparkProperties": {
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


def get_submission_state(submission_id):
    response = requests.get(
        url=f"http://localhost:6066/v1/submissions/status/{submission_id}"
    )

    return response.json().get("driverState", "NOTFOUND")


if __name__ == "__main__":
    py_file_path = "hdfs://hadoop-namenode:8020/jobs/ingestion.py"
    response = submit_job(
        master_url="http://localhost:6066",
        app_name="Ingestion",
        args=[
            py_file_path,
        ],
    )

    if response["success"]:
        submission_id = response["submissionId"]
        state = ""
        while not state in ["FINISHED", "FAILED", "NOTFOUND"]:
            current_state = get_submission_state(submission_id)
            if state != current_state:
                state = current_state
                print(state)
            sleep(0.1)
    else:
        print("Something went wrong:")
        pprint(response)
