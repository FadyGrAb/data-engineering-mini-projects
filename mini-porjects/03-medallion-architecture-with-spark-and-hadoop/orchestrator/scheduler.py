import json
import os
from pprint import pprint
from time import sleep

import requests
from dotenv import load_dotenv

load_dotenv()


class FailedJobException(Exception):
    def __init__(self, message="", *args) -> None:
        super().__init__(*args)
        self.message = message

    def __str__(self) -> str:
        return self.message


class KilledJobException(Exception):
    def __init__(self, message="", *args) -> None:
        super().__init__(*args)
        self.message = message

    def __str__(self) -> str:
        return self.message


class NotFoundJobException(Exception):
    def __init__(self, message="", *args) -> None:
        super().__init__(*args)
        self.message = message

    def __str__(self) -> str:
        return self.message


class BadSubmissionRequestException(Exception):
    def __init__(self, message="", *args) -> None:
        super().__init__(*args)
        self.message = message

    def __str__(self) -> str:
        return self.message


def submit_job(master_rest_url, app_name, args=None, env=dict()):
    url = f"{master_rest_url}/v1/submissions/create"
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
        "environmentVariables": env,
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
    master_rest_url = os.environ.get("SPARK_REST_URL", "http://localhost:6066")
    hadoop_url = "hdfs://hadoop-namenode:8020/jobs"

    stages = [
        "ingestion",
        "transformation",
        "serving",
    ]
    try:
        for idx, stage in enumerate(stages):
            medallion = ""
            env = dict()
            if idx == 0:
                env = {
                    "DB_USERNAME": os.environ.get("DB_USERNAME"),
                    "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
                }
                medallion = "[Bronze]"
            elif idx == 1:
                medallion = "[Silver]"
            elif idx == 2:
                medallion = "[Gold]"

            print(f"{stage.capitalize()} {medallion}".center(100, "_"))

            py_file = f"{hadoop_url}/{stage}.py"

            response = submit_job(
                master_rest_url=master_rest_url,
                app_name=stage,
                args=[
                    py_file,
                ],
                env=env,
            )

            if response["success"]:
                submission_id = response["submissionId"]
                state = ""
                print("Executing:", py_file)
                print("Submission ID:", submission_id)
                while state != "FINISHED":
                    current_state = get_submission_state(submission_id)
                    if state != current_state:
                        state = current_state
                        print(state)
                        if state == "FAILED":
                            raise FailedJobException
                        elif state == "NOTFOUND":
                            raise NotFoundJobException
                        elif state == "KILLED":
                            raise KilledJobException
                    sleep(0.1)
            else:
                raise BadSubmissionRequestException

    except BadSubmissionRequestException:
        print("Couldn't submit to REST server!")
        pprint(response)

    except FailedJobException:
        print("Spark driver has failed!")

    except KilledJobException:
        print("Spark driver was killed!")

    except NotFoundJobException:
        print(f"Can't found this submission: {submission_id}")

    except KeyboardInterrupt:
        print("Aborted")

    except Exception as e:
        print(e)
