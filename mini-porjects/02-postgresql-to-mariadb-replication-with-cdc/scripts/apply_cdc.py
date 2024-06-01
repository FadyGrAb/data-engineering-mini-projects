import json
import os
import sys
from pprint import pprint

import mariadb
import pika
from dotenv import load_dotenv

load_dotenv()

conn_settings = {
    "host": os.environ.get("MDB_HOST", "localhost"),
    "user": os.environ.get("MDB_USER", "mariadb"),
    "password": os.environ.get("MDB_PASSWORD", "mariadb"),
    "database": os.environ.get("MDB_DB", "requests"),
}

conn_db = mariadb.connect(**conn_settings)
cur = conn_db.cursor()


def insert(data, cur):
    table = data["table"]
    columns = ",".join([str(item) for item in data["columnnames"]])
    placeholders = ", ".join(["%s"] * len(data["columnvalues"]))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    cur.execute(query, data["columnvalues"])


def update(data, cur):
    table = data["table"]
    update = ", ".join([f"{col} = %s" for col in data["columnnames"]])
    id_name = data["oldkeys"]["keynames"][0]
    id_value = data["oldkeys"]["keyvalues"][0]
    query = f"UPDATE {table} SET {update} WHERE {id_name} = {id_value}"
    cur.execute(query, data["columnvalues"])


def delete(data, cur):
    table = data["table"]
    id_name = data["oldkeys"]["keynames"][0]
    id_value = data["oldkeys"]["keyvalues"][0]
    query = f"DELETE FROM {table} WHERE {id_name} = {id_value}"
    cur.execute(query)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="cdc")

    def callback(ch, method, properties, body):
        print(f" [RECEIVED] ".center(40, "-"))
        body = json.loads(body.decode())
        for record in body:
            if record["kind"] == "insert":
                try:
                    insert(record, cur)
                    conn_db.commit()
                except mariadb.Error as e:
                    print(e)
            elif record["kind"] == "update":
                try:
                    update(record, cur)
                    conn_db.commit()
                except mariadb.Error as e:
                    print(e)
            elif record["kind"] == "delete":
                try:
                    delete(record, cur)
                    conn_db.commit()
                except mariadb.Error as e:
                    print(e)

            pprint(record)

    channel.basic_consume(queue="cdc", on_message_callback=callback, auto_ack=True)

    print("Waiting for CDC from queue. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Aborted!")
        sys.exit(0)
    finally:
        cur.close()
        conn_db.close()
