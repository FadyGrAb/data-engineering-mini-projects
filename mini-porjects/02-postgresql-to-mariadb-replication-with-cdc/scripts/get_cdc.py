import json
import os
import sys
import time
from pprint import pprint

import pika
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # Load env vars from .env file


conn_settings = {
    "dbname": os.environ.get("POSTGRES_DB", "requests"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    "host": os.environ.get("PSQL_HOST", "locahost"),
    "port": os.environ.get("PSQL_PORT", "5432"),
}

# Connect to Postgres and get a cursor
slot_name = os.environ.get("PSQL_LOGICAL_DECODING_SLOT_NAME", "slot_cdc")
conn_db = psycopg2.connect(**conn_settings)
cur = conn_db.cursor()

# Connect to RabbitMQ
conn_q = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = conn_q.channel()

channel.queue_declare(queue=os.environ.get("QUEUE_NAME", "cdc"))
routing_key = "cdc"

if __name__ == "__main__":
    try:
        print("Waiting for CDC data from Postgresq. To exit press CTRL+C")
        # Continuously fetch new data
        while True:
            # Execute the SQL query to retrieve change records
            cur.execute(
                "SELECT * FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                (slot_name,),
            )

            # Fetch all change records
            rows = cur.fetchall()

            # Process each row, which contains the change record as JSON data
            for row in rows:
                change_record = row[2]
                change_record = json.loads(change_record)["change"]

                for change in change_record:
                    # Send to queue
                    channel.basic_publish(
                        exchange="",
                        routing_key=routing_key,
                        body=json.dumps(change_record).encode(),
                    )
                    print(f" [SENT] ".center(40, "-"))
                    pprint(change)

            # Wait for a period before fetching new data again
            time.sleep(1)  # Adjust sleep time as needed
    except KeyboardInterrupt:
        print("Aborted!")
        sys.exit(0)
    finally:
        # Close the cursor and connections
        cur.close()
        conn_db.close()
        conn_q.close()
