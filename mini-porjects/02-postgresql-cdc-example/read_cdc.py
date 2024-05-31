import json
import time
from pprint import pprint

import psycopg2

# PostgreSQL connection settings
dbname = "requests"
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"

# Replication slot name
slot_name = "slot_cdc"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=dbname, user=user, password=password, host=host, port=port
)

# Open a cursor
cur = conn.cursor()

try:
    # Continuously fetch new data
    while True:
        # Execute the SQL query to retrieve change records
        cur.execute(
            "SELECT * FROM pg_logical_slot_get_changes(%s, NULL, NULL)", (slot_name,)
        )

        # Fetch all change records
        rows = cur.fetchall()

        # Process each row, which contains the change record as JSON data
        for row in rows:
            change_record = row[2]
            change_record = json.loads(change_record)["change"]
            for change in change_record:
                pprint(change)

        # Wait for a period before fetching new data again
        time.sleep(1)  # Adjust sleep time as needed

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()
