import os
import random
from datetime import datetime, timedelta
from time import sleep
from colorama import Fore, Style

import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_username = os.environ.get("DB_USERNAME")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST", "localhost")

retail_branches = [
    "City Central Mall",
    "Metro Plaza",
    "Grand Avenue Center",
    "Harbor View Mall",
    "Oakwood Square",
    "Sunset Park Mall",
    "Pinecrest Shopping Center",
    "Riverfront Plaza",
    "Springfield Town Center",
    "Lakeside Mall",
]

operations = ["new", "intransit", "delivered"]


def new():
    """
    A function to simulate new orders
    """

    global order_time

    insert_order = """
    INSERT INTO orders (ordertime, branch)
    VALUES
        (%s, %s);
    """

    insert_history = """
    INSERT INTO ordershistory (orderid, status, updatedat)
    VALUES
        ((SELECT MAX(orderid) FROM orders), %s, %s);
    """

    # Setting weights for choices to make some branches appear more in the analysis.
    weights = [
        3,
        2,
        2,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
    ]

    branch = random.choices(retail_branches, weights=weights, k=1)[0]
    order_time += timedelta(minutes=random.randint(1, 59))

    # Begin a transaction
    cursor.execute("BEGIN;")
    cursor.execute(insert_order, (order_time.strftime("%Y-%m-%d %H:%M:%S"), branch))
    cursor.execute(
        insert_history, ("INITIATED", order_time.strftime("%Y-%m-%d %H:%M:%S"))
    )
    connection.commit()

    print(
        f"{Fore.YELLOW}[NEW]{Style.RESET_ALL}",
        branch,
        order_time.strftime("%Y-%m-%d %H:%M:%S"),
    )


def intransit():
    global order_time
    ids = get_last_status("INITIATED")
    if len(ids) > 0:
        orderid = random.choice(ids)
        insert_history = """
        INSERT INTO ordershistory (orderid, status, updatedat)
        VALUES
            (%s, %s, %s);
        """
        order_time += timedelta(minutes=random.randint(1, 59))
        cursor.execute(
            insert_history,
            (orderid, "INTRANSIT", order_time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        print(
            f"{Fore.CYAN}[INTRANSIT]{Style.RESET_ALL}",
            orderid,
            order_time.strftime("%Y-%m-%d %H:%M:%S"),
        )


def delivered():
    global order_time
    ids = get_last_status("INTRANSIT")
    if len(ids) > 0:
        orderid = random.choice(ids)
        insert_history = """
        INSERT INTO ordershistory (orderid, status, updatedat)
        VALUES
            (%s, %s, %s);
        """
        order_time += timedelta(minutes=random.randint(1, 59))
        cursor.execute(
            insert_history,
            (orderid, "DELIVERED", order_time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        print(
            f"{Fore.GREEN}[DELIVERED]{Style.RESET_ALL}",
            orderid,
            order_time.strftime("%Y-%m-%d %H:%M:%S"),
        )


def get_last_date() -> datetime:
    cursor.execute(
        """
            SELECT MAX(updatedat) FROM ordershistory;
        """
    )
    connection.commit()
    result = cursor.fetchall()[0][0]
    return result if result is not None else datetime.now()


def get_last_status(status: str) -> list:
    query = """
        WITH laststatus AS (
            SELECT
                oh.orderid,
                oh.status,
                oh.updatedat,
                ROW_NUMBER() OVER(PARTITION BY oh.orderid ORDER BY oh.updatedat DESC) as rn
            FROM ordershistory AS oh
        )
        SELECT
            ls.orderid
        FROM
            laststatus AS ls
        WHERE
            rn = 1 AND ls.status = %s;
    """
    cursor.execute(query, (status,))
    connection.commit()
    result = cursor.fetchall()
    flatten = [item for tpl in result for item in tpl]
    return flatten


try:

    connection_params = {
        "database": "retail",
        "user": db_username,
        "password": db_password,
        "host": db_host,
        "port": "5432",
    }

    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    order_time = get_last_date()
    while True:
        op = random.choices(operations, weights=[2, 1, 1], k=1)[0]
        if op == "new":
            new()
        elif op == "intransit":
            intransit()
        elif op == "delivered":
            delivered()

        sleep(0.5)

except psycopg2.Error as e:
    print(e)
    connection.rollback()
except KeyboardInterrupt:
    print("Aborted!")
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
