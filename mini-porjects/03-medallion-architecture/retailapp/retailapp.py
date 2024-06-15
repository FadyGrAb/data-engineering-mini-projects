import os
import random
from datetime import datetime, timedelta
from time import sleep
from colorama import Fore, Style

import psycopg2
from dotenv import load_dotenv

load_dotenv()


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
    cursor.execute(insert_history, ("NEW", order_time.strftime("%Y-%m-%d %H:%M:%S")))
    connection.commit()

    print(
        f"{Fore.YELLOW}[NEW]{Style.RESET_ALL}",
        branch,
        order_time.strftime("%Y-%m-%d %H:%M:%S"),
    )


def update_delivery(from_status: str, to_status: str) -> None:
    """Simulates the new order update

    Args:
        from_status (str): The original order status "INITIATED/INTRANSIT"
        to_status (str): The new order status "INTRANSIT/DELIVERED"
    """
    global order_time
    ids = get_last_status(from_status)
    if len(ids) > 0:
        orderid = random.choice(ids)
        insert_history = """
        INSERT INTO ordershistory (orderid, status, updatedat)
        VALUES
            (%s, %s, %s);
        """
        order_time += timedelta(minutes=random.randint(1, 59))
        order_time_str = order_time.strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            insert_history,
            (orderid, to_status, order_time_str),
        )
        color = Fore.CYAN if to_status == "INTRANSIT" else Fore.GREEN
        print(
            f"{color}[{to_status}]{Style.RESET_ALL}",
            orderid,
            order_time_str,
        )


def get_last_date() -> datetime:
    """
    Gets the last date in the oredershistory table or the current time
    """
    cursor.execute(
        """
            SELECT MAX(updatedat) FROM ordershistory;
        """
    )
    connection.commit()
    result = cursor.fetchall()[0][0]
    return result if result is not None else datetime.now()


def get_last_status(status: str) -> list:
    """
    Gets the orders last status
    """
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


db_username = os.environ.get("DB_USERNAME")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST", "localhost")

# Fictional retail branches
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

operations = ["NEW", "INTRANSIT", "DELIVERED"]
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
        if op == "NEW":
            new()
        elif op == "INTRANSIT":
            update_delivery("NEW", "INTRANSIT")
        elif op == "DELIVERED":
            update_delivery("INTRANSIT", "DELIVERED")

        sleep(0.25)

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
