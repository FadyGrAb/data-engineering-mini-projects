#!/usr/bin/env python
import os
import sys

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="cdc")

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

    channel.basic_consume(queue="cdc", on_message_callback=callback, auto_ack=True)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Aborted!")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
