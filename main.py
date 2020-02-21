#!/usr/bin/env python

import pika
from collections import namedtuple

literal = lambda **kwargs : namedtuple('literal', kwargs)(**kwargs)

def start_message_listener(host, queue):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host))
    rcv_channel = connection.channel()

    rcv_channel.queue_declare(queue=queue)

    rcv_channel.basic_consume(
        queue=queue,
        on_message_callback=print,
        auto_ack=True
    )

    print(' [*] Waiting for messages.')
    rcv_channel.start_consuming()