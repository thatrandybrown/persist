#!/usr/bin/env python

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=''))
channel = connection.channel()

channel.queue_declare(queue='')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

channel.basic_consume(
    queue='', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages.)
channel.start_consuming()