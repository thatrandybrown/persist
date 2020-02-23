import pika
from collections import namedtuple
import sys
import json

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

def makeApp(config):
    def start():
        start_message_listener(
            config['message']['uri'],
            config['message']['consumption_queue']
        )
    return literal(start=start)

app = makeApp(json.loads(sys.argv[1])) if len(sys.argv) == 2 else makeApp({})
app.start()