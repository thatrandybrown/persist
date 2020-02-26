import pika
from collections import namedtuple
import sys
import json
from tinydb import TinyDB

literal = lambda **kwargs : namedtuple('literal', kwargs)(**kwargs)

def write_to_disk(target, item):
    db = TinyDB(target)
    event_id = db.insert(item)
    db.close()
    return event_id

def start_message_listener(host, queue, persistence_target):
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
            config['message']['consumption_queue'],
            config['persistence']['target']
        )
    return literal(start=start)

app = makeApp(json.loads(sys.argv[1])) if len(sys.argv) == 2 else makeApp({})
app.start()