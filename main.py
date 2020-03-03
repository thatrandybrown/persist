import pika
from collections import namedtuple
import sys
import json
from functools import reduce
from tinydb import TinyDB, where

literal = lambda **kwargs : namedtuple('literal', kwargs)(**kwargs)

def write_to_disk(target, item):
    db = TinyDB(target)
    event_id = db.insert(item)

    # this algorithm will not work for fancier data structures, probably
    all_entries = db.search(where('id') == item['id'])
    db.close()

    dated_entries = filter(lambda x : 'timestamp' in x.keys(), all_entries)
    sorted_entries = sorted(dated_entries, key=lambda item: item['timestamp'])
    return reduce(lambda x,y : {**x, **y}, sorted_entries, {})

def start_message_listener(host, rcv_queue, snd_queue, persistence_target):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host))
    rcv_channel = connection.channel()

    rcv_channel.queue_declare(queue=rcv_queue)

    rcv_channel.basic_consume(
        queue=rcv_queue,
        on_message_callback=manage_message_ingest(host, snd_queue, persistence_target),
        auto_ack=True
    )

    print(' [*] Waiting for messages.')
    rcv_channel.start_consuming()

def makeApp(config):
    def start():
        start_message_listener(
            config['message']['uri'],
            config['message']['consumption_queue'],
            config['message']['send_queue'],
            config['persistence']['target']
        )
    return literal(start=start)

app = makeApp(json.loads(sys.argv[1])) if len(sys.argv) == 2 else makeApp({})
app.start()