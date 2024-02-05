import yaml
from indexer import Indexer
from log_reader import LogReader
import queue
import time
import threading

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

if __name__ == "__main__":
    config = load_config("config.yaml")

    es_endpoint = config['es_endpoint']
    services = config['services']

    # Dictionary to hold queues and LogReader instances for each service
    service_queues = {}
    log_readers = {}

    for service in services:
        log_queue = queue.Queue()
        service_queues[service] = log_queue

        log_reader = LogReader(service, log_queue)
        log_reader.start()
        log_readers[service] = log_reader

        indexer = Indexer(es_endpoint, log_queue, service)
        indexer.start_indexing()

    try:
        while True:
            time.sleep(0.1)  # Main thread can perform other tasks or just sleep
    except KeyboardInterrupt:
        # Stop all log readers and indexers
        for service in services:
            log_readers[service].stop()
            indexer.stop_indexing()
