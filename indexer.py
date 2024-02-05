# indexer.py
import time
import threading
import json
import subprocess
from elasticsearch_connector import ElasticsearchConnector
# from log_to_doc_converter import LogToDocConverter
import requests

class Indexer:
    def __init__(self, es_endpoint, queue, service):
        self.es_connector = ElasticsearchConnector(es_endpoint)
        self.queue = queue
        self.stop_event = threading.Event()
        self.service = service
        self.es_endpoint = es_endpoint

    def index_document(self, index_name, document_id, document):
        return self.es_connector.index_document(index_name, document_id, document)

    def start_indexing(self):
        self.thread = threading.Thread(target=self.check_and_index)
        self.thread.start()

    def stop_indexing(self):
        self.stop_event.set()
        self.thread.join()

    # def gen_id(self, es_endpoint, index_name):

    #     if es_endpoint.endswith('/'):
    #         es_endpoint = es_endpoint[:-1]

    #     url = f"{es_endpoint}/{index_name}/_count"
        
    #     try:
    #         response = requests.get(url)
    #         if response.status_code == 404:
    #             # If index does not exist, return 1
    #             return 1
    #         response.raise_for_status()  # Raises a HTTPError if the HTTP request returned an unsuccessful status code
    #         result = response.json()
    #         return int(result.get("count", 0)) + 1
    #     except requests.RequestException as e:
    #         raise Exception(f"HTTP request error: {e}") 

    def check_and_index(self):
        print (f"Indexing doc to {self.service}_log")
        index_name = self.service + "_log"
        # count = self.gen_id(self.es_endpoint, index_name)
        count = 1
        while not self.stop_event.is_set():
            try:
                if not self.queue.empty():
                    document = self.queue.get()
                    print(document)                    
                    self.index_document(index_name, count, document)
                    # print(index_name + count + "\n")
                    count += 1
            except Exception as e:
                print(f"Error indexing document: {e}")
            time.sleep(1)  
