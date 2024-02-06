# indexer.py
import time
import threading
import json
import subprocess
from elasticsearch_connector import ElasticsearchConnector
# from log_to_doc_converter import LogToDocConverter
import requests
from datetime import datetime

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

    def create_index(self, es_endpoint, index_name):
        # Construct the URL for the index
        url = f"{es_endpoint}/{index_name}/_settings"
        
        # Optional: Define the index settings and mappings (if needed)
        # If you don't need to specify settings or mappings, you can omit the `data` parameter in the request
        index_configuration = {
            "settings": {
                "index.default_pipeline": f"add-timestamp-{index_name}"
            }
        }
        
        # Send the PUT request to create or update the index
        headers = {'Content-Type': 'application/json'}
        response = requests.put(url, headers=headers, json=index_configuration)
        
        # Check if the request was successful
        if response.status_code in [200, 201]:  # 200 OK or 201 Created
            print(f"Index '{index_name}' created or updated successfully.")
        else:
            print(f"Failed to create or update index. Status code: {response.status_code}, Response: {response.text}")

    def create_timestamp_pipeline(self, es_endpoint, index_name):
        # Construct the URL for the ingest pipeline API
        url = f"{es_endpoint}/_ingest/pipeline/add-timestamp-{index_name}"
        
        # Define the payload for the pipeline
        payload = {
            "processors": [
                {
                    "set": {
                        "field": "timestamp",
                        "value": "{{_ingest.timestamp}}"
                    }
                }
            ]
        }
        
        # Send the PUT request
        headers = {'Content-Type': 'application/json'}
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        
        # Check if the request was successful
        if response.status_code == 200:
            print("Ingest pipeline created successfully.")
        else:
            print(f"Failed to create ingest pipeline. Status code: {response.status_code}, Response: {response.text}")

    def check_and_index(self):
        print (f"Indexing doc to {self.service}_log")
        index_name = self.service + "_log"
        self.create_timestamp_pipeline(self.es_endpoint, index_name)
        self.create_index(self.es_endpoint, index_name)
        # count = self.gen_id(self.es_endpoint, index_name)
        count = 1
        while not self.stop_event.is_set():
            try:
                if not self.queue.empty():
                    document = self.queue.get()
                    json_obj = json.loads(document)
                    print(f"{json_obj} \n\n")
                    self.index_document(index_name, count, json_obj)
                    # print(index_name + count + "\n")
                    count += 1
            except Exception as e:
                print(f"Error indexing document: {e}")
            time.sleep(1)  
