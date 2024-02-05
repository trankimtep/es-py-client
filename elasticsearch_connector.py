# elasticsearch_connector.py
from elasticsearch import Elasticsearch

class ElasticsearchConnector:
    def __init__(self, host):
        self.client = Elasticsearch(host)

    def index_document(self, index_name, document_id, document):
        return self.client.index(index=index_name, id=document_id, document=document)
