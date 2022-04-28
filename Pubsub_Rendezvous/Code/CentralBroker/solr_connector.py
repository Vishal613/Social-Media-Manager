import pysolr
import requests
from collections import OrderedDict

CORE_NAME = "base_core"
# IP = "localhost"
IP = "solr"
#IP = "3.16.37.251"


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{IP}:8983/solr/'
        self.solr = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def create_documents(self, docs):
        print("Indexing ", len(docs), str(" documents into solr at IP: {IP} and core :{core}").format(IP=IP, core=CORE_NAME))
        print(self.solr.add(docs))

    def delete_docs(self, q):
        self.solr.delete(q=q)

    def add_fields(self):
        data = {
            "add-field": [
                {
                    "name": "data_text",
                    "type": "string",
                    "multiValued": False
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


class SearchHelper:

    def __init__(self):
        self.solr_url = f'http://{IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def search(self, keyword):
        solrDocs = self.connection.search(q="*:*", **{'fq': 'keyword:' + keyword}, rows=25)
        tweet_list = []
        for doc in solrDocs.docs:
            tweet_list.append(doc['data_text'][0])
        return tweet_list

    def searchWithFilter(self, filter):
        solrDocs = self.connection.search(q="*:*", **{'fq': filter}, rows=25)
        map = OrderedDict()
        for doc in solrDocs.docs:
            subscriber_id = doc['subscriber_id']
            topics = doc['topics']
            map[subscriber_id[0]] = set(topics)
        return map

    def searchTopics(self, broker_id):
        solrDocs = self.connection.search(q="*:*", **{'fq': 'doc_type:topics_data AND handler_broker:'+broker_id}, rows=25)
        topics = []
        if len(solrDocs) > 0:
            topics = solrDocs.docs[0]['topics']
        return topics


if __name__ == '__main__':
    search = SearchHelper()
    map = search.searchWithFilter('doc_type:subscriber_data')
    print(map)
