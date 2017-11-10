#!/usr/bin/python
###########################################################################
# https://squirro.com/2013/03/12/elasticsearch-and-joining/
###########################################################################
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elk_one'}])

if es.indices.exists('test-index'):
    es.indices.delete(index='test-index')

SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "urls": {
            "properties": {
                "url": {
                    "type": "string"
                }
            }
        }
     }
}
es.indices.create(index='test-index', body=SETTINGS)

# ignore 404 and 400
es.indices.delete(index='test-index', params={"ignore" : [400, 404]})