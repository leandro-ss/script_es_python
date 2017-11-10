#!/usr/bin/python
#############################################################################################
# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
#############################################################################################
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elk_one'}])

IDX="test-index"

if es.indices.exists(IDX):
    es.indices.delete(index=IDX)
es.index(index=IDX, doc_type="test", id=1, body=
{
  "message": "this is a test"
})

es.indices.refresh(index= IDX)
# Use the scan&scroll method to fetch all documents from your old index
result = es.search( index=IDX, body =
{ 
    "from" : 0,
    "size" : 10,
    "query": {
        "match" : {
            "message" : {
                "query" : "test is",
                "operator" : "and"
            }
        }
    }
})

import json
print(json.dumps(result, indent=4, sort_keys=True))
