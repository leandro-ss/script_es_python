#!/usr/bin/python
###########################################################################
# https://squirro.com/2013/03/12/elasticsearch-and-joining/
###########################################################################
import json
import requests
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch([{"host": "elk_one"}])

SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
      "comment": {
        "_parent": {
          "type": "document"
        },
        "_routing": {
          "required": True
        }
      }
    }
}

if es.indices.exists("test-documents") :
    es.indices.delete(index="test-documents")

es.indices.create(index="test-documents", body = SETTINGS)

result = es.index(index="test-documents", doc_type="document", id=1, body={
    "title": "Habemus Papam",
    "content": "In Rome, white smoke rose from the chimney atop of the Sistine Chapel.",
    "date": "2013-03-10T12:12:12"
})
print (result)

result = es.index(index="test-documents", doc_type="comment", id=2, body={
    "text": "Finally!",
    "author": "Jane Roe",
    "date": "2013-03-12T13:13:13"
}, params={"parent":1})
print (result)

result = es.index(index="test-documents", doc_type="comment", id=3, body={
    "text": "Oh my god ...",
    "author": "John Doe",
    "date": "2013-03-11T14:14:14"
}, params={"parent":1})
print (result)

print(es.indices.refresh(index= "test-documents"))

result = es.search(index="test-documents", doc_type= "document", body= {
  "query": {
    "bool": {
      "must" : [{ "match": {"title": "papam"} }],
      "filter": {
          "has_child": {
              "type": "comment",
              "query" : {
                  "match": {
                      "author": {
                          "query" : "John Doe",
                          "operator" : "and"
                      }
                  }
              }
          }
      }
    }
  }
})

print(json.dumps(result, indent=4, sort_keys=True))