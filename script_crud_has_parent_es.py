import json
import requests
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch([{"host": "elk_one"}])

settings = {
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

es.indices.create(index="test-documents", body = settings)

es.index(index="test-documents", doc_type="document", id=1, body=
{
    "title": "Habemus Papam",
    "content": "In Rome, white smoke rose from the chimney atop of the Sistine Chapel.",
    "date": "2013-03-10T12:12:12"
})

es.index(index="test-documents", doc_type="comment", id=2, body=
{
    "text": "Finally!",
    "author": "Jane Roe",
    "date": "2013-03-12T13:13:13"
}, params={"parent":1})

es.index(index="test-documents", doc_type="comment", id=3, body=
{
    "text": "Oh my god ...",
    "author": "John Doe",
    "date": "2013-03-11T14:14:14"
}, params={"parent":1})

search=es.search(index="test-documents", doc_type= "document", body= {
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

print(json.dumps(search, indent=4))

es.indices.delete (index="test-documents")
