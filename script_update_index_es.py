#!/usr/bin/python
###########################################################################
import json
import requests
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch([{'host': 'elk_one'}])

es.index(index="test-index", doc_type="people", id=1, body=
{
  "email": "john@smith.com",
  "first_name": "John",
  "last_name":  "Smith",
  "age": 25
})

# Use the scan&scroll method to fetch all documents from your old index
res = helpers.scan( es , query = {"query": { "match_all": {} }},index="test-index")

update_data = []

for x in res:
    x["_source"]["age"]=28
    x["_source"]["email"]="robert@doneway.com"
    x["_source"]["first_name"]="Robert"
    x["_source"]["last_name"]="Doneway"

    # This is a useless field
    update_data.append(x)

helpers.bulk(es,update_data)
es.indices.refresh(index="test-index")

print(json.dumps(es.search(index="test-index"), sort_keys=True, indent=4))

print(es.indices.delete(index="test-index"))
