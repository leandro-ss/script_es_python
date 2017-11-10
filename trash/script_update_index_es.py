#!/usr/bin/python
###########################################################################
import json
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch([{'host': '192.168.56.110'}])

IDX="metricbeat-*"

#if es.indices.exists(IDX):
#    es.indices.delete(index=IDX)
#es.index(index=IDX, doc_type="people", id=1, body=
#{
#  "email": "john@smith.com", "first_name": "John", "last_name":  "Smith", "age": 25
#})
# Use the scan&scroll method to fetch all documents from your old index
res = helpers.scan(es, index=IDX, query =
{
  "query": {
      "bool": {
          "filter": [
              { 
                  "match" : { 
                      "metricset.name.keyword": "cpu" 
                  }
              },
          ]
      }
  }
})

update_data = []

for x in res:
    
    x["_source"]["system"]["cpu"]["custom_busy"]={}
    x["_source"]["system"]["cpu"]["custom_busy"]["pct"] = 1-( x["_source"]["system"]["cpu"]["idle"]["pct"] / x["_source"]["system"]["cpu"]["cores"])

    # This is a useless field
    update_data.append(x)

    #print(json.dumps(x, indent=4, sort_keys=True))

helpers.bulk(es,update_data, ignore=400)
es.indices.refresh(index=IDX, ignore = [400, 404])


GET /_search
{
    "query": {
        "ids" : {
            "type" : "metricsets",
            "values" : ["AV5OKKolsFQ8NvbrL3TZ"] 
        }
    }
}

GET /_search
{
    "query": {
        "exists" : { "field" : "system.cpu.custom.pct" }
    }
}

POST /filebeat-silk-log-*/_update_by_query
{
  "query": { 
    "term": {
      "silk_script": "PAG_04_2_Cadastrar-CancelarDebitoAutomatico"
    }
  },
  "script": { "inline": "ctx._source.system.cpu.custom_busy = new HashMap();\
                         ctx._source.system.cpu.custom_busy.pct = 1 - (ctx._source.system.cpu.idle.cpt / ctx._source.system.cpu.cores)
  }
}

