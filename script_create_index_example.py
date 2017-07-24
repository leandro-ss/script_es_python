#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '10:31.75.70:80/elastic'}])

# delete index if exists
if es.indices.exists('test-index'):
    es.indices.delete(index='test-index')
# index settings
settings = {
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
# create index
es.indices.create(index='test-index',  body=settings)

# ignore 400 cause by IndexAlreadyExistsException when creating an index
#es.indices.create(index='test-index', ignore=400)

# ignore 404 and 400
es.indices.delete(index='test-index', ignore=[400, 404])

# connect to localhost directly and another node using SSL on port 443
# and an url_prefix. Note that ``port`` needs to be an int.
