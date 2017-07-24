#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elk_one', 'port': 9200}])
es = Elasticsearch([{'host': 'elk_two', 'port': 9200}],http_auth=('elastic', 'changeme'))

schema = es.indices.get_mapping() ## python dict with the map of the cluster

indices_list = schema.keys()
just_indices = [index for index in indices_list if not index.startswith(".")] ## remove the objects created by marvel, e.g. ".marvel-date"

import json
for index in es.indices.get('cloud*'):
    print(json.dumps(es.indices.get_settings(index), sort_keys=True, indent=4))
    #print(json.dumps(es.indices.get_mapping(index), sort_keys=True, indent=4))