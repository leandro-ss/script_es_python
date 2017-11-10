#!/usr/bin/python
###########################################################################

#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elk_one'}])
#es = Elasticsearch([{'host': 'elk_two'}],http_auth=('elastic', 'changeme'))

import json
for index in es.indices.get('python-silk-percent'):
    print(json.dumps(es.indices.get_settings(index), sort_keys=True, indent=4))
    print(json.dumps(es.indices.get_mapping(index), sort_keys=True, indent=4))