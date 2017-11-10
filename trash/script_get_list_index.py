#!/usr/bin/python
###########################################################################

#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elk_one'}])
#es = Elasticsearch([{'host': 'elk_two'}],http_auth=('elastic', 'changeme'))

schema = es.indices.get_mapping() ## python dict with the map of the cluster

indices_list = schema.keys()
just_indices = [index for index in indices_list if not index.startswith(".")] ## remove the objects created by marvel, e.g. ".marvel-date"

import json
for index in just_indices: print(index)